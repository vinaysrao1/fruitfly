package output

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	_ "github.com/marcboeker/go-duckdb"

	"github.com/vinaysrao1/fruitfly/types"
)

const (
	batchSize             = 100
	flushInterval         = 500 * time.Millisecond
	retainDays            = 30
	webhookTimeout        = 5 * time.Second
	maxConcurrentWebhooks = 100
)

// Writer handles DuckDB persistence and webhook delivery for evaluated results.
type Writer struct {
	db            *sql.DB
	webhookURL    string
	webhookClient *http.Client
	webhookSem    chan struct{}
	Ready         atomic.Bool

	// emitInteresting, when set, persists and webhooks only "interesting"
	// results — non-approve verdicts or rule failures. Plain approvals are
	// counted in stats and dropped, which removes the output path as the
	// throughput ceiling. Set before Run.
	emitInteresting bool

	stats struct {
		approve, block, review atomic.Int64 // all results, by verdict
		errored                atomic.Int64 // results with >=1 failed rule
		skipped                atomic.Int64 // results dropped by emit policy
	}
}

// SetEmitInteresting selects the "interesting-only" emission policy.
// Must be called before Run.
func (w *Writer) SetEmitInteresting(on bool) { w.emitInteresting = on }

// Stats returns cumulative result counts for the metrics endpoint.
func (w *Writer) Stats() map[string]int64 {
	return map[string]int64{
		"results_approve": w.stats.approve.Load(),
		"results_block":   w.stats.block.Load(),
		"results_review":  w.stats.review.Load(),
		"results_errored": w.stats.errored.Load(),
		"results_skipped": w.stats.skipped.Load(),
	}
}

// observe records a result in stats and reports whether the emission policy
// keeps it (persist + webhook) or drops it.
func (w *Writer) observe(r types.Result) (keep bool) {
	switch r.FinalVerdict {
	case types.VerdictBlock:
		w.stats.block.Add(1)
	case types.VerdictReview:
		w.stats.review.Add(1)
	default:
		w.stats.approve.Add(1)
	}
	if len(r.FailedRules) > 0 {
		w.stats.errored.Add(1)
	}
	if w.emitInteresting && r.FinalVerdict == types.VerdictApprove && len(r.FailedRules) == 0 {
		w.stats.skipped.Add(1)
		return false
	}
	return true
}

// NewWriter opens DuckDB at dbPath, creates the results table, returns Writer.
func NewWriter(dbPath string, webhookURL string) (*Writer, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open duckdb %s: %w", dbPath, err)
	}
	db.SetMaxOpenConns(1)

	// DuckDB uses WAL mode by default for file-based databases. No explicit PRAGMA needed.

	if err := createSchema(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}

	if _, err := db.Exec("SET memory_limit='256MB'"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set memory limit: %w", err)
	}

	w := &Writer{
		db:         db,
		webhookURL: webhookURL,
		webhookClient: &http.Client{
			Timeout: webhookTimeout,
		},
		webhookSem: make(chan struct{}, maxConcurrentWebhooks),
	}
	w.Ready.Store(true)
	return w, nil
}

func createSchema(db *sql.DB) error {
	const schema = `
CREATE TABLE IF NOT EXISTS results (
    event_id       VARCHAR PRIMARY KEY,
    event_type     VARCHAR NOT NULL,
    verdict        VARCHAR NOT NULL,
    triggered_rules JSON,
    failed_rules   JSON,
    payload        JSON,
    latency_us     BIGINT,
    processed_at   TIMESTAMP DEFAULT current_timestamp
)`
	_, err := db.Exec(schema)
	return err
}

// Run reads Results from in, batch-inserts to DuckDB, and fires webhook goroutines.
// Blocks until in is closed, then flushes remaining batch and closes DuckDB.
func (w *Writer) Run(ctx context.Context, in <-chan types.Result) error {
	var batch []types.Result
	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	retentionTicker := time.NewTicker(24 * time.Hour)
	defer retentionTicker.Stop()

	for {
		select {
		case result, ok := <-in:
			if !ok {
				w.flush(batch)
				return w.Close()
			}
			if !w.observe(result) {
				continue
			}
			batch = append(batch, result)
			if len(batch) >= batchSize {
				w.flush(batch)
				batch = resetBatch(batch)
			}
			select {
			case w.webhookSem <- struct{}{}:
				go func(r types.Result) {
					defer func() { <-w.webhookSem }()
					w.sendWebhook(ctx, r)
				}(result)
			default:
				slog.Warn("webhook: concurrency limit reached, dropping", "event_id", result.EventID)
			}

		case <-flushTicker.C:
			if len(batch) > 0 {
				w.flush(batch)
				batch = resetBatch(batch)
			}

		case <-retentionTicker.C:
			w.runRetention()

		case <-ctx.Done():
			// Context cancelled. Drain remaining results from the channel before
			// closing so that events already in-flight are not lost.
			// Use a timeout to avoid blocking forever if the channel is never closed.
			drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer drainCancel()
			for {
				select {
				case result, ok := <-in:
					if !ok {
						w.flush(batch)
						return w.Close()
					}
					if !w.observe(result) {
						continue
					}
					batch = append(batch, result)
					if len(batch) >= batchSize {
						w.flush(batch)
						batch = resetBatch(batch)
					}
				case <-drainCtx.Done():
					w.flush(batch)
					return w.Close()
				}
			}
		}
	}
}

// resetBatch zeroes flushed elements so their payloads can be GC'd while the
// backing array is reused.
func resetBatch(batch []types.Result) []types.Result {
	clear(batch)
	return batch[:0]
}

func (w *Writer) flush(batch []types.Result) {
	if len(batch) == 0 {
		return
	}
	if err := w.insertBatch(batch); err != nil {
		slog.Error("flush: batch insert failed, retrying once", "error", err, "batch_size", len(batch))
		if err2 := w.insertBatch(batch); err2 != nil {
			slog.Error("flush: batch insert failed after retry, dropping batch",
				"error", err2, "batch_size", len(batch))
		}
	}
}

func (w *Writer) insertBatch(batch []types.Result) error {
	tx, err := w.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	const q = `INSERT OR IGNORE INTO results
		(event_id, event_type, verdict, triggered_rules, failed_rules, payload, latency_us, processed_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

	stmt, err := tx.Prepare(q)
	if err != nil {
		return fmt.Errorf("prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, r := range batch {
		triggeredJSON, err := json.Marshal(r.TriggeredRules)
		if err != nil {
			return fmt.Errorf("marshal triggered_rules for %s: %w", r.EventID, err)
		}
		failedJSON, err := json.Marshal(r.FailedRules)
		if err != nil {
			return fmt.Errorf("marshal failed_rules for %s: %w", r.EventID, err)
		}
		payloadJSON := r.RawPayload
		if len(payloadJSON) == 0 {
			payloadJSON, err = json.Marshal(r.Payload)
			if err != nil {
				return fmt.Errorf("marshal payload for %s: %w", r.EventID, err)
			}
		}

		processedAt := r.ProcessedAt
		if processedAt.IsZero() {
			processedAt = time.Now()
		}

		if _, err := stmt.Exec(
			r.EventID,
			r.EventType,
			string(r.FinalVerdict),
			string(triggeredJSON),
			string(failedJSON),
			string(payloadJSON),
			r.LatencyUS,
			processedAt,
		); err != nil {
			return fmt.Errorf("insert %s: %w", r.EventID, err)
		}
	}

	return tx.Commit()
}

func (w *Writer) runRetention() {
	_, err := w.db.Exec(
		fmt.Sprintf(`DELETE FROM results WHERE processed_at < now()::TIMESTAMP - interval '%d days'`, retainDays),
	)
	if err != nil {
		slog.Error("retention: delete failed", "error", err)
	}
}

// Close flushes pending state and closes the DuckDB connection.
func (w *Writer) Close() error {
	w.Ready.Store(false)
	return w.db.Close()
}

// sendWebhook posts result as JSON to webhook URL with retries.
func (w *Writer) sendWebhook(ctx context.Context, result types.Result) {
	if w.webhookURL == "" {
		return
	}

	body, err := json.Marshal(result)
	if err != nil {
		slog.Error("webhook: marshal failed", "event_id", result.EventID, "error", err)
		return
	}

	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.webhookURL, bytes.NewReader(body))
		if err != nil {
			slog.Error("webhook: build request failed", "event_id", result.EventID, "error", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := w.webhookClient.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 300 {
				return
			}
			if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != 429 {
				slog.Warn("webhook: client error, not retrying",
					"event_id", result.EventID, "status", resp.StatusCode)
				return
			}
		}
		if attempt == maxAttempts-1 {
			break // no point sleeping after the final attempt
		}

		backoff := time.Duration(100*(1<<attempt)) * time.Millisecond
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}

	slog.Error("webhook: delivery failed after retries",
		"event_id", result.EventID, "url", w.webhookURL)
}
