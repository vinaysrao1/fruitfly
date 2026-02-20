package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/vinaysrao1/fruitfly/executor"
	"github.com/vinaysrao1/fruitfly/ingest"
	"github.com/vinaysrao1/fruitfly/output"
	"github.com/vinaysrao1/fruitfly/rules"
	"github.com/vinaysrao1/fruitfly/types"
)

const (
	maxEventBytes = 256 * 1024
	eventTimeout  = 5 * time.Second
	ruleTimeout   = 1 * time.Second
)

// Starlark rule constants for tests.
const approveAllRule = `
rule_id = "approve-all"
event_type = "*"
priority = 100
def evaluate(event):
    return verdict("approve")
`

const blockAllRule = `
rule_id = "block-all"
event_type = "*"
priority = 100
def evaluate(event):
    return verdict("block", reason="blocked")
`

const blockSpamRule = `
rule_id = "block-spam"
event_type = "spam"
priority = 100
def evaluate(event):
    return verdict("block", reason="spam detected")
`

// testPipelineOpts holds optional pipeline configuration overrides.
type testPipelineOpts struct {
	eventChanCap  int
	resultChanCap int
	workerCount   int
}

// defaultOpts returns default pipeline options for tests.
func defaultOpts() testPipelineOpts {
	return testPipelineOpts{
		eventChanCap:  100,
		resultChanCap: 100,
		workerCount:   2,
	}
}

// testPipeline holds a fully wired pipeline for integration testing.
type testPipeline struct {
	// Channels
	eventChan  chan types.Event
	resultChan chan types.Result

	// Components
	snapshotPtr  *atomic.Pointer[rules.Snapshot]
	compiler     *rules.Compiler
	reloader     *rules.Reloader
	writer       *output.Writer
	pool         *executor.Pool
	ingestServer *ingest.Server

	// Test resources
	rulesDir   string
	dbPath     string
	webhookSrv *httptest.Server
	webhookCh  chan []byte // receives raw webhook POST bodies
}

// newTestPipeline creates a fully wired pipeline for integration testing.
// ruleSource is the initial Starlark rule content.
// webhookURL is an optional webhook URL. When empty, an httptest server is created
// internally and tp.webhookSrv / tp.webhookCh are populated. When non-empty, the
// provided URL is used directly and tp.webhookSrv / tp.webhookCh are left nil.
func newTestPipeline(t *testing.T, ruleSource string, opts testPipelineOpts, webhookURL string) *testPipeline {
	t.Helper()

	// Create temp rules directory and write initial rule.
	rulesDir := t.TempDir()
	writeRule(t, rulesDir, "rule.star", ruleSource)

	// Create temp DuckDB path.
	dbPath := filepath.Join(t.TempDir(), "test.duckdb")

	// Set up the webhook: either create an internal httptest server or use the provided URL.
	var webhookCh chan []byte
	var webhookSrv *httptest.Server
	if webhookURL == "" {
		webhookCh = make(chan []byte, 200)
		webhookSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			select {
			case webhookCh <- body:
			default:
			}
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(webhookSrv.Close)
		webhookURL = webhookSrv.URL
	}

	// Create channels.
	eventChan := make(chan types.Event, opts.eventChanCap)
	resultChan := make(chan types.Result, opts.resultChanCap)

	// Create compiler with full UDFs.
	compiler := &rules.Compiler{UDFs: rules.DefaultUDFs()}

	// Create shared snapshot pointer that reloader, pool, and test all share.
	snapshotPtr := &atomic.Pointer[rules.Snapshot]{}

	// Initialize reloader (performs initial compilation and stores first snapshot).
	reloader, err := rules.NewReloader(compiler, rulesDir, snapshotPtr)
	if err != nil {
		t.Fatalf("NewReloader: %v", err)
	}

	// Initialize writer.
	writer, err := output.NewWriter(dbPath, webhookURL)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	// Initialize pool (shares snapshotPtr with reloader).
	pool := executor.NewPool(opts.workerCount, snapshotPtr, eventTimeout, ruleTimeout)

	// Initialize ingest server.
	ingestServer := ingest.NewServer(":0", maxEventBytes, eventChan)

	return &testPipeline{
		eventChan:    eventChan,
		resultChan:   resultChan,
		snapshotPtr:  snapshotPtr,
		compiler:     compiler,
		reloader:     reloader,
		writer:       writer,
		pool:         pool,
		ingestServer: ingestServer,
		rulesDir:     rulesDir,
		dbPath:       dbPath,
		webhookSrv:   webhookSrv,
		webhookCh:    webhookCh,
	}
}

// start launches pipeline goroutines. Returns cancel func and done channels.
func (tp *testPipeline) start(t *testing.T) (cancel context.CancelFunc, poolDone <-chan struct{}, writerDone <-chan error) {
	t.Helper()

	ctx, cancelFn := context.WithCancel(context.Background())

	poolDoneCh := make(chan struct{})
	go func() {
		defer close(poolDoneCh)
		tp.pool.Run(ctx, tp.eventChan, tp.resultChan)
	}()

	writerDoneCh := make(chan error, 1)
	go func() {
		writerDoneCh <- tp.writer.Run(ctx, tp.resultChan)
	}()

	go tp.reloader.Run(ctx)

	return cancelFn, poolDoneCh, writerDoneCh
}

// shutdown executes the graceful shutdown sequence and waits for completion.
func (tp *testPipeline) shutdown(t *testing.T, cancel context.CancelFunc, poolDone <-chan struct{}, writerDone <-chan error) {
	t.Helper()

	// Cancel context and close eventChan to signal workers to drain.
	cancel()
	close(tp.eventChan)

	// Wait for pool to finish (it closes resultChan).
	select {
	case <-poolDone:
	case <-time.After(10 * time.Second):
		t.Fatal("pool did not finish within 10 seconds")
	}

	// Wait for writer to flush and close DuckDB.
	select {
	case <-writerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("writer did not finish within 10 seconds")
	}
}

// queryDB opens a fresh read-only connection to DuckDB (after writer has closed it)
// and returns all rows from the results table.
func (tp *testPipeline) queryDB(t *testing.T) []dbRow {
	t.Helper()

	db, err := sql.Open("duckdb", tp.dbPath)
	if err != nil {
		t.Fatalf("open duckdb for read: %v", err)
	}
	defer db.Close()

	// DuckDB JSON columns return as []interface{} or map[string]interface{} when scanned.
	// Use CAST to VARCHAR to get them as strings.
	rows, err := db.Query(`SELECT event_id, event_type, verdict, CAST(triggered_rules AS VARCHAR), latency_us FROM results`)
	if err != nil {
		t.Fatalf("query results: %v", err)
	}
	defer rows.Close()

	var results []dbRow
	for rows.Next() {
		var r dbRow
		// triggered_rules may be NULL; use sql.NullString to handle that.
		var triggeredRulesNull sql.NullString
		if err := rows.Scan(&r.EventID, &r.EventType, &r.Verdict, &triggeredRulesNull, &r.LatencyUS); err != nil {
			t.Fatalf("scan row: %v", err)
		}
		if triggeredRulesNull.Valid {
			r.TriggeredRulesJSON = triggeredRulesNull.String
		} else {
			r.TriggeredRulesJSON = "[]"
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}
	return results
}

// queryDBCount returns the total number of rows in the results table.
func (tp *testPipeline) queryDBCount(t *testing.T) int {
	t.Helper()

	db, err := sql.Open("duckdb", tp.dbPath)
	if err != nil {
		t.Fatalf("open duckdb for read: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM results`).Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	return count
}

// dbRow holds fields queried back from the results table.
type dbRow struct {
	EventID            string
	EventType          string
	Verdict            string
	TriggeredRulesJSON string
	LatencyUS          int64
}

// postEvent sends a JSON event to the ingest handler via httptest and returns the response.
func postEvent(t *testing.T, handler http.Handler, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()

	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	req := httptest.NewRequest("POST", "/events", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}

// writeRule writes a Starlark rule file to the given directory.
func writeRule(t *testing.T, dir, filename, source string) {
	t.Helper()
	path := filepath.Join(dir, filename)
	if err := os.WriteFile(path, []byte(source), 0o644); err != nil {
		t.Fatalf("write rule %s: %v", path, err)
	}
}

// makeEvent builds a valid event payload for testing.
func makeEvent(eventType string) map[string]any {
	return map[string]any{
		"event_id":   fmt.Sprintf("test-%d", time.Now().UnixNano()),
		"event_type": eventType,
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
		"payload":    map[string]any{"source": "test"},
	}
}

// waitForWebhook waits for a webhook delivery with a timeout and returns the body.
func waitForWebhook(t *testing.T, webhookCh <-chan []byte, timeout time.Duration) []byte {
	t.Helper()
	select {
	case body := <-webhookCh:
		return body
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for webhook delivery after %v", timeout)
		return nil
	}
}

// parseWebhookVerdict parses the verdict field from a webhook JSON body.
func parseWebhookVerdict(t *testing.T, body []byte) string {
	t.Helper()
	var result struct {
		FinalVerdict string `json:"FinalVerdict"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("parse webhook body: %v, body: %s", err, body)
	}
	return result.FinalVerdict
}

// parseWebhookEventID parses the EventID field from a webhook JSON body.
func parseWebhookEventID(t *testing.T, body []byte) string {
	t.Helper()
	var result struct {
		EventID string `json:"EventID"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("parse webhook body: %v", err)
	}
	return result.EventID
}

// queryDBLatencyPercentiles opens a fresh DuckDB read connection and returns
// the P50 and P99 latency values (in microseconds) from the results table.
// DuckDB returns percentile_cont as float64, so we scan into float64 and convert.
func (tp *testPipeline) queryDBLatencyPercentiles(t *testing.T) (p50us, p99us int64) {
	t.Helper()

	db, err := sql.Open("duckdb", tp.dbPath)
	if err != nil {
		t.Fatalf("open duckdb for latency percentiles: %v", err)
	}
	defer db.Close()

	var p50f, p99f float64
	row := db.QueryRow(`SELECT
		percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_us),
		percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_us)
	FROM results`)
	if err := row.Scan(&p50f, &p99f); err != nil {
		t.Fatalf("scan latency percentiles: %v", err)
	}
	return int64(p50f), int64(p99f)
}

