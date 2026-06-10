package integration_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/vinaysrao1/fruitfly/types"
)

// TestInvariants_AfterLoad runs 100 events through the pipeline and verifies
// SQL-level invariants T21-T24 on the resulting DuckDB data.
func TestInvariants_AfterLoad(t *testing.T) {
	opts := defaultOpts()
	tp := newTestPipeline(t, approveAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	const numEvents = 100
	handler := tp.ingestServer.Handler()

	// Send 100 events with unique event IDs.
	for i := 0; i < numEvents; i++ {
		body := map[string]any{
			"event_id":   fmt.Sprintf("invariant-test-%d", i),
			"event_type": "test",
			"timestamp":  time.Now().UTC().Format(time.RFC3339),
			"payload":    map[string]any{"index": i},
		}
		resp := postEvent(t, handler, body)
		if resp.Code != 202 {
			t.Fatalf("event %d: expected 202, got %d: %s", i, resp.Code, resp.Body.String())
		}
	}

	// Wait for all events to be processed via webhooks.
	for i := 0; i < numEvents; i++ {
		select {
		case <-tp.webhookCh:
		case <-time.After(15 * time.Second):
			t.Fatalf("timed out waiting for webhook delivery %d/%d", i+1, numEvents)
		}
	}

	// Shutdown to ensure all writes are flushed.
	tp.shutdown(t, cancel, poolDone, writerDone)

	// Open a read connection for invariant checks.
	db, err := sql.Open("duckdb", tp.dbPath)
	if err != nil {
		t.Fatalf("open duckdb for invariant checks: %v", err)
	}
	defer db.Close()

	// T21: Lossless pipeline invariant.
	// COUNT(*) in DuckDB must equal the number of 202 responses sent.
	t.Run("T21_Lossless", func(t *testing.T) {
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM results").Scan(&count); err != nil {
			t.Fatalf("count query: %v", err)
		}
		if count != numEvents {
			t.Errorf("lossless invariant violated: expected %d rows, got %d", numEvents, count)
		}
	})

	// T22: Valid verdicts invariant.
	// Every verdict must be in {approve, block, review}.
	// No empty or null verdicts.
	t.Run("T22_ValidVerdicts", func(t *testing.T) {
		var invalid int
		if err := db.QueryRow(
			"SELECT COUNT(*) FROM results WHERE verdict NOT IN ('approve', 'block', 'review')",
		).Scan(&invalid); err != nil {
			t.Fatalf("invalid verdict query: %v", err)
		}
		if invalid != 0 {
			t.Errorf("valid verdicts invariant violated: found %d rows with invalid verdict", invalid)
		}

		var nullOrEmpty int
		if err := db.QueryRow(
			"SELECT COUNT(*) FROM results WHERE verdict IS NULL OR verdict = ''",
		).Scan(&nullOrEmpty); err != nil {
			t.Fatalf("null verdict query: %v", err)
		}
		if nullOrEmpty != 0 {
			t.Errorf("valid verdicts invariant violated: found %d rows with null/empty verdict", nullOrEmpty)
		}
	})

	// T23: No duplicate event_id invariant.
	// GROUP BY event_id HAVING COUNT(*) > 1 must return 0 rows.
	t.Run("T23_NoDuplicates", func(t *testing.T) {
		var dups int
		if err := db.QueryRow(
			"SELECT COUNT(*) FROM (SELECT event_id FROM results GROUP BY event_id HAVING COUNT(*) > 1)",
		).Scan(&dups); err != nil {
			t.Fatalf("duplicates query: %v", err)
		}
		if dups != 0 {
			t.Errorf("no duplicates invariant violated: found %d duplicate event_ids", dups)
		}
	})

	// T24: Bounded latency invariant.
	// All latency_us must be < 5,000,000 (eventTimeout = 5s).
	t.Run("T24_BoundedLatency", func(t *testing.T) {
		const maxLatencyUS = int64(5_000_000)
		var slow int
		if err := db.QueryRow(
			"SELECT COUNT(*) FROM results WHERE latency_us >= 5000000",
		).Scan(&slow); err != nil {
			t.Fatalf("latency query: %v", err)
		}
		if slow != 0 {
			t.Errorf("bounded latency invariant violated: found %d rows with latency >= 5s", slow)
		}

		// Also verify latency_us is positive for all rows.
		var nonPositive int
		if err := db.QueryRow(
			"SELECT COUNT(*) FROM results WHERE latency_us <= 0",
		).Scan(&nonPositive); err != nil {
			t.Fatalf("latency positive query: %v", err)
		}
		if nonPositive != 0 {
			t.Errorf("bounded latency invariant violated: found %d rows with non-positive latency_us", nonPositive)
		}

		_ = maxLatencyUS
	})
}

// TestInvariant_Lossless_WithMixedVerdicts runs events through a pipeline with
// multiple rules producing different verdicts and verifies invariants still hold.
func TestInvariant_Lossless_WithMixedVerdicts(t *testing.T) {
	const mixedRules = `
rule_id = "approve-evens"
event_type = "even"
priority = 100
def evaluate(event):
    return verdict("approve")
`
	opts := defaultOpts()
	tp := newTestPipeline(t, mixedRules, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	handler := tp.ingestServer.Handler()

	const numEvents = 20
	for i := 0; i < numEvents; i++ {
		body := map[string]any{
			"event_id":   fmt.Sprintf("mixed-invariant-%d", i),
			"event_type": "even",
			"timestamp":  time.Now().UTC().Format(time.RFC3339),
			"payload":    map[string]any{"index": i},
		}
		resp := postEvent(t, handler, body)
		if resp.Code != 202 {
			t.Fatalf("event %d: expected 202, got %d", i, resp.Code)
		}
	}

	// Wait for all events to be processed.
	for i := 0; i < numEvents; i++ {
		select {
		case <-tp.webhookCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for event %d", i)
		}
	}

	tp.shutdown(t, cancel, poolDone, writerDone)

	db, err := sql.Open("duckdb", tp.dbPath)
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	// Lossless: exactly numEvents rows.
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM results").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != numEvents {
		t.Errorf("expected %d rows, got %d", numEvents, count)
	}

	// No duplicates.
	var dups int
	if err := db.QueryRow(
		"SELECT COUNT(*) FROM (SELECT event_id FROM results GROUP BY event_id HAVING COUNT(*) > 1)",
	).Scan(&dups); err != nil {
		t.Fatalf("duplicates query: %v", err)
	}
	if dups != 0 {
		t.Errorf("found %d duplicate event_ids", dups)
	}

	// Valid verdicts.
	var invalid int
	if err := db.QueryRow(
		"SELECT COUNT(*) FROM results WHERE verdict NOT IN ('approve', 'block', 'review')",
	).Scan(&invalid); err != nil {
		t.Fatalf("verdict check: %v", err)
	}
	if invalid != 0 {
		t.Errorf("found %d invalid verdicts", invalid)
	}
}

// TestInvariant_NoDuplicates_InsertOrIgnore verifies that the INSERT OR IGNORE
// constraint correctly prevents duplicate event_id entries.
func TestInvariant_NoDuplicates_InsertOrIgnore(t *testing.T) {
	opts := defaultOpts()
	tp := newTestPipeline(t, approveAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	// Send the same event twice directly to eventChan with the same event_id.
	const duplicateID = "duplicate-event-invariant"
	event := types.Event{
		EventID:    duplicateID,
		EventType:  "test",
		Timestamp:  time.Now(),
		Payload:    map[string]any{"version": 1},
		ReceivedAt: time.Now(),
	}
	tp.eventChan <- event
	tp.eventChan <- event // same event_id

	// Wait for both to be processed.
	waitForWebhook(t, tp.webhookCh, 5*time.Second)
	waitForWebhook(t, tp.webhookCh, 5*time.Second)

	tp.shutdown(t, cancel, poolDone, writerDone)

	db, err := sql.Open("duckdb", tp.dbPath)
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow(
		"SELECT COUNT(*) FROM results WHERE event_id = ?", duplicateID,
	).Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row for duplicate event_id, got %d (INSERT OR IGNORE violated)", count)
	}

	// Also verify the overall no-duplicates invariant.
	var dups int
	if err := db.QueryRow(
		"SELECT COUNT(*) FROM (SELECT event_id FROM results GROUP BY event_id HAVING COUNT(*) > 1)",
	).Scan(&dups); err != nil {
		t.Fatalf("duplicates query: %v", err)
	}
	if dups != 0 {
		t.Errorf("global no-duplicates invariant violated: found %d duplicate event_ids", dups)
	}
}
