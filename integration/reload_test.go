package integration_test

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/vinaysrao1/fruitfly/types"
)

// openTestDB opens a fresh DuckDB read connection at the given path.
func openTestDB(t *testing.T, dbPath string) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	return db
}

// approveTaggedV1Rule always approves with rule_id "approve-v1",
// allowing tests to identify which snapshot processed the event.
const approveTaggedV1Rule = `
rule_id = "approve-v1"
event_type = "*"
priority = 100
def evaluate(event):
    return verdict("approve", reason="snapshot-v1")
`

// blockTaggedV2Rule always blocks with rule_id "block-v2",
// used as the replacement rule after hot reload.
const blockTaggedV2Rule = `
rule_id = "block-v2"
event_type = "*"
priority = 100
def evaluate(event):
    return verdict("block", reason="snapshot-v2")
`

// TestHotReload_UnderTraffic_SnapshotIsolation (T17):
// Send 1000 events, swap rules at event 500. Verify snapshot-per-event isolation:
// every result must use exactly one snapshot (no mixed rule sets).
// Before swap: rule "approve-v1" -> approve. After swap: rule "block-v2" -> block.
// No result should have both "approve-v1" and "block-v2" in triggered rules.
func TestHotReload_UnderTraffic_SnapshotIsolation(t *testing.T) {
	opts := defaultOpts()
	opts.workerCount = 4
	opts.eventChanCap = 200
	opts.resultChanCap = 200

	tp := newTestPipeline(t, approveTaggedV1Rule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	const totalEvents = 1000
	const swapAt = 500

	// Send all events, swapping the rule at event 500.
	for i := 0; i < totalEvents; i++ {
		event := types.Event{
			EventID:    fmt.Sprintf("reload-test-event-%d", i),
			EventType:  "test",
			Timestamp:  time.Now(),
			Payload:    map[string]any{"index": i},
			ReceivedAt: time.Now(),
		}
		tp.eventChan <- event

		// At event 500, swap rules.
		if i == swapAt-1 {
			writeRule(t, tp.rulesDir, "rule.star", blockTaggedV2Rule)
			tp.reloader.Reload()
			// Brief pause to allow reload to propagate before continuing.
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Give time for all events to be processed.
	time.Sleep(2 * time.Second)

	// Shutdown.
	tp.shutdown(t, cancel, poolDone, writerDone)

	// Query DuckDB to verify all events processed and snapshot isolation held.
	db := openTestDB(t, tp.dbPath)
	defer db.Close()

	// All 1000 events must be in DuckDB.
	var totalCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM results").Scan(&totalCount); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if totalCount != totalEvents {
		t.Errorf("expected %d results in DuckDB, got %d", totalEvents, totalCount)
	}

	// Verify all verdicts are valid.
	var invalidCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM results WHERE verdict NOT IN ('approve', 'block', 'review')").Scan(&invalidCount); err != nil {
		t.Fatalf("invalid verdict count query: %v", err)
	}
	if invalidCount != 0 {
		t.Errorf("found %d results with invalid verdict after hot reload", invalidCount)
	}

	// The key invariant: every event used exactly one snapshot.
	// Each result's triggered_rules JSON contains rule_id fields.
	// No event should have rules from both snapshots (approve-v1 and block-v2).
	rows, err := db.Query(`SELECT event_id, verdict, CAST(triggered_rules AS VARCHAR) FROM results`)
	if err != nil {
		t.Fatalf("query triggered rules: %v", err)
	}
	defer rows.Close()

	mixedCount := 0
	for rows.Next() {
		var eventID, verdict, triggeredJSON string
		if err := rows.Scan(&eventID, &verdict, &triggeredJSON); err != nil {
			t.Fatalf("scan row: %v", err)
		}

		hasV1 := strings.Contains(triggeredJSON, "approve-v1")
		hasV2 := strings.Contains(triggeredJSON, "block-v2")

		// Both rule IDs in the same triggered_rules = snapshot mixing violation.
		if hasV1 && hasV2 {
			mixedCount++
			t.Errorf("event %s has mixed rule sets: triggered_rules=%s", eventID, triggeredJSON)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}

	if mixedCount > 0 {
		t.Errorf("total events with mixed snapshots: %d (snapshot isolation violated)", mixedCount)
	}

	var approveCount, blockCount int
	db.QueryRow("SELECT COUNT(*) FROM results WHERE verdict = 'approve'").Scan(&approveCount) //nolint:errcheck
	db.QueryRow("SELECT COUNT(*) FROM results WHERE verdict = 'block'").Scan(&blockCount)     //nolint:errcheck

	t.Logf("T17 snapshot isolation: approve=%d (v1), block=%d (v2), total=%d", approveCount, blockCount, totalCount)

	// All results must be accounted for as either approve or block.
	if approveCount+blockCount != totalCount {
		t.Errorf("approve+block=%d != total=%d; some events have unexpected verdict", approveCount+blockCount, totalCount)
	}
}

// TestHotReload_BadRules_PreservesOldSnapshot (T18):
// When a bad rule file is introduced mid-traffic, the reloader rejects it
// and the old snapshot remains active, ensuring no disruption to processing.
func TestHotReload_BadRules_PreservesOldSnapshot(t *testing.T) {
	opts := defaultOpts()
	tp := newTestPipeline(t, approveAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	handler := tp.ingestServer.Handler()

	// Send 5 events before injecting bad rule.
	for i := 0; i < 5; i++ {
		resp := postEvent(t, handler, makeEvent("test"))
		if resp.Code != 202 {
			t.Errorf("pre-reload event %d: expected 202, got %d", i, resp.Code)
		}
	}

	// Wait for events to be processed.
	for i := 0; i < 5; i++ {
		waitForWebhook(t, tp.webhookCh, 5*time.Second)
	}

	// Capture the initial snapshot ID.
	initialSnap := tp.snapshotPtr.Load()
	if initialSnap == nil {
		t.Fatal("initial snapshot is nil")
	}
	initialID := initialSnap.ID

	// Replace rule file with a syntax error.
	writeRule(t, tp.rulesDir, "rule.star", `
rule_id = "broken"
def evaluate(event)
    return verdict("approve")
`)

	// Trigger reload.
	tp.reloader.Reload()

	// Give the reloader time to attempt the reload.
	time.Sleep(500 * time.Millisecond)

	// Snapshot should still be the original.
	currentSnap := tp.snapshotPtr.Load()
	if currentSnap == nil {
		t.Fatal("snapshot became nil after bad reload")
	}
	if currentSnap.ID != initialID {
		t.Errorf("snapshot ID changed after bad reload: got %s, want %s", currentSnap.ID, initialID)
	}

	// Send 5 more events after bad reload attempt: should still work with old rules.
	for i := 0; i < 5; i++ {
		resp := postEvent(t, handler, makeEvent("test"))
		if resp.Code != 202 {
			t.Errorf("post-bad-reload event %d: expected 202, got %d", i, resp.Code)
		}
	}

	// Wait for the additional events to be processed.
	for i := 0; i < 5; i++ {
		waitForWebhook(t, tp.webhookCh, 5*time.Second)
	}

	// Shutdown.
	tp.shutdown(t, cancel, poolDone, writerDone)

	// All 10 events should be in DuckDB.
	dbCount := tp.queryDBCount(t)
	if dbCount != 10 {
		t.Errorf("expected 10 rows in DuckDB, got %d", dbCount)
	}

	// All verdicts should be approve (old rule still active).
	db := openTestDB(t, tp.dbPath)
	defer db.Close()
	var nonApprove int
	if err := db.QueryRow("SELECT COUNT(*) FROM results WHERE verdict != 'approve'").Scan(&nonApprove); err != nil {
		t.Fatalf("query: %v", err)
	}
	if nonApprove != 0 {
		t.Errorf("expected all approves (old rules preserved), got %d non-approve verdicts", nonApprove)
	}
}
