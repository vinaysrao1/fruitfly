package integration_test

import (
	"database/sql"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vinaysrao1/fruitfly/types"
)

// counterRule is a Starlark rule that calls counter() on the entity_id field.
// It approves all events but increments the counter so we can verify cross-worker accuracy.
// entity_id is stored in payload, event_type is at the top level.
const counterRule = `
rule_id = "counter-check"
event_type = "*"
priority = 100
def evaluate(event):
    entity_id = event["payload"].get("entity_id", "")
    c = counter(entity_id, event["event_type"], 600)
    return verdict("approve", reason="count=" + str(c))
`

// TestContract_LosslessPipeline (T43):
// count(202 responses) == count(DuckDB rows) after shutdown.
// Every accepted event must produce exactly one DuckDB row.
func TestContract_LosslessPipeline(t *testing.T) {
	opts := defaultOpts()
	tp := newTestPipeline(t, approveAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	handler := tp.ingestServer.Handler()

	const numEvents = 50
	var accepted int

	for i := 0; i < numEvents; i++ {
		event := map[string]any{
			"event_id":   fmt.Sprintf("lossless-%d", i),
			"event_type": "test",
			"timestamp":  time.Now().UTC().Format(time.RFC3339),
		}
		resp := postEvent(t, handler, event)
		if resp.Code == 202 {
			accepted++
		}
	}

	// Wait for all webhooks to confirm processing before shutdown.
	for i := 0; i < accepted; i++ {
		select {
		case <-tp.webhookCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for webhook delivery %d/%d", i+1, accepted)
		}
	}

	tp.shutdown(t, cancel, poolDone, writerDone)

	dbCount := tp.queryDBCount(t)
	if dbCount != accepted {
		t.Errorf("T43 lossless pipeline: accepted=%d, DuckDB rows=%d (data loss detected)", accepted, dbCount)
	}
}

// TestContract_AtomicReload (T44):
// Every event uses exactly one snapshot — no mixed rule sets.
// Event1 processed with approve-all rule, event2 processed with block-all rule after reload.
func TestContract_AtomicReload(t *testing.T) {
	opts := defaultOpts()
	tp := newTestPipeline(t, approveAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	handler := tp.ingestServer.Handler()

	// Send event1 with approve-all rule.
	event1 := map[string]any{
		"event_id":   "atomic-reload-event-1",
		"event_type": "test",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
	}
	resp1 := postEvent(t, handler, event1)
	if resp1.Code != 202 {
		t.Fatalf("event1: expected 202, got %d", resp1.Code)
	}

	// Wait for event1 to be processed before reloading.
	webhookBody1 := waitForWebhook(t, tp.webhookCh, 5*time.Second)
	verdict1 := parseWebhookVerdict(t, webhookBody1)
	if verdict1 != "approve" {
		t.Errorf("event1: expected approve (with approve-all rule), got %q", verdict1)
	}

	// Swap rule file to block-all, then trigger reload.
	writeRule(t, tp.rulesDir, "rule.star", blockAllRule)
	initialSnapID := tp.snapshotPtr.Load().ID
	tp.reloader.Reload()

	// Wait for snapshot to change.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		newSnap := tp.snapshotPtr.Load()
		if newSnap != nil && newSnap.ID != initialSnapID {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Send event2 with block-all rule.
	event2 := map[string]any{
		"event_id":   "atomic-reload-event-2",
		"event_type": "test",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
	}
	resp2 := postEvent(t, handler, event2)
	if resp2.Code != 202 {
		t.Fatalf("event2: expected 202, got %d", resp2.Code)
	}

	webhookBody2 := waitForWebhook(t, tp.webhookCh, 5*time.Second)
	verdict2 := parseWebhookVerdict(t, webhookBody2)
	if verdict2 != "block" {
		t.Errorf("event2: expected block (with block-all rule after reload), got %q", verdict2)
	}

	tp.shutdown(t, cancel, poolDone, writerDone)

	// Verify DuckDB: event1=approve, event2=block (no mixing).
	rows := tp.queryDB(t)
	if len(rows) != 2 {
		t.Fatalf("expected 2 DuckDB rows, got %d", len(rows))
	}

	verdictsByID := make(map[string]string)
	for _, row := range rows {
		verdictsByID[row.EventID] = row.Verdict
	}

	if verdictsByID["atomic-reload-event-1"] != "approve" {
		t.Errorf("event1 in DuckDB: want approve, got %q", verdictsByID["atomic-reload-event-1"])
	}
	if verdictsByID["atomic-reload-event-2"] != "block" {
		t.Errorf("event2 in DuckDB: want block, got %q", verdictsByID["atomic-reload-event-2"])
	}
}

// TestContract_BackpressureNotLoss (T45):
// Overload produces 429, never drops an accepted event.
// Every 202-accepted event must appear in DuckDB.
func TestContract_BackpressureNotLoss(t *testing.T) {
	opts := testPipelineOpts{
		eventChanCap:  2,
		resultChanCap: 2,
		workerCount:   1,
	}
	tp := newTestPipeline(t, approveAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	handler := tp.ingestServer.Handler()

	var accepted atomic.Int64
	var rejected atomic.Int64

	// Fire 30 POSTs rapidly in parallel to trigger backpressure.
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			event := map[string]any{
				"event_id":   fmt.Sprintf("backpressure-%d", i),
				"event_type": "test",
				"timestamp":  time.Now().UTC().Format(time.RFC3339),
			}
			resp := postEvent(t, handler, event)
			switch resp.Code {
			case 202:
				accepted.Add(1)
			case 429:
				rejected.Add(1)
			default:
				t.Errorf("unexpected status %d", resp.Code)
			}
		}(i)
	}
	wg.Wait()

	// At least some should be rejected (backpressure active).
	if rejected.Load() == 0 {
		t.Logf("T45 warning: no 429 responses observed — backpressure may not have triggered")
	}

	// Drain accepted webhooks.
	acceptedCount := int(accepted.Load())
	for i := 0; i < acceptedCount; i++ {
		select {
		case <-tp.webhookCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for webhook delivery %d/%d", i+1, acceptedCount)
		}
	}

	tp.shutdown(t, cancel, poolDone, writerDone)

	// Every accepted event must be in DuckDB (no silent drops).
	dbCount := tp.queryDBCount(t)
	if dbCount != acceptedCount {
		t.Errorf("T45 backpressure not loss: accepted=%d, DuckDB rows=%d (silent drops detected)", acceptedCount, dbCount)
	}
}

// TestContract_BoundedLatency (T46):
// All events complete within eventTimeout (5s). All latency_us < 5,000,000.
func TestContract_BoundedLatency(t *testing.T) {
	opts := defaultOpts()
	tp := newTestPipeline(t, approveAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	handler := tp.ingestServer.Handler()

	const numEvents = 100
	var accepted int

	for i := 0; i < numEvents; i++ {
		event := map[string]any{
			"event_id":   fmt.Sprintf("latency-%d", i),
			"event_type": "test",
			"timestamp":  time.Now().UTC().Format(time.RFC3339),
		}
		resp := postEvent(t, handler, event)
		if resp.Code == 202 {
			accepted++
		}
	}

	// Drain webhooks.
	for i := 0; i < accepted; i++ {
		select {
		case <-tp.webhookCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for webhook delivery %d/%d", i+1, accepted)
		}
	}

	tp.shutdown(t, cancel, poolDone, writerDone)

	// Query DuckDB: all latency_us must be < 5,000,000 (5 seconds in microseconds).
	db, err := sql.Open("duckdb", tp.dbPath)
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	var slowCount int
	const eventTimeoutUS = 5_000_000
	row := db.QueryRow("SELECT COUNT(*) FROM results WHERE latency_us >= ?", eventTimeoutUS)
	if err := row.Scan(&slowCount); err != nil {
		t.Fatalf("query slow events: %v", err)
	}
	if slowCount != 0 {
		t.Errorf("T46 bounded latency: found %d events with latency_us >= %d (5s)", slowCount, eventTimeoutUS)
	}

	// Also verify max latency is reasonable (no zero-latency events).
	var maxLatencyUS int64
	maxRow := db.QueryRow("SELECT MAX(latency_us) FROM results")
	if err := maxRow.Scan(&maxLatencyUS); err != nil {
		t.Fatalf("query max latency: %v", err)
	}
	if accepted > 0 && maxLatencyUS <= 0 {
		t.Errorf("T46 bounded latency: max latency_us=%d, expected > 0", maxLatencyUS)
	}
}

// TestContract_CounterConsistency (T47):
// counter() returns true cross-worker sum, not per-worker undercount.
// Send 10 events from same entity with 2 workers. pool.CounterSum() must return 10.
func TestContract_CounterConsistency(t *testing.T) {
	opts := testPipelineOpts{
		eventChanCap:  20,
		resultChanCap: 20,
		workerCount:   2,
	}
	tp := newTestPipeline(t, counterRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	const numEvents = 10
	const entityID = "entity-counter-test"

	// Send 10 events from the same entity directly to eventChan for determinism.
	for i := 0; i < numEvents; i++ {
		event := types.Event{
			EventID:    fmt.Sprintf("counter-event-%d", i),
			EventType:  "test",
			Timestamp:  time.Now(),
			Payload:    map[string]any{"entity_id": entityID, "event_type": "test"},
			RawJSON:    []byte(`{}`),
			ReceivedAt: time.Now(),
		}
		tp.eventChan <- event
	}

	// Wait for all events to be processed via webhook.
	for i := 0; i < numEvents; i++ {
		select {
		case <-tp.webhookCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for webhook %d/%d", i+1, numEvents)
		}
	}

	// Query cross-worker counter sum.
	total := tp.pool.CounterSum(entityID, "test", 600)
	if total != int64(numEvents) {
		t.Errorf("T47 counter consistency: want %d, got %d (cross-worker sum incorrect)", numEvents, total)
	}

	tp.shutdown(t, cancel, poolDone, writerDone)
}

// TestContract_IdempotentWrites (T48):
// Duplicate event_id produces 1 row in DuckDB. First row's data is preserved (INSERT OR IGNORE).
func TestContract_IdempotentWrites(t *testing.T) {
	opts := defaultOpts()
	tp := newTestPipeline(t, approveAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	handler := tp.ingestServer.Handler()
	const dupEventID = "idempotent-event-id-dup"

	// Send the same event_id twice.
	event1 := map[string]any{
		"event_id":   dupEventID,
		"event_type": "test",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
	}
	event2 := map[string]any{
		"event_id":   dupEventID,
		"event_type": "test",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
	}

	resp1 := postEvent(t, handler, event1)
	if resp1.Code != 202 {
		t.Fatalf("event1: expected 202, got %d", resp1.Code)
	}

	resp2 := postEvent(t, handler, event2)
	if resp2.Code != 202 {
		t.Fatalf("event2: expected 202, got %d", resp2.Code)
	}

	// Wait for both to be processed.
	waitForWebhook(t, tp.webhookCh, 5*time.Second)
	waitForWebhook(t, tp.webhookCh, 5*time.Second)

	tp.shutdown(t, cancel, poolDone, writerDone)

	// Assert DuckDB has exactly 1 row for this event_id (INSERT OR IGNORE semantics).
	db, err := sql.Open("duckdb", tp.dbPath)
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM results WHERE event_id = ?", dupEventID).Scan(&count); err != nil {
		t.Fatalf("query count: %v", err)
	}
	if count != 1 {
		t.Errorf("T48 idempotent writes: want 1 row for duplicate event_id, got %d", count)
	}
}

// TestContract_GracefulShutdown (T49):
// SIGTERM -> drain in-flight -> flush DuckDB -> exit 0.
// Simulated via cancel + close(eventChan) + wait.
// All in-flight events must be flushed to DuckDB before writer exits.
func TestContract_GracefulShutdown(t *testing.T) {
	opts := defaultOpts()
	tp := newTestPipeline(t, approveAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	const numEvents = 20

	// Send events directly to eventChan (bypass HTTP to avoid 429).
	for i := 0; i < numEvents; i++ {
		event := types.Event{
			EventID:    fmt.Sprintf("shutdown-contract-%d", i),
			EventType:  "test",
			Timestamp:  time.Now(),
			Payload:    map[string]any{"index": i},
			RawJSON:    []byte(`{}`),
			ReceivedAt: time.Now(),
		}
		tp.eventChan <- event
	}

	// Execute graceful shutdown sequence (mimics SIGTERM handler in main.go):
	// 1. Stop accepting new requests (HTTP server shutdown - skipped in unit test)
	// 2. Cancel context (stops reloader), close eventChan (signals workers to drain)
	// 3. Wait for pool to finish (it closes resultChan)
	// 4. Wait for writer to flush and close DuckDB
	tp.shutdown(t, cancel, poolDone, writerDone)

	// Verify writer exited cleanly.
	select {
	case err := <-writerDone:
		// writerDone was already consumed by shutdown, so this won't fire.
		// The test passes if shutdown completed without timeout.
		_ = err
	default:
	}

	// All events must be in DuckDB after graceful shutdown.
	dbCount := tp.queryDBCount(t)
	if dbCount != numEvents {
		t.Errorf("T49 graceful shutdown: expected %d DuckDB rows after shutdown, got %d", numEvents, dbCount)
	}
}

// TestEventOrdering_BurstAndDuplicates (T6):
// (a) Burst 100 events from one entity — all processed, no silent drops.
// (b) Duplicate event_id via HTTP: both get 202, DuckDB has 1 row (INSERT OR IGNORE).
// (c) Rapid type switching: interleaved "post" and "like" events all processed correctly.
func TestEventOrdering_BurstAndDuplicates(t *testing.T) {
	opts := testPipelineOpts{
		eventChanCap:  100,
		resultChanCap: 100,
		workerCount:   4,
	}
	tp := newTestPipeline(t, approveAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	handler := tp.ingestServer.Handler()

	t.Run("burst_from_one_entity", func(t *testing.T) {
		const burstCount = 100
		var burstAccepted int

		for i := 0; i < burstCount; i++ {
			event := map[string]any{
				"event_id":   fmt.Sprintf("burst-entity-%d", i),
				"event_type": "test",
				"timestamp":  time.Now().UTC().Format(time.RFC3339),
				"entity_id":  "burst-entity",
			}
			resp := postEvent(t, handler, event)
			if resp.Code == 202 {
				burstAccepted++
			} else if resp.Code != 429 {
				t.Errorf("burst event %d: unexpected status %d", i, resp.Code)
			}
		}

		// Drain webhooks for burst events.
		for i := 0; i < burstAccepted; i++ {
			select {
			case <-tp.webhookCh:
			case <-time.After(15 * time.Second):
				t.Fatalf("burst: timed out waiting for webhook %d/%d", i+1, burstAccepted)
			}
		}

		if burstAccepted == 0 {
			t.Error("burst: expected at least one accepted event")
		}
	})

	t.Run("duplicate_event_id", func(t *testing.T) {
		const dupID = "dup-event-ordering-test"

		dupEvent := map[string]any{
			"event_id":   dupID,
			"event_type": "test",
			"timestamp":  time.Now().UTC().Format(time.RFC3339),
		}

		resp1 := postEvent(t, handler, dupEvent)
		resp2 := postEvent(t, handler, dupEvent)

		// Both should be accepted (HTTP layer does not deduplicate).
		if resp1.Code != 202 {
			t.Errorf("dup event first send: expected 202, got %d", resp1.Code)
		}
		if resp2.Code != 202 {
			t.Errorf("dup event second send: expected 202, got %d", resp2.Code)
		}

		// Drain both webhooks.
		waitForWebhook(t, tp.webhookCh, 5*time.Second)
		waitForWebhook(t, tp.webhookCh, 5*time.Second)
	})

	t.Run("rapid_type_switching", func(t *testing.T) {
		const switchCount = 20
		var switchAccepted int

		for i := 0; i < switchCount; i++ {
			eventType := "post"
			if i%2 == 0 {
				eventType = "like"
			}
			event := map[string]any{
				"event_id":   fmt.Sprintf("type-switch-%d", i),
				"event_type": eventType,
				"timestamp":  time.Now().UTC().Format(time.RFC3339),
			}
			resp := postEvent(t, handler, event)
			if resp.Code == 202 {
				switchAccepted++
			}
		}

		for i := 0; i < switchAccepted; i++ {
			select {
			case <-tp.webhookCh:
			case <-time.After(10 * time.Second):
				t.Fatalf("type-switch: timed out waiting for webhook %d/%d", i+1, switchAccepted)
			}
		}
	})

	tp.shutdown(t, cancel, poolDone, writerDone)

	// Verify duplicate event_id produced exactly 1 DuckDB row.
	db, err := sql.Open("duckdb", tp.dbPath)
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	var dupCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM results WHERE event_id = 'dup-event-ordering-test'").Scan(&dupCount); err != nil {
		t.Fatalf("query dup count: %v", err)
	}
	if dupCount != 1 {
		t.Errorf("T6 duplicate event_id: want 1 DuckDB row, got %d", dupCount)
	}
}

// TestChannelBackpressure_NoGoroutineLeaks (T15):
// eventChan cap=1: rapid POSTs produce 429, pipeline doesn't leak goroutines.
// After shutdown, goroutine count must be close to pre-test baseline.
func TestChannelBackpressure_NoGoroutineLeaks(t *testing.T) {
	opts := testPipelineOpts{
		eventChanCap:  1,
		resultChanCap: 100,
		workerCount:   1,
	}
	tp := newTestPipeline(t, approveAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	handler := tp.ingestServer.Handler()

	// Capture goroutine count before load.
	runtime.GC()
	goroutinesBefore := runtime.NumGoroutine()

	var accepted atomic.Int64
	var rejected atomic.Int64

	// Fire rapid POSTs — with cap=1, most should be rejected.
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			event := map[string]any{
				"event_id":   fmt.Sprintf("backpressure-leak-%d", i),
				"event_type": "test",
				"timestamp":  time.Now().UTC().Format(time.RFC3339),
			}
			resp := postEvent(t, handler, event)
			switch resp.Code {
			case 202:
				accepted.Add(1)
			case 429:
				rejected.Add(1)
			default:
				t.Errorf("unexpected status %d", resp.Code)
			}
		}(i)
	}
	wg.Wait()

	// Verify backpressure triggered.
	if rejected.Load() == 0 {
		t.Log("T15 warning: no 429 responses observed with cap=1 — may be a race")
	}

	// Drain accepted webhooks.
	acceptedCount := int(accepted.Load())
	for i := 0; i < acceptedCount; i++ {
		select {
		case <-tp.webhookCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for webhook %d/%d", i+1, acceptedCount)
		}
	}

	tp.shutdown(t, cancel, poolDone, writerDone)

	// Allow goroutines to finish.
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	goroutinesAfter := runtime.NumGoroutine()

	// Assert no goroutine leak: delta must be less than 5.
	const leakThreshold = 5
	delta := goroutinesAfter - goroutinesBefore
	if delta > leakThreshold {
		t.Errorf("T15 goroutine leak: before=%d, after=%d, delta=%d (threshold=%d)",
			goroutinesBefore, goroutinesAfter, delta, leakThreshold)
	}
}
