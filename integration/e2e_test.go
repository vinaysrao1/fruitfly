package integration_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vinaysrao1/fruitfly/types"
)

// TestE2E_HappyPath (Test 36):
// Block rule for spam events. POST spam event -> webhook confirms block -> DuckDB row present.
func TestE2E_HappyPath(t *testing.T) {
	opts := defaultOpts()
	tp := newTestPipeline(t, blockSpamRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	handler := tp.ingestServer.Handler()
	event := makeEvent("spam")

	resp := postEvent(t, handler, event)
	if resp.Code != 202 {
		t.Fatalf("expected 202, got %d: %s", resp.Code, resp.Body.String())
	}

	// Wait for webhook to confirm processing.
	webhookBody := waitForWebhook(t, tp.webhookCh, 5*time.Second)
	verdict := parseWebhookVerdict(t, webhookBody)
	if verdict != "block" {
		t.Errorf("webhook: expected verdict=block, got %q", verdict)
	}

	eventID := event["event_id"].(string)
	webhookEventID := parseWebhookEventID(t, webhookBody)
	if webhookEventID != eventID {
		t.Errorf("webhook: expected event_id=%q, got %q", eventID, webhookEventID)
	}

	// Shut down and query DuckDB.
	tp.shutdown(t, cancel, poolDone, writerDone)

	rows := tp.queryDB(t)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row in DuckDB, got %d", len(rows))
	}
	row := rows[0]
	if row.Verdict != "block" {
		t.Errorf("DuckDB: expected verdict=block, got %q", row.Verdict)
	}
	if row.EventID != eventID {
		t.Errorf("DuckDB: expected event_id=%q, got %q", eventID, row.EventID)
	}
	if row.LatencyUS <= 0 {
		t.Errorf("DuckDB: expected latency_us > 0, got %d", row.LatencyUS)
	}
	if row.LatencyUS >= 1_000_000 {
		t.Errorf("DuckDB: expected latency_us < 1,000,000, got %d", row.LatencyUS)
	}
}

// TestE2E_Backpressure (Test 37):
// Tiny buffers + rapid POSTs -> at least one 429 -> every 202-accepted event in DuckDB.
func TestE2E_Backpressure(t *testing.T) {
	opts := testPipelineOpts{
		eventChanCap:  1,
		resultChanCap: 1,
		workerCount:   1,
	}
	tp := newTestPipeline(t, approveAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	handler := tp.ingestServer.Handler()

	var accepted atomic.Int64
	var rejected atomic.Int64

	// Fire 20 POSTs rapidly in parallel.
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			event := makeEvent("test")
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

	// Verify at least one 429 was returned.
	if rejected.Load() == 0 {
		t.Errorf("expected at least one 429 (backpressure), but all %d requests were accepted", accepted.Load())
	}

	// Shut down and count DuckDB rows.
	tp.shutdown(t, cancel, poolDone, writerDone)

	dbCount := tp.queryDBCount(t)
	acceptedCount := int(accepted.Load())
	if dbCount != acceptedCount {
		t.Errorf("expected DuckDB rows=%d (matching accepted count), got %d", acceptedCount, dbCount)
	}
}

// TestE2E_GracefulShutdown (Test 39):
// Send 10 events directly to eventChan -> close eventChan -> wait for pool and writer.
// All 10 events must be in DuckDB.
func TestE2E_GracefulShutdown(t *testing.T) {
	opts := defaultOpts()
	tp := newTestPipeline(t, approveAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	// Send 10 events directly to eventChan.
	const numEvents = 10
	for i := 0; i < numEvents; i++ {
		event := types.Event{
			EventID:    fmt.Sprintf("shutdown-event-%d", i),
			EventType:  "test",
			Timestamp:  time.Now(),
			Payload:    map[string]any{"index": i},
			RawJSON:    []byte(`{}`),
			ReceivedAt: time.Now(),
		}
		tp.eventChan <- event
	}

	// Execute graceful shutdown.
	tp.shutdown(t, cancel, poolDone, writerDone)

	// Query DuckDB: all 10 events must be present.
	dbCount := tp.queryDBCount(t)
	if dbCount != numEvents {
		t.Errorf("expected %d rows in DuckDB after graceful shutdown, got %d", numEvents, dbCount)
	}
}
