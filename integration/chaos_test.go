package integration_test

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vinaysrao1/fruitfly/types"
)

// TestWebhookDown_DuckDBStillWrites (#40):
// Webhook URL points to a closed port (connection refused).
// All results must still be written to DuckDB.
func TestWebhookDown_DuckDBStillWrites(t *testing.T) {
	// Use port 1 which is reserved and always refuses connections.
	webhookURL := "http://127.0.0.1:1/hook"

	opts := defaultOpts()
	tp := newTestPipeline(t, approveAllRule, opts, webhookURL)
	cancel, poolDone, writerDone := tp.start(t)

	// Send 5 events directly to eventChan.
	const numEvents = 5
	for i := 0; i < numEvents; i++ {
		event := types.Event{
			EventID:    fmt.Sprintf("webhook-down-event-%d", i),
			EventType:  "test",
			Timestamp:  time.Now(),
			Payload:    map[string]any{"index": i},
			RawJSON:    []byte(`{}`),
			ReceivedAt: time.Now(),
		}
		tp.eventChan <- event
	}

	// Shutdown drains the pipeline. DuckDB writes happen synchronously in the
	// writer's Run loop. Webhook goroutines are cancelled via context cancellation.
	tp.shutdown(t, cancel, poolDone, writerDone)

	// Assert all 5 events are in DuckDB despite webhook failures.
	dbCount := tp.queryDBCount(t)
	if dbCount != numEvents {
		t.Errorf("expected %d rows in DuckDB despite webhook down, got %d", numEvents, dbCount)
	}
}

// TestMalformedEvents_Rejected (#41):
// Table-driven test for malformed event payloads.
// Each malformed case must return 400. One valid event must succeed.
func TestMalformedEvents_Rejected(t *testing.T) {
	opts := defaultOpts()
	tp := newTestPipeline(t, approveAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	handler := tp.ingestServer.Handler()

	cases := []struct {
		name        string
		body        string
		contentType string
	}{
		{
			name:        "empty body",
			body:        "",
			contentType: "application/json",
		},
		{
			name:        "non-JSON",
			body:        "this is not json at all",
			contentType: "application/json",
		},
		{
			name:        "JSON array instead of object",
			body:        `[{"event_type":"test","timestamp":"2024-01-01T00:00:00Z"}]`,
			contentType: "application/json",
		},
		{
			name:        "event_type as integer",
			body:        `{"event_type":42,"timestamp":"2024-01-01T00:00:00Z"}`,
			contentType: "application/json",
		},
		{
			name:        "event_type as null",
			body:        `{"event_type":null,"timestamp":"2024-01-01T00:00:00Z"}`,
			contentType: "application/json",
		},
		{
			name:        "missing event_type",
			body:        `{"timestamp":"2024-01-01T00:00:00Z","payload":{}}`,
			contentType: "application/json",
		},
		{
			name:        "missing timestamp",
			body:        `{"event_type":"test","payload":{}}`,
			contentType: "application/json",
		},
		{
			name:        "invalid timestamp string",
			body:        `{"event_type":"test","timestamp":"not-a-valid-timestamp"}`,
			contentType: "application/json",
		},
		{
			name:        "timestamp as integer",
			body:        `{"event_type":"test","timestamp":1234567890}`,
			contentType: "application/json",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/events", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", tc.contentType)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			if w.Code != 400 {
				t.Errorf("case %q: expected 400, got %d (body: %s)", tc.name, w.Code, w.Body.String())
			}
		})
	}

	// Send one valid event and verify it's processed correctly.
	validEvent := makeEvent("test")
	resp := postEvent(t, handler, validEvent)
	if resp.Code != 202 {
		t.Fatalf("valid event: expected 202, got %d: %s", resp.Code, resp.Body.String())
	}

	// Wait for the valid event to be processed via webhook.
	waitForWebhook(t, tp.webhookCh, 5*time.Second)

	tp.shutdown(t, cancel, poolDone, writerDone)

	// Assert exactly 1 DuckDB row (only the valid event).
	dbCount := tp.queryDBCount(t)
	if dbCount != 1 {
		t.Errorf("expected exactly 1 row in DuckDB (valid event only), got %d", dbCount)
	}
}

// TestRuleCompileError_DuringReload (#42):
// Start with valid rules, send event, verify approve.
// Replace rule file with invalid Starlark, trigger reload.
// Assert old snapshot preserved, subsequent events still approved.
func TestRuleCompileError_DuringReload(t *testing.T) {
	opts := defaultOpts()
	tp := newTestPipeline(t, approveAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	handler := tp.ingestServer.Handler()

	// Step 1: Send event with valid approve rule.
	event1 := makeEvent("test")
	resp1 := postEvent(t, handler, event1)
	if resp1.Code != 202 {
		t.Fatalf("event 1: expected 202, got %d", resp1.Code)
	}

	webhookBody1 := waitForWebhook(t, tp.webhookCh, 5*time.Second)
	verdict1 := parseWebhookVerdict(t, webhookBody1)
	if verdict1 != "approve" {
		t.Errorf("event 1: expected verdict=approve, got %q", verdict1)
	}

	// Record the initial snapshot ID.
	initialSnap := tp.snapshotPtr.Load()
	if initialSnap == nil {
		t.Fatal("initial snapshot is nil")
	}
	initialSnapID := initialSnap.ID

	// Step 2: Overwrite rule file with invalid Starlark.
	writeRule(t, tp.rulesDir, "rule.star", "this is not valid starlark!!!")

	// Trigger reload.
	tp.reloader.Reload()

	// Wait for reload attempt to complete.
	time.Sleep(500 * time.Millisecond)

	// Assert snapshot ID is unchanged (bad reload must preserve old snapshot).
	currentSnap := tp.snapshotPtr.Load()
	if currentSnap == nil {
		t.Fatal("snapshot became nil after bad reload")
	}
	if currentSnap.ID != initialSnapID {
		t.Errorf("expected snapshot ID to remain %q after bad reload, but it changed to %q",
			initialSnapID, currentSnap.ID)
	}

	// Step 3: Send another event and verify still approved (old rules still active).
	event2 := makeEvent("test")
	resp2 := postEvent(t, handler, event2)
	if resp2.Code != 202 {
		t.Fatalf("event 2: expected 202, got %d", resp2.Code)
	}

	webhookBody2 := waitForWebhook(t, tp.webhookCh, 5*time.Second)
	verdict2 := parseWebhookVerdict(t, webhookBody2)
	if verdict2 != "approve" {
		t.Errorf("event 2: expected verdict=approve after bad reload (old rules still active), got %q", verdict2)
	}

	tp.shutdown(t, cancel, poolDone, writerDone)

	// Assert 2 DuckDB rows.
	dbCount := tp.queryDBCount(t)
	if dbCount != 2 {
		t.Errorf("expected 2 rows in DuckDB, got %d", dbCount)
	}
}

// TestBurstTraffic_200EventsIn100ms (#43):
// Send 200 events in a tight loop (no rate limiting).
// Assert >= 100 accepted, accepted count == DuckDB row count, no panics/deadlocks.
func TestBurstTraffic_200EventsIn100ms(t *testing.T) {
	opts := testPipelineOpts{
		eventChanCap:  100,
		resultChanCap: 100,
		workerCount:   2,
	}
	tp := newTestPipeline(t, approveAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	handler := tp.ingestServer.Handler()

	const burstCount = 200

	var accepted int
	var rejected int

	// Send 200 events in a tight loop.
	for i := 0; i < burstCount; i++ {
		event := makeEvent("test")
		resp := postEvent(t, handler, event)
		switch resp.Code {
		case 202:
			accepted++
		case 429:
			rejected++
		default:
			t.Errorf("unexpected status %d", resp.Code)
		}
	}

	// Assert at least 100 accepted (channel buffer of 100).
	if accepted < 100 {
		t.Errorf("expected at least 100 accepted events, got %d", accepted)
	}

	// Drain webhook deliveries for all accepted events.
	for i := 0; i < accepted; i++ {
		select {
		case <-tp.webhookCh:
		case <-time.After(15 * time.Second):
			t.Fatalf("timed out waiting for webhook delivery %d/%d", i+1, accepted)
		}
	}

	tp.shutdown(t, cancel, poolDone, writerDone)

	// Assert accepted count == DuckDB row count (no silent drops).
	dbCount := tp.queryDBCount(t)
	if dbCount != accepted {
		t.Errorf("accepted count %d != DuckDB row count %d (silent data loss)", accepted, dbCount)
	}
}

// TestRuleTimeout_DoesNotBlockPipeline (#44):
// Rule with a very long loop (effectively infinite) should be cancelled by ruleTimeout (1s).
// Pipeline should continue processing subsequent events normally.
func TestRuleTimeout_DoesNotBlockPipeline(t *testing.T) {
	// Starlark does not allow 'while' by default, so use a for loop over a
	// very large range to simulate an effectively infinite loop.
	// The ruleTimeout (1s) will cancel the Starlark thread via thread.Cancel().
	const infiniteLoopRule = `
rule_id = "infinite-loop"
event_type = "*"
priority = 100
def evaluate(event):
    for _ in range(1000000000000000000):
        pass
    return verdict("block")
`

	opts := defaultOpts()
	tp := newTestPipeline(t, infiniteLoopRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	handler := tp.ingestServer.Handler()

	// Send first event - the rule will timeout after ruleTimeout (1s).
	// The event should still be processed with default approve verdict
	// (all rules failed, so resolveVerdict returns approve).
	event1 := makeEvent("test")
	resp1 := postEvent(t, handler, event1)
	if resp1.Code != 202 {
		t.Fatalf("event 1: expected 202, got %d", resp1.Code)
	}

	// Wait for webhook - rule times out at 1s, so result arrives within ~eventTimeout (5s).
	webhookBody1 := waitForWebhook(t, tp.webhookCh, 5*time.Second)
	verdict1 := parseWebhookVerdict(t, webhookBody1)
	// When all rules fail (timeout), FinalVerdict defaults to "approve".
	if verdict1 != "approve" {
		t.Errorf("event 1: expected verdict=approve (rule timed out, default applies), got %q", verdict1)
	}

	// Send second event to prove the pipeline is not blocked.
	event2 := makeEvent("test")
	resp2 := postEvent(t, handler, event2)
	if resp2.Code != 202 {
		t.Fatalf("event 2: expected 202, got %d (pipeline might be blocked)", resp2.Code)
	}

	webhookBody2 := waitForWebhook(t, tp.webhookCh, 5*time.Second)
	verdict2 := parseWebhookVerdict(t, webhookBody2)
	if verdict2 != "approve" {
		t.Errorf("event 2: expected verdict=approve (rule timed out, default applies), got %q", verdict2)
	}

	tp.shutdown(t, cancel, poolDone, writerDone)

	// Assert 2 DuckDB rows.
	dbCount := tp.queryDBCount(t)
	if dbCount != 2 {
		t.Errorf("expected 2 rows in DuckDB, got %d", dbCount)
	}
}
