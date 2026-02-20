package integration_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/vinaysrao1/fruitfly/types"
)

// TestIngestToExecutor_EventContract (Test 33):
// POST valid JSON via ingest handler -> executor produces result with correct verdict and event_id.
func TestIngestToExecutor_EventContract(t *testing.T) {
	opts := defaultOpts()
	tp := newTestPipeline(t, blockAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	handler := tp.ingestServer.Handler()
	event := makeEvent("login")

	resp := postEvent(t, handler, event)
	if resp.Code != 202 {
		t.Fatalf("expected 202, got %d: %s", resp.Code, resp.Body.String())
	}

	eventID := event["event_id"].(string)

	// Wait for webhook to confirm the event was processed before shutting down.
	// This ensures the result is in the pipeline before we trigger shutdown.
	webhookBody := waitForWebhook(t, tp.webhookCh, 5*time.Second)
	webhookVerdict := parseWebhookVerdict(t, webhookBody)
	if webhookVerdict != "block" {
		t.Errorf("webhook: expected verdict=block, got %q", webhookVerdict)
	}

	// Shut down to flush all results to DuckDB.
	tp.shutdown(t, cancel, poolDone, writerDone)

	rows := tp.queryDB(t)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row in DuckDB, got %d", len(rows))
	}

	row := rows[0]
	if row.Verdict != "block" {
		t.Errorf("expected verdict=block, got %q", row.Verdict)
	}
	if row.EventID != eventID {
		t.Errorf("expected event_id=%q, got %q", eventID, row.EventID)
	}

	// Verify triggered_rules contains the rule_id.
	var triggeredRules []struct {
		RuleID string `json:"RuleID"`
	}
	if err := json.Unmarshal([]byte(row.TriggeredRulesJSON), &triggeredRules); err != nil {
		t.Fatalf("parse triggered_rules JSON: %v, json: %q", err, row.TriggeredRulesJSON)
	}
	if len(triggeredRules) == 0 {
		t.Fatal("expected triggered_rules to contain at least one rule")
	}
	found := false
	for _, tr := range triggeredRules {
		if tr.RuleID == "block-all" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected triggered_rules to contain rule_id=block-all, got: %s", row.TriggeredRulesJSON)
	}
}

// TestExecutorToOutput_ResultContract (Test 34):
// Send types.Event directly to eventChan -> executor result written to DuckDB correctly.
func TestExecutorToOutput_ResultContract(t *testing.T) {
	opts := defaultOpts()
	tp := newTestPipeline(t, approveAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	// Send event directly to eventChan.
	event := types.Event{
		EventID:    "direct-event-123",
		EventType:  "test",
		Timestamp:  time.Now(),
		Payload:    map[string]any{"source": "direct"},
		RawJSON:    []byte(`{"event_id":"direct-event-123","event_type":"test"}`),
		ReceivedAt: time.Now(),
	}

	tp.eventChan <- event

	// Wait for webhook to confirm result was processed.
	webhookBody := waitForWebhook(t, tp.webhookCh, 5*time.Second)
	verdict := parseWebhookVerdict(t, webhookBody)
	if verdict != "approve" {
		t.Errorf("expected verdict=approve from webhook, got %q", verdict)
	}
	eventID := parseWebhookEventID(t, webhookBody)
	if eventID != "direct-event-123" {
		t.Errorf("expected event_id=direct-event-123 from webhook, got %q", eventID)
	}

	// Shut down and query DuckDB.
	tp.shutdown(t, cancel, poolDone, writerDone)

	rows := tp.queryDB(t)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row in DuckDB, got %d", len(rows))
	}

	row := rows[0]
	if row.EventID != "direct-event-123" {
		t.Errorf("expected event_id=direct-event-123, got %q", row.EventID)
	}
	if row.Verdict != "approve" {
		t.Errorf("expected verdict=approve, got %q", row.Verdict)
	}
	if row.LatencyUS <= 0 {
		t.Errorf("expected latency_us > 0, got %d", row.LatencyUS)
	}

	// Verify triggered_rules is valid JSON.
	if !json.Valid([]byte(row.TriggeredRulesJSON)) {
		t.Errorf("triggered_rules is not valid JSON: %q", row.TriggeredRulesJSON)
	}
}

// TestRulesToExecutor_SnapshotSwap (Test 35):
// Verify that rule hot-swap mid-traffic changes the verdict for subsequent events.
func TestRulesToExecutor_SnapshotSwap(t *testing.T) {
	opts := defaultOpts()
	tp := newTestPipeline(t, approveAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	handler := tp.ingestServer.Handler()

	// Step 1: POST event 1 -> expect approve verdict via webhook.
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

	// Step 2: Overwrite rule file with block rule.
	writeRule(t, tp.rulesDir, "rule.star", blockAllRule)

	// Step 3: Trigger reload and wait for snapshot to change.
	initialSnap := tp.snapshotPtr.Load()
	tp.reloader.Reload()

	// Poll until snapshot ID changes (max 2 seconds).
	deadline := time.Now().Add(2 * time.Second)
	swapped := false
	for time.Now().Before(deadline) {
		newSnap := tp.snapshotPtr.Load()
		if newSnap != nil && newSnap.ID != initialSnap.ID {
			swapped = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !swapped {
		t.Fatal("snapshot did not swap within 2 seconds after reload")
	}

	// Step 4: POST event 2 -> expect block verdict via webhook.
	event2 := makeEvent("test")
	resp2 := postEvent(t, handler, event2)
	if resp2.Code != 202 {
		t.Fatalf("event 2: expected 202, got %d", resp2.Code)
	}

	webhookBody2 := waitForWebhook(t, tp.webhookCh, 5*time.Second)
	verdict2 := parseWebhookVerdict(t, webhookBody2)
	if verdict2 != "block" {
		t.Errorf("event 2: expected verdict=block after hot reload, got %q", verdict2)
	}

	// Step 5: Shut down and verify both rows in DuckDB.
	tp.shutdown(t, cancel, poolDone, writerDone)

	rows := tp.queryDB(t)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows in DuckDB, got %d", len(rows))
	}

	// Collect verdicts.
	verdicts := make(map[string]int)
	for _, row := range rows {
		verdicts[row.Verdict]++
	}
	if verdicts["approve"] != 1 {
		t.Errorf("expected 1 approve row, got %d", verdicts["approve"])
	}
	if verdicts["block"] != 1 {
		t.Errorf("expected 1 block row, got %d", verdicts["block"])
	}

	// Verify rule IDs in triggered_rules match the expected rules.
	for _, row := range rows {
		var triggered []struct {
			RuleID string `json:"RuleID"`
		}
		if err := json.Unmarshal([]byte(row.TriggeredRulesJSON), &triggered); err != nil {
			t.Fatalf("parse triggered_rules: %v", err)
		}
		if len(triggered) == 0 {
			t.Errorf("row %s: expected triggered_rules to be non-empty, got: %s", row.EventID, row.TriggeredRulesJSON)
			continue
		}
		if row.Verdict == "approve" && !strings.Contains(triggered[0].RuleID, "approve") {
			t.Errorf("approve row: expected approve-all rule, got %q", triggered[0].RuleID)
		}
		if row.Verdict == "block" && !strings.Contains(triggered[0].RuleID, "block") {
			t.Errorf("block row: expected block-all rule, got %q", triggered[0].RuleID)
		}
	}
}
