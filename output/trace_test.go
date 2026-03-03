package output

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vinaysrao1/fruitfly/types"
)

// makeTraceEvent builds a test event with the given ID.
func makeTraceEvent(id string) types.Event {
	return types.Event{
		EventID:    id,
		EventType:  "test",
		Timestamp:  time.Now(),
		Payload:    map[string]any{"key": "value"},
		ReceivedAt: time.Now(),
	}
}

// readTraceFile reads all JSONL entries from the trace file at path.
func readTraceFile(t *testing.T, path string) []TraceEntry {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open trace file: %v", err)
	}
	defer f.Close()

	var entries []TraceEntry
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		var entry TraceEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("unmarshal trace entry: %v, line: %s", err, line)
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan trace file: %v", err)
	}
	return entries
}

// TestTraceRecorder_Disabled verifies that a disabled recorder is a complete no-op.
func TestTraceRecorder_Disabled(t *testing.T) {
	tr, err := NewTraceRecorder(false, "should-not-be-created.jsonl")
	if err != nil {
		t.Fatalf("NewTraceRecorder(disabled): %v", err)
	}
	defer tr.Close()

	// Record should not panic or create any file.
	tr.Record(makeTraceEvent("e1"), makeResult("e1", "test", types.VerdictApprove), "snap-1")

	// File should not exist.
	if _, err := os.Stat("should-not-be-created.jsonl"); err == nil {
		os.Remove("should-not-be-created.jsonl")
		t.Error("trace file was created even though recorder is disabled")
	}
}

// TestTraceRecorder_Enabled_WritesEntries verifies that an enabled recorder
// writes JSONL entries that can be read back correctly.
func TestTraceRecorder_Enabled_WritesEntries(t *testing.T) {
	tracePath := filepath.Join(t.TempDir(), "trace.jsonl")

	tr, err := NewTraceRecorder(true, tracePath)
	if err != nil {
		t.Fatalf("NewTraceRecorder: %v", err)
	}

	const numEntries = 10
	for i := 0; i < numEntries; i++ {
		eventID := "evt-" + string(rune('a'+i))
		event := makeTraceEvent(eventID)
		result := makeResult(eventID, "test", types.VerdictApprove)
		tr.Record(event, result, "snap-id-123")
	}

	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	entries := readTraceFile(t, tracePath)
	if len(entries) != numEntries {
		t.Fatalf("expected %d entries, got %d", numEntries, len(entries))
	}

	for i, entry := range entries {
		expectedEventID := "evt-" + string(rune('a'+i))
		if entry.Event.EventID != expectedEventID {
			t.Errorf("entry[%d]: event.EventID = %q, want %q", i, entry.Event.EventID, expectedEventID)
		}
		if entry.Result.EventID != expectedEventID {
			t.Errorf("entry[%d]: result.EventID = %q, want %q", i, entry.Result.EventID, expectedEventID)
		}
		if entry.SnapshotID != "snap-id-123" {
			t.Errorf("entry[%d]: snapshot_id = %q, want %q", i, entry.SnapshotID, "snap-id-123")
		}
		if entry.Timestamp.IsZero() {
			t.Errorf("entry[%d]: timestamp is zero", i)
		}
	}
}

// TestTraceRecorder_AllFields verifies that all TraceEntry fields are serialized correctly.
func TestTraceRecorder_AllFields(t *testing.T) {
	tracePath := filepath.Join(t.TempDir(), "trace.jsonl")

	tr, err := NewTraceRecorder(true, tracePath)
	if err != nil {
		t.Fatalf("NewTraceRecorder: %v", err)
	}

	event := types.Event{
		EventID:    "full-event-1",
		EventType:  "purchase",
		Timestamp:  time.Date(2026, 2, 27, 10, 0, 0, 0, time.UTC),
		Payload:    map[string]any{"amount": 42.5, "currency": "USD"},
		ReceivedAt: time.Date(2026, 2, 27, 10, 0, 0, 100, time.UTC),
	}

	result := types.Result{
		EventID:      "full-event-1",
		EventType:    "purchase",
		FinalVerdict: types.VerdictBlock,
		TriggeredRules: []types.RuleResult{
			{RuleID: "block-rule", Verdict: types.VerdictBlock, Reason: "spam"},
		},
		FailedRules: []types.RuleResult{},
		Payload:     event.Payload,
		LatencyUS:   1234,
		ProcessedAt: time.Date(2026, 2, 27, 10, 0, 0, 200, time.UTC),
	}

	tr.Record(event, result, "snap-abc-123")

	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	entries := readTraceFile(t, tracePath)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	e := entries[0]
	if e.Event.EventID != "full-event-1" {
		t.Errorf("event.EventID: got %q, want %q", e.Event.EventID, "full-event-1")
	}
	if e.Event.EventType != "purchase" {
		t.Errorf("event.EventType: got %q, want %q", e.Event.EventType, "purchase")
	}
	if e.Result.FinalVerdict != types.VerdictBlock {
		t.Errorf("result.FinalVerdict: got %q, want %q", e.Result.FinalVerdict, types.VerdictBlock)
	}
	if len(e.Result.TriggeredRules) != 1 {
		t.Errorf("result.TriggeredRules: got %d, want 1", len(e.Result.TriggeredRules))
	} else if e.Result.TriggeredRules[0].RuleID != "block-rule" {
		t.Errorf("result.TriggeredRules[0].RuleID: got %q, want %q", e.Result.TriggeredRules[0].RuleID, "block-rule")
	}
	if e.SnapshotID != "snap-abc-123" {
		t.Errorf("snapshot_id: got %q, want %q", e.SnapshotID, "snap-abc-123")
	}
}

// TestTraceRecorder_Append verifies that multiple recorder sessions append to the same file.
func TestTraceRecorder_Append(t *testing.T) {
	tracePath := filepath.Join(t.TempDir(), "trace.jsonl")

	// First session: write 5 entries.
	tr1, err := NewTraceRecorder(true, tracePath)
	if err != nil {
		t.Fatalf("NewTraceRecorder (session 1): %v", err)
	}
	for i := 0; i < 5; i++ {
		tr1.Record(makeTraceEvent("s1-evt-"+string(rune('a'+i))), makeResult("s1-evt", "test", types.VerdictApprove), "snap-1")
	}
	if err := tr1.Close(); err != nil {
		t.Fatalf("Close (session 1): %v", err)
	}

	// Second session: append 5 more entries.
	tr2, err := NewTraceRecorder(true, tracePath)
	if err != nil {
		t.Fatalf("NewTraceRecorder (session 2): %v", err)
	}
	for i := 0; i < 5; i++ {
		tr2.Record(makeTraceEvent("s2-evt-"+string(rune('a'+i))), makeResult("s2-evt", "test", types.VerdictBlock), "snap-2")
	}
	if err := tr2.Close(); err != nil {
		t.Fatalf("Close (session 2): %v", err)
	}

	// Should have 10 total entries.
	entries := readTraceFile(t, tracePath)
	if len(entries) != 10 {
		t.Errorf("expected 10 entries (5+5 appended), got %d", len(entries))
	}
}

// TestTraceRecorder_ConcurrentWrites verifies thread safety under concurrent writes.
func TestTraceRecorder_ConcurrentWrites(t *testing.T) {
	tracePath := filepath.Join(t.TempDir(), "trace.jsonl")

	tr, err := NewTraceRecorder(true, tracePath)
	if err != nil {
		t.Fatalf("NewTraceRecorder: %v", err)
	}

	const goroutines = 10
	const perGoroutine = 10
	done := make(chan struct{})

	for g := 0; g < goroutines; g++ {
		go func(gid int) {
			for i := 0; i < perGoroutine; i++ {
				eventID := "concurrent-" + string(rune('a'+gid)) + "-" + string(rune('0'+i))
				tr.Record(makeTraceEvent(eventID), makeResult(eventID, "test", types.VerdictApprove), "snap-concurrent")
			}
			done <- struct{}{}
		}(g)
	}

	for g := 0; g < goroutines; g++ {
		<-done
	}

	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	entries := readTraceFile(t, tracePath)
	if len(entries) != goroutines*perGoroutine {
		t.Errorf("expected %d entries, got %d (some writes lost)", goroutines*perGoroutine, len(entries))
	}
}

// TestTraceRecorder_InvalidPath verifies that an error is returned for invalid paths.
func TestTraceRecorder_InvalidPath(t *testing.T) {
	_, err := NewTraceRecorder(true, "/nonexistent/dir/trace.jsonl")
	if err == nil {
		t.Error("expected error for invalid trace path, got nil")
	}
}

// TestTraceRecorder_CloseDisabled verifies that Close on a disabled recorder is safe.
func TestTraceRecorder_CloseDisabled(t *testing.T) {
	tr, err := NewTraceRecorder(false, "")
	if err != nil {
		t.Fatalf("NewTraceRecorder(disabled): %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Errorf("Close on disabled recorder: %v", err)
	}
}
