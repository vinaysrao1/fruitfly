package executor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vinaysrao1/fruitfly/rules"
	"github.com/vinaysrao1/fruitfly/types"
)

func newTestCompiler() *rules.Compiler {
	return &rules.Compiler{UDFs: rules.DefaultUDFs()}
}

func compileRule(t *testing.T, source string) *rules.Snapshot {
	t.Helper()
	c := newTestCompiler()
	rule, err := c.CompileSource("test.star", source)
	if err != nil {
		t.Fatalf("CompileSource failed: %v", err)
	}
	return &rules.Snapshot{
		ID:       "test-snap",
		Rules:    []rules.Rule{*rule},
		LoadedAt: time.Now(),
	}
}

func compileRules(t *testing.T, sources []struct{ filename, source string }) *rules.Snapshot {
	t.Helper()
	c := newTestCompiler()
	snap := &rules.Snapshot{ID: "test-snap", LoadedAt: time.Now()}
	for _, s := range sources {
		rule, err := c.CompileSource(s.filename, s.source)
		if err != nil {
			t.Fatalf("CompileSource(%s) failed: %v", s.filename, err)
		}
		snap.Rules = append(snap.Rules, *rule)
	}
	return snap
}

func makePool(snap *rules.Snapshot, workers int) (*Pool, *atomic.Pointer[rules.Snapshot]) {
	var ptr atomic.Pointer[rules.Snapshot]
	ptr.Store(snap)
	pool := NewPool(workers, &ptr, 5*time.Second, 1*time.Second)
	return pool, &ptr
}

func runSingleEvent(t *testing.T, pool *Pool, event types.Event) types.Result {
	t.Helper()
	in := make(chan types.Event, 1)
	out := make(chan types.Result, 1)
	in <- event
	close(in)
	pool.Run(context.Background(), in, out)
	result, ok := <-out
	if !ok {
		t.Fatal("output channel closed without result")
	}
	return result
}

func testEvent(eventType string, payload map[string]any) types.Event {
	return types.Event{
		EventID:    "test-event-id",
		EventType:  eventType,
		Timestamp:  time.Now(),
		Payload:    payload,
		ReceivedAt: time.Now(),
	}
}

func TestSingleRule_CorrectVerdict(t *testing.T) {
	snap := compileRule(t, `
rule_id = "block-rule"
event_type = "post"
priority = 100
def evaluate(event):
    return verdict("block", reason="spam detected")
`)
	pool, _ := makePool(snap, 1)
	result := runSingleEvent(t, pool, testEvent("post", nil))

	if result.FinalVerdict != types.VerdictBlock {
		t.Errorf("FinalVerdict = %q, want %q", result.FinalVerdict, types.VerdictBlock)
	}
	if len(result.TriggeredRules) != 1 {
		t.Fatalf("TriggeredRules len = %d, want 1", len(result.TriggeredRules))
	}
	if result.TriggeredRules[0].RuleID != "block-rule" {
		t.Errorf("RuleID = %q, want %q", result.TriggeredRules[0].RuleID, "block-rule")
	}
	if result.TriggeredRules[0].Reason != "spam detected" {
		t.Errorf("Reason = %q, want %q", result.TriggeredRules[0].Reason, "spam detected")
	}
	if len(result.FailedRules) != 0 {
		t.Errorf("FailedRules len = %d, want 0", len(result.FailedRules))
	}
}

func TestPriorityResolution(t *testing.T) {
	snap := compileRules(t, []struct{ filename, source string }{
		{"high.star", `
rule_id = "high-approve"
event_type = "post"
priority = 100
def evaluate(event):
    return verdict("approve")
`},
		{"low.star", `
rule_id = "low-block"
event_type = "post"
priority = 50
def evaluate(event):
    return verdict("block", reason="lower priority")
`},
	})
	pool, _ := makePool(snap, 1)
	result := runSingleEvent(t, pool, testEvent("post", nil))

	if result.FinalVerdict != types.VerdictApprove {
		t.Errorf("FinalVerdict = %q, want approve (higher priority wins)", result.FinalVerdict)
	}
}

func TestSamePriority_TieBreaking(t *testing.T) {
	snap := compileRules(t, []struct{ filename, source string }{
		{"approve.star", `
rule_id = "same-approve"
event_type = "post"
priority = 100
def evaluate(event):
    return verdict("approve")
`},
		{"block.star", `
rule_id = "same-block"
event_type = "post"
priority = 100
def evaluate(event):
    return verdict("block", reason="tie-break")
`},
	})
	pool, _ := makePool(snap, 1)
	result := runSingleEvent(t, pool, testEvent("post", nil))

	if result.FinalVerdict != types.VerdictBlock {
		t.Errorf("FinalVerdict = %q, want block (block > approve at same priority)", result.FinalVerdict)
	}
}

func TestRuleError_InFailedRules(t *testing.T) {
	snap := compileRule(t, `
rule_id = "error-rule"
event_type = "post"
priority = 100
def evaluate(event):
    x = 1 / 0
    return verdict("block")
`)
	pool, _ := makePool(snap, 1)
	result := runSingleEvent(t, pool, testEvent("post", nil))

	if len(result.FailedRules) != 1 {
		t.Fatalf("FailedRules len = %d, want 1", len(result.FailedRules))
	}
	if result.FailedRules[0].Err == nil {
		t.Error("FailedRules[0].Err is nil")
	}
	if len(result.TriggeredRules) != 0 {
		t.Errorf("TriggeredRules len = %d, want 0", len(result.TriggeredRules))
	}
	if result.FinalVerdict != types.VerdictApprove {
		t.Errorf("FinalVerdict = %q, want approve (default when all fail)", result.FinalVerdict)
	}
}

func TestNoMatchingRules_DefaultApprove(t *testing.T) {
	snap := compileRule(t, `
rule_id = "post-only"
event_type = "post"
priority = 100
def evaluate(event):
    return verdict("block")
`)
	pool, _ := makePool(snap, 1)
	result := runSingleEvent(t, pool, testEvent("unknown", nil))

	if result.FinalVerdict != types.VerdictApprove {
		t.Errorf("FinalVerdict = %q, want approve", result.FinalVerdict)
	}
	if len(result.TriggeredRules) != 0 {
		t.Errorf("TriggeredRules len = %d, want 0", len(result.TriggeredRules))
	}
}

func TestMemo_CalledOnce(t *testing.T) {
	snap := compileRules(t, []struct{ filename, source string }{
		{"rule1.star", `
rule_id = "memo-rule-1"
event_type = "post"
priority = 100
def evaluate(event):
    val = memo("key", lambda: counter("user", "memo_test", 3600))
    if val == 1:
        return verdict("approve", reason="memo-saw-1")
    return verdict("block", reason="memo-saw-unexpected")
`},
		{"rule2.star", `
rule_id = "memo-rule-2"
event_type = "post"
priority = 100
def evaluate(event):
    val = memo("key", lambda: counter("user", "memo_test", 3600))
    if val == 1:
        return verdict("approve", reason="memo-saw-1")
    return verdict("block", reason="memo-saw-unexpected")
`},
	})
	pool, _ := makePool(snap, 1)
	result := runSingleEvent(t, pool, testEvent("post", nil))

	if result.FinalVerdict != types.VerdictApprove {
		t.Errorf("FinalVerdict = %q, want approve (memo should call fn once)", result.FinalVerdict)
	}
	for _, rr := range result.TriggeredRules {
		if rr.Reason != "memo-saw-1" {
			t.Errorf("rule %s reason = %q, want memo-saw-1", rr.RuleID, rr.Reason)
		}
	}
}

func TestMemo_IsolationBetweenEvents(t *testing.T) {
	snap := compileRule(t, `
rule_id = "isolation-rule"
event_type = "post"
priority = 100
def evaluate(event):
    val = memo("key", lambda: counter("user", "isolation_test", 3600))
    return verdict("approve", reason=str(val))
`)

	var ptr atomic.Pointer[rules.Snapshot]
	ptr.Store(snap)
	pool := NewPool(1, &ptr, 5*time.Second, 1*time.Second)

	in := make(chan types.Event, 2)
	out := make(chan types.Result, 2)
	in <- testEvent("post", nil)
	in <- testEvent("post", nil)
	close(in)

	pool.Run(context.Background(), in, out)

	r1 := <-out
	r2 := <-out

	if r1.TriggeredRules[0].Reason != "1" {
		t.Errorf("event 1: reason = %q, want 1", r1.TriggeredRules[0].Reason)
	}
	if r2.TriggeredRules[0].Reason != "2" {
		t.Errorf("event 2: reason = %q, want 2 (memo should be fresh)", r2.TriggeredRules[0].Reason)
	}
}

func TestCounterSum_CrossWorker(t *testing.T) {
	snap := compileRule(t, `
rule_id = "counter-rule"
event_type = "post"
priority = 100
def evaluate(event):
    val = counter("user", "post_type", 3600)
    return verdict("approve", reason=str(val))
`)

	var ptr atomic.Pointer[rules.Snapshot]
	ptr.Store(snap)
	pool := NewPool(2, &ptr, 5*time.Second, 1*time.Second)

	in := make(chan types.Event, 2)
	out := make(chan types.Result, 2)
	in <- testEvent("post", nil)
	in <- testEvent("post", nil)
	close(in)

	pool.Run(context.Background(), in, out)

	<-out
	<-out

	total := pool.CounterSum("user", "post_type", 3600)
	if total != 2 {
		t.Errorf("CounterSum = %d, want 2", total)
	}
}

func TestRuleTimeout_DoesNotBlockPipeline(t *testing.T) {
	// Compile a rule with an infinite loop (while True: pass equivalent in Starlark)
	snap := compileRule(t, `
rule_id = "timeout-rule"
event_type = "post"
priority = 100
def evaluate(event):
    x = 0
    for i in range(1000000000):
        x += 1
    return verdict("approve")
`)
	// Use a short rule timeout (100ms)
	var ptr atomic.Pointer[rules.Snapshot]
	ptr.Store(snap)
	pool := NewPool(1, &ptr, 5*time.Second, 100*time.Millisecond)

	result := runSingleEvent(t, pool, testEvent("post", nil))

	if len(result.FailedRules) != 1 {
		t.Fatalf("FailedRules len = %d, want 1", len(result.FailedRules))
	}
	if result.FailedRules[0].Err == nil {
		t.Error("expected timeout error")
	}
	if result.FinalVerdict != types.VerdictApprove {
		t.Errorf("FinalVerdict = %q, want approve (default when rule times out)", result.FinalVerdict)
	}
}

func TestOutputChannelClosed_AfterAllWorkers(t *testing.T) {
	snap := compileRule(t, `
rule_id = "simple-rule"
event_type = "post"
priority = 100
def evaluate(event):
    return verdict("approve")
`)
	pool, _ := makePool(snap, 1)

	in := make(chan types.Event, 3)
	out := make(chan types.Result, 3)
	in <- testEvent("post", nil)
	in <- testEvent("post", nil)
	in <- testEvent("post", nil)
	close(in)

	done := make(chan struct{})
	go func() {
		pool.Run(context.Background(), in, out)
		close(done)
	}()

	count := 0
	for range out {
		count++
	}
	<-done

	if count != 3 {
		t.Errorf("received %d results, want 3", count)
	}
}
