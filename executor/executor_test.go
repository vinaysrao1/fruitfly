package executor

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vinaysrao1/fruitfly/rules"
	"github.com/vinaysrao1/fruitfly/types"
	"go.starlark.net/starlark"
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

// --- T9b: Rule returns None -> FailedRules ---

// T9b: evaluate() returns None -> interpretVerdict fails -> rule in FailedRules.
func TestRuleReturnsNone(t *testing.T) {
	snap := compileRule(t, `
rule_id = "none-rule"
event_type = "post"
priority = 100
def evaluate(event):
    return None
`)
	pool, _ := makePool(snap, 1)
	result := runSingleEvent(t, pool, testEvent("post", nil))

	if len(result.FailedRules) != 1 {
		t.Fatalf("FailedRules len = %d, want 1", len(result.FailedRules))
	}
	if result.FailedRules[0].Err == nil {
		t.Fatal("FailedRules[0].Err is nil, expected error")
	}
	if !strings.Contains(result.FailedRules[0].Err.Error(), "must return a verdict()") {
		t.Errorf("error = %q, want substring 'must return a verdict()'", result.FailedRules[0].Err.Error())
	}
	if result.FinalVerdict != types.VerdictApprove {
		t.Errorf("FinalVerdict = %q, want approve", result.FinalVerdict)
	}
}

// --- T9c: Conflicting same-priority verdicts -> block wins ---
// Already covered by TestSamePriority_TieBreaking, but added here for spec completeness.

// --- T9d: Rule panic recovery ---

// T9d: A rule that triggers a Go-level panic must be caught by evalRule's defer recover().
// We directly call evalRule with a custom panicking builtin injected into the UDF dict.
// This exercises the actual recover() path — not just Starlark errors.
//
// Strategy: build a compiler that declares "panic_now" as a predeclared name (pointing to
// a no-op placeholder so compilation succeeds), then at eval time pass a UDF dict where
// "panic_now" is a builtin that triggers a real Go panic.
func TestRulePanic_RecoveryViaPanickingUDF(t *testing.T) {
	// Build compiler with a placeholder "panic_now" so the Starlark program compiles.
	placeholderPanic := starlark.NewBuiltin("panic_now", func(
		thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple,
	) (starlark.Value, error) {
		return starlark.None, nil // placeholder: never actually called during compile
	})
	udfsWithPanic := rules.DefaultUDFs()
	udfsWithPanic["panic_now"] = placeholderPanic

	c := &rules.Compiler{UDFs: udfsWithPanic}
	rule, err := c.CompileSource("panic.star", `
rule_id = "panic-rule"
event_type = "post"
priority = 100
def evaluate(event):
    return panic_now()
`)
	if err != nil {
		t.Fatalf("CompileSource: %v", err)
	}

	snap := &rules.Snapshot{
		ID:       "panic-snap",
		Rules:    []rules.Rule{*rule},
		LoadedAt: time.Now(),
	}

	var ptr atomic.Pointer[rules.Snapshot]
	ptr.Store(snap)
	pool := NewPool(1, &ptr, 5*time.Second, 1*time.Second)

	// Build a worker and replace "panic_now" with a real Go-panicking builtin.
	w := &worker{
		id:         0,
		pool:       pool,
		memo:       make(map[string]any),
		regexCache: make(map[string]*regexp.Regexp),
		evalCache:  make(map[string]starlark.Callable),
	}
	w.udfs = buildUDFs(w)
	// Replace placeholder with a builtin that triggers a real Go panic.
	w.udfs["panic_now"] = starlark.NewBuiltin("panic_now", func(
		thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple,
	) (starlark.Value, error) {
		panic("intentional Go panic from test UDF")
	})

	starlarkEvt := eventToStarlark(testEvent("post", nil))
	rr := w.evalRule(context.Background(), *rule, starlarkEvt, w.udfs)

	// The Go panic must be caught by defer recover() in evalRule.
	if rr.Err == nil {
		t.Fatal("expected rr.Err to be set after Go panic, got nil")
	}
	if !strings.Contains(rr.Err.Error(), "panicked") {
		t.Errorf("error = %q, want substring 'panicked'", rr.Err.Error())
	}
	if !strings.Contains(rr.Err.Error(), "intentional Go panic") {
		t.Errorf("error = %q, want substring 'intentional Go panic'", rr.Err.Error())
	}
}

// T9d_StarlarkError: Verify that a Starlark-level error (1/0) still reaches FailedRules
// via the pool pipeline (this is the original test coverage preserved for completeness).
func TestRulePanic_StarlarkErrorViaPipeline(t *testing.T) {
	snap := compileRule(t, `
rule_id = "error-rule"
event_type = "post"
priority = 100
def evaluate(event):
    return memo("k", lambda: 1/0)
`)
	pool, _ := makePool(snap, 1)
	result := runSingleEvent(t, pool, testEvent("post", nil))

	if len(result.FailedRules) != 1 {
		t.Fatalf("FailedRules len = %d, want 1", len(result.FailedRules))
	}
	if result.FailedRules[0].Err == nil {
		t.Fatal("expected error in FailedRules[0]")
	}
	// Pool should be unaffected — send another event.
	result2 := runSingleEvent(t, pool, testEvent("post", nil))
	if len(result2.FailedRules) == 0 && result2.FinalVerdict == "" {
		t.Error("second event: pool appears to have hung or broken")
	}
}

// --- T9e: Rule timeout (infinite loop) contains DeadlineExceeded ---

// T9e: Augments TestRuleTimeout_DoesNotBlockPipeline to verify error message.
func TestRuleTimeout_ErrorContainsDeadlineExceeded(t *testing.T) {
	snap := compileRule(t, `
rule_id = "infinite-loop-rule"
event_type = "post"
priority = 100
def evaluate(event):
    x = 0
    for i in range(1000000000):
        x += 1
    return verdict("approve")
`)
	var ptr atomic.Pointer[rules.Snapshot]
	ptr.Store(snap)
	pool := NewPool(1, &ptr, 5*time.Second, 100*time.Millisecond)

	result := runSingleEvent(t, pool, testEvent("post", nil))

	if len(result.FailedRules) != 1 {
		t.Fatalf("FailedRules len = %d, want 1", len(result.FailedRules))
	}
	if result.FailedRules[0].Err == nil {
		t.Fatal("expected timeout error")
	}
	errStr := result.FailedRules[0].Err.Error()
	// The error wraps context.DeadlineExceeded. Check for either the wrapped type name
	// or the standard "context deadline exceeded" string.
	if !strings.Contains(errStr, "deadline exceeded") && !strings.Contains(errStr, "DeadlineExceeded") {
		t.Errorf("error = %q, want substring 'deadline exceeded' or 'DeadlineExceeded'", errStr)
	}
}

// --- T4: Payload boundary mutations ---

// T4: anyToStarlark and event processing with edge-case payloads.
func TestPayloadBoundaries(t *testing.T) {
	// A simple rule that just approves — we care that it doesn't panic.
	approveRuleSrc := `
rule_id = "boundary-rule"
event_type = "post"
priority = 100
def evaluate(event):
    return verdict("approve")
`

	// Build deep nesting helper.
	buildDeepNested := func(depth int) map[string]any {
		inner := map[string]any{"leaf": "value"}
		for i := 0; i < depth-1; i++ {
			inner = map[string]any{"nested": inner}
		}
		return inner
	}

	tests := []struct {
		name    string
		payload map[string]any
	}{
		{
			name:    "empty payload",
			payload: map[string]any{},
		},
		{
			name:    "null field",
			payload: map[string]any{"key": nil},
		},
		{
			name:    "type coercion string",
			payload: map[string]any{"char_count": "5"},
		},
		{
			name:    "extreme float64",
			payload: map[string]any{"value": math.MaxFloat64},
		},
		{
			name:    "unicode large string",
			payload: map[string]any{"text": strings.Repeat("X", 10000)},
		},
		{
			name:    "deep nesting 50 levels",
			payload: buildDeepNested(50),
		},
		{
			name:    "list payload value",
			payload: map[string]any{"items": []any{"a", "b", "c"}},
		},
		{
			name:    "bool payload value",
			payload: map[string]any{"flag": true},
		},
		{
			name:    "int64 payload value",
			payload: map[string]any{"count": int64(42)},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			snap := compileRule(t, approveRuleSrc)
			pool, _ := makePool(snap, 1)
			result := runSingleEvent(t, pool, testEvent("post", tt.payload))

			if result.FinalVerdict == "" {
				t.Errorf("FinalVerdict is empty — pool may have crashed")
			}
			if len(result.FailedRules) != 0 {
				t.Errorf("FailedRules len = %d, want 0: %v", len(result.FailedRules), result.FailedRules[0].Err)
			}
		})
	}
}

// --- T19: Counter state across reloads ---

// T19: Counter increments persist across snapshot swaps (counters are per-worker).
// We use an unbuffered input channel with pool.Run() in a background goroutine so we
// can synchronize the snapshot swap between specific events. This guarantees that
// some events see snapA and some see snapB, while the worker's counters accumulate
// across the entire run.
func TestCounterState_AcrossReloads(t *testing.T) {
	ruleSrc := func(ruleID string) string {
		return `
rule_id = "` + ruleID + `"
event_type = "post"
priority = 100
def evaluate(event):
    val = counter("user", "post", 3600)
    return verdict("approve", reason=str(val))
`
	}

	snapA := compileRule(t, ruleSrc("counter-rule-A"))
	snapB := compileRule(t, ruleSrc("counter-rule-B"))

	var ptr atomic.Pointer[rules.Snapshot]
	ptr.Store(snapA)
	pool := NewPool(1, &ptr, 5*time.Second, 1*time.Second)

	// Use an unbuffered input channel and run pool in background.
	// This lets us control exactly when snapshot is swapped between events.
	in := make(chan types.Event)
	out := make(chan types.Result, 10)

	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.Run(context.Background(), in, out)
	}()

	const eventsBeforeSwap = 3
	const eventsAfterSwap = 2
	const totalEvents = eventsBeforeSwap + eventsAfterSwap

	// Send first batch with snapA, wait for each result to confirm processing.
	for i := 0; i < eventsBeforeSwap; i++ {
		in <- testEvent("post", nil)
		<-out // wait for result before sending next event
	}

	// Now swap snapshot to snapB. The worker will pick up the new snapshot on next event.
	ptr.Store(snapB)

	// Send second batch with snapB.
	for i := 0; i < eventsAfterSwap; i++ {
		in <- testEvent("post", nil)
		<-out
	}

	close(in)
	<-done

	// Counter should have accumulated across all events regardless of snapshot swap.
	finalSum := pool.CounterSum("user", "post", 3600)
	if finalSum != totalEvents {
		t.Errorf("CounterSum after %d events with snapshot swap = %d, want %d", totalEvents, finalSum, totalEvents)
	}
}

// --- T20: Counter concurrency stress ---

// T20: Concurrent counterIncrement (writers) and CounterSum (readers) on the same pool.
// Verifies no data race (run with -race).
//
// Design: directly exercise the counter sync primitives without going through pool.Run,
// since pool.Run writes pool.workers which creates a race with CounterSum readers.
// Instead, manually create workers and use the counter primitives directly.
func TestCounterConcurrency_Stress(t *testing.T) {
	var ptr atomic.Pointer[rules.Snapshot]
	pool := NewPool(4, &ptr, 5*time.Second, 1*time.Second)

	// Manually create workers so we can exercise counter concurrency.
	// This direct assignment is safe because no goroutines are started yet —
	// all writer and reader goroutines are launched AFTER pool.workers is fully
	// initialized, so there is no concurrent access at this point.
	pool.workers = make([]*worker, 4)
	for i := range pool.workers {
		w := &worker{
			id:   i,
			pool: pool,
		}
		pool.workers[i] = w
	}

	const goroutines = 8
	const incrementsPerGoroutine = 100
	const totalExpected = goroutines * incrementsPerGoroutine

	var wg sync.WaitGroup
	now := time.Now().Unix()

	// Writers: goroutines incrementing counters via different workers.
	for i := 0; i < goroutines; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := pool.workers[i%len(pool.workers)]
			for j := 0; j < incrementsPerGoroutine; j++ {
				w.counterIncrement("stress-entity", "post", now)
			}
		}()
	}

	// Readers: goroutines calling CounterSum concurrently.
	stopReaders := make(chan struct{})
	var readerWg sync.WaitGroup
	for i := 0; i < 4; i++ {
		readerWg.Add(1)
		go func() {
			defer readerWg.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
					pool.CounterSum("stress-entity", "post", 3600)
				}
			}
		}()
	}

	// Wait for all writers to finish.
	wg.Wait()
	close(stopReaders)
	readerWg.Wait()

	// After all writes, CounterSum should equal totalExpected.
	finalSum := pool.CounterSum("stress-entity", "post", 3600)
	if finalSum != totalExpected {
		t.Errorf("CounterSum = %d, want %d (no lost increments)", finalSum, totalExpected)
	}
}

// --- T29: Counter GC threshold ---

// T29: Directly exercise gcCounters() to verify expired bucket deletion.
// pool.Run() resets workers on each call, so we use a single-event run to populate
// pool.workers, then inject expired buckets and call gcCounters() directly.
func TestCounterGC_Threshold(t *testing.T) {
	snap := compileRule(t, `
rule_id = "gc-rule"
event_type = "post"
priority = 100
def evaluate(event):
    return verdict("approve")
`)

	var ptr atomic.Pointer[rules.Snapshot]
	ptr.Store(snap)
	pool := NewPool(1, &ptr, 5*time.Second, 1*time.Second)

	// Run 1 event to initialize pool.workers so we can access workers[0].
	in := make(chan types.Event, 1)
	out := make(chan types.Result, 1)
	in <- testEvent("post", nil)
	close(in)
	pool.Run(context.Background(), in, out)
	for range out {
	}

	// Inject an expired counter bucket into worker 0.
	w := pool.workers[0]
	expiredBucket := counterKey{
		EntityID:  "expired-entity",
		EventType: "post",
		Bucket:    time.Now().Unix() - counterMaxWindowSeconds - 1,
	}
	var expiredCounter atomic.Int64
	expiredCounter.Store(42)
	w.counters.Store(expiredBucket, &expiredCounter)

	// Verify the expired bucket is present before GC.
	var foundBefore bool
	w.counters.Range(func(k, v any) bool {
		if k.(counterKey) == expiredBucket {
			foundBefore = true
		}
		return true
	})
	if !foundBefore {
		t.Fatal("expired bucket not found in counters before GC")
	}

	// Trigger GC directly.
	w.gcCounters()

	// Verify expired bucket was deleted.
	var foundAfter bool
	w.counters.Range(func(k, v any) bool {
		if k.(counterKey) == expiredBucket {
			foundAfter = true
		}
		return true
	})
	if foundAfter {
		t.Error("expired bucket still present after gcCounters() — expected it to be deleted")
	}

	// Verify a non-expired bucket is retained.
	freshBucket := counterKey{
		EntityID:  "fresh-entity",
		EventType: "post",
		Bucket:    (time.Now().Unix() / counterBucketSeconds) * counterBucketSeconds,
	}
	var freshCounter atomic.Int64
	freshCounter.Store(7)
	w.counters.Store(freshBucket, &freshCounter)
	w.gcCounters()

	var foundFresh bool
	w.counters.Range(func(k, v any) bool {
		if k.(counterKey) == freshBucket {
			foundFresh = true
		}
		return true
	})
	if !foundFresh {
		t.Error("fresh bucket was deleted by gcCounters() — expected it to be retained")
	}
}

// --- T30: Nil snapshot -> default approve ---

// T30: When snapshot pointer is nil, event should return default approve verdict.
func TestNilSnapshot_DefaultApprove(t *testing.T) {
	var ptr atomic.Pointer[rules.Snapshot]
	// Do not store any snapshot — ptr.Load() returns nil.
	pool := NewPool(1, &ptr, 5*time.Second, 1*time.Second)

	result := runSingleEvent(t, pool, testEvent("post", nil))

	if result.FinalVerdict != types.VerdictApprove {
		t.Errorf("FinalVerdict = %q, want approve (nil snapshot should default to approve)", result.FinalVerdict)
	}
	if len(result.TriggeredRules) != 0 {
		t.Errorf("TriggeredRules len = %d, want 0", len(result.TriggeredRules))
	}
	if len(result.FailedRules) != 0 {
		t.Errorf("FailedRules len = %d, want 0", len(result.FailedRules))
	}
	if result.EventID == "" {
		t.Error("EventID should be preserved even with nil snapshot")
	}
}

// compileRulesWithID compiles multiple rules into a snapshot with a specific ID.
// Used when tests need distinct snapshot IDs to test cache invalidation.
func compileRulesWithID(t *testing.T, snapID string, sources []struct{ filename, source string }) *rules.Snapshot {
	t.Helper()
	c := newTestCompiler()
	snap := &rules.Snapshot{ID: snapID, LoadedAt: time.Now()}
	for _, s := range sources {
		rule, err := c.CompileSource(s.filename, s.source)
		if err != nil {
			t.Fatalf("CompileSource(%s) failed: %v", s.filename, err)
		}
		snap.Rules = append(snap.Rules, *rule)
	}
	return snap
}

// --- T31: evalCache rule swap ---

// T31: Swap from snapshot A (rules A,B) to snapshot B (rules A,D) while pool is running.
// Verify evalCache is cleared when snapshot changes and rule D executes correctly.
func TestEvalCache_RuleSwap(t *testing.T) {
	// snapA has rule-A (approve) and rule-B (approve) both at priority 100.
	snapA := compileRulesWithID(t, "snap-A", []struct{ filename, source string }{
		{"rule-a.star", `
rule_id = "rule-A"
event_type = "post"
priority = 100
def evaluate(event):
    return verdict("approve", reason="from-A")
`},
		{"rule-b.star", `
rule_id = "rule-B"
event_type = "post"
priority = 100
def evaluate(event):
    return verdict("approve", reason="from-B")
`},
	})

	// snapB has rule-A (approve) and rule-D (block) both at priority 100.
	// With rule-D present, block (weight=3) should win over approve (weight=1).
	snapB := compileRulesWithID(t, "snap-B", []struct{ filename, source string }{
		{"rule-a.star", `
rule_id = "rule-A"
event_type = "post"
priority = 100
def evaluate(event):
    return verdict("approve", reason="from-A")
`},
		{"rule-d.star", `
rule_id = "rule-D"
event_type = "post"
priority = 100
def evaluate(event):
    return verdict("block", reason="from-D")
`},
	})

	var ptr atomic.Pointer[rules.Snapshot]
	ptr.Store(snapA)
	pool := NewPool(1, &ptr, 5*time.Second, 1*time.Second)

	// Use an unbuffered input channel and synchronize the snapshot swap between events
	// so we can guarantee event 1 is processed with snapA and event 2 with snapB.
	in := make(chan types.Event)
	out := make(chan types.Result, 2)

	// Run pool in background.
	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.Run(context.Background(), in, out)
	}()

	// Event 1: send and wait for result before swapping snapshot.
	in <- testEvent("post", nil)
	r1 := <-out

	// Now swap to snapB.
	ptr.Store(snapB)

	// Event 2: processed with snapB (worker will detect new snap.ID, clear evalCache).
	in <- testEvent("post", nil)
	r2 := <-out

	close(in)
	<-done

	// Event 1 with snapA: both rules A and B trigger -> approve (both approve).
	if r1.FinalVerdict != types.VerdictApprove {
		t.Errorf("event 1: FinalVerdict = %q, want approve", r1.FinalVerdict)
	}
	var sawB bool
	for _, rr := range r1.TriggeredRules {
		if rr.RuleID == "rule-B" {
			sawB = true
		}
	}
	if !sawB {
		t.Errorf("event 1: expected rule-B to trigger; triggered rules: %v", r1.TriggeredRules)
	}

	// Event 2 with snapB: rules A and D -> D returns block -> block wins.
	if r2.FinalVerdict != types.VerdictBlock {
		t.Errorf("event 2: FinalVerdict = %q, want block (rule-D should run after snapshot swap)", r2.FinalVerdict)
	}
	var sawD, sawBAgain bool
	for _, rr := range r2.TriggeredRules {
		if rr.RuleID == "rule-D" {
			sawD = true
		}
		if rr.RuleID == "rule-B" {
			sawBAgain = true
		}
	}
	if !sawD {
		t.Errorf("event 2: expected rule-D to trigger; triggered rules: %v", r2.TriggeredRules)
	}
	if sawBAgain {
		t.Error("event 2: rule-B should not run (not in snapB)")
	}
}

// --- T32: regexCache unbounded growth documentation test ---

// T32: Directly populate regexCache with unique patterns and verify unbounded accumulation.
// This is a documentation test — it demonstrates that regexCache has no eviction policy.
// We use a single-event run to initialize pool.workers, then directly insert unique
// compiled patterns into w.regexCache to show it grows without bound.
func TestRegexCache_UnboundedGrowth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	const numUniquePatterns = 500

	snap := compileRule(t, `
rule_id = "regex-growth"
event_type = "post"
priority = 100
def evaluate(event):
    matched = regex_match("^test.*", "test input")
    return verdict("approve", reason="ok")
`)

	var ptr atomic.Pointer[rules.Snapshot]
	ptr.Store(snap)
	pool := NewPool(1, &ptr, 5*time.Second, 1*time.Second)

	// Run 1 event to initialize pool.workers.
	in := make(chan types.Event, 1)
	out := make(chan types.Result, 1)
	in <- testEvent("post", nil)
	close(in)
	pool.Run(context.Background(), in, out)
	for range out {
	}

	// After 1 event with 1 unique pattern, regexCache should have exactly 1 entry.
	w := pool.workers[0]
	if got := len(w.regexCache); got != 1 {
		t.Errorf("regexCache size after 1-pattern event = %d, want 1", got)
	}

	// Directly insert N unique compiled patterns to demonstrate unbounded growth.
	// This simulates what would happen if N distinct patterns were used across events.
	for i := 0; i < numUniquePatterns; i++ {
		pattern := fmt.Sprintf("^unique-pattern-%d-.*$", i)
		re, err := regexp.Compile(pattern)
		if err != nil {
			t.Fatalf("compile pattern %d: %v", i, err)
		}
		w.regexCache[pattern] = re
	}

	// Cache now holds the 1 original pattern + numUniquePatterns injected entries.
	wantSize := 1 + numUniquePatterns
	if got := len(w.regexCache); got != wantSize {
		t.Errorf("regexCache size after injecting %d unique patterns = %d, want %d",
			numUniquePatterns, got, wantSize)
	}

	t.Logf("regexCache size with %d unique patterns: %d (grows unboundedly — no eviction)",
		numUniquePatterns, len(w.regexCache))
	t.Log("NOTE: regexCache has no eviction policy. With N unique patterns, it grows to size N unboundedly.")
}
