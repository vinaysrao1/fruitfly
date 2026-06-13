package executor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vinaysrao1/fruitfly/rules"
)

// TestEvalRule_DeadCtxAfterUncancelFails: the event-deadline AfterFunc's
// Cancel can land in the window between processEvent's per-rule ctx.Err()
// check and evalRule's Uncancel. Uncancel then ERASES the cancellation and
// the rule runs its full step budget under a dead context instead of failing
// promptly. evalRule must re-check ctx.Err() after Uncancel and fail the rule
// exactly like the loop's deadline-skip path.
//
// Verified bidirectionally: with the re-check removed, the rule below runs
// ~5M steps to completion and returns its verdict under a dead context.
func TestEvalRule_DeadCtxAfterUncancelFails(t *testing.T) {
	c := newTestCompiler()
	rule, err := c.CompileSource("erase.star", `
rule_id = "erase-rule"
event_type = "post"
priority = 10
def evaluate(event):
    x = 0
    for i in range(500000):
        x += 1
    return verdict("approve", reason="ran to completion under a dead deadline")
`)
	if err != nil {
		t.Fatalf("CompileSource: %v", err)
	}

	var ptr atomic.Pointer[rules.Snapshot]
	pool := NewPool(1, &ptr, 5*time.Second, time.Second)
	w := pool.newWorker(0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the event deadline has already expired
	// Simulate the AfterFunc's Cancel landing in the race window: after the
	// loop's ctx.Err() pre-check, before evalRule's Uncancel.
	w.thread.Cancel("context canceled")

	rr := w.evalRule(ctx, rule, eventToStarlark(testEvent("post", nil)))
	if rr.Err == nil {
		t.Fatalf("rule ran to completion under a dead context (Uncancel erased the deadline Cancel): verdict=%q reason=%q",
			rr.Verdict, rr.Reason)
	}
	if !errors.Is(rr.Err, context.Canceled) {
		t.Errorf("err = %v, want a context.Canceled cause (same shape as the loop's deadline-skip path)", rr.Err)
	}
	if rr.ErrMsg == "" {
		t.Error("ErrMsg empty, want it mirrored from Err for result serialization")
	}
}
