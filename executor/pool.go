package executor

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vinaysrao1/fruitfly/rules"
	"github.com/vinaysrao1/fruitfly/types"
	"go.starlark.net/starlark"
)

type worker struct {
	id         int
	pool       *Pool
	memo       map[string]starlark.Value
	regexCache map[string]*regexp.Regexp
	// thread is reused for every evaluation on this worker: rules ship with
	// pre-initialized shared callables, and stateful UDFs resolve this
	// worker through the thread's EnvLocal, so no per-eval thread or
	// predeclared dict is needed.
	thread    *starlark.Thread
	args      starlark.Tuple // reusable 1-slot args for evaluate(event)
	curEntity string         // routing key of the event being processed
	curRule   string         // rule being evaluated (for print logging)
}

// Pool manages worker goroutines that evaluate rules against events.
type Pool struct {
	workerCount  int
	snapshot     *atomic.Pointer[rules.Snapshot]
	eventTimeout time.Duration
	ruleTimeout  time.Duration // retained for API compatibility; per-rule bounding is step-based
	counters     counterStore
	// counterAffinity, when set (cluster mode), restricts counter() keys to
	// the event's routing entity: any other key would scatter increments
	// across pods' private stores and silently undercount.
	counterAffinity bool
	workers         []*worker
}

// SetCounterAffinity enables the cluster-mode counter key restriction.
// Must be called before Run.
func (p *Pool) SetCounterAffinity(on bool) { p.counterAffinity = on }

// NewPool creates a worker pool. Does not start workers.
func NewPool(
	workerCount int,
	snapshot *atomic.Pointer[rules.Snapshot],
	eventTimeout, ruleTimeout time.Duration,
) *Pool {
	return &Pool{
		workerCount:  workerCount,
		snapshot:     snapshot,
		eventTimeout: eventTimeout,
		ruleTimeout:  ruleTimeout,
	}
}

// newWorker builds a worker with its reusable evaluation thread.
func (p *Pool) newWorker(id int) *worker {
	w := &worker{
		id:         id,
		pool:       p,
		memo:       make(map[string]starlark.Value),
		regexCache: make(map[string]*regexp.Regexp),
		args:       make(starlark.Tuple, 1),
	}
	w.thread = &starlark.Thread{
		Print: func(_ *starlark.Thread, msg string) {
			slog.Info("rule log", "rule_id", w.curRule, "message", msg)
		},
	}
	w.thread.SetLocal(rules.EnvLocal, w)
	return w
}

// Run starts N worker goroutines. Blocks until in is closed and all events processed.
// Closes out before returning.
func (p *Pool) Run(ctx context.Context, in <-chan types.Event, out chan<- types.Result) {
	p.workers = make([]*worker, p.workerCount)
	for i := range p.workers {
		p.workers[i] = p.newWorker(i)
	}

	var wg sync.WaitGroup
	wg.Add(p.workerCount)
	for _, w := range p.workers {
		w := w
		go func() {
			defer wg.Done()
			w.run(ctx, in, out)
		}()
	}

	// Evict aged-out counter series once a minute while workers run.
	gcDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-gcDone:
				return
			case <-ticker.C:
				p.counters.gc(time.Now().Unix())
			}
		}
	}()

	wg.Wait()
	close(gcDone)
	close(out)
}

// CounterSum returns the total count for (entityID, eventType) within the
// trailing window. Counters live in a key-sharded store, so this is a
// single-shard read regardless of worker count. windowSeconds must be
// between 1 and counterMaxWindowSeconds.
func (p *Pool) CounterSum(entityID, eventType string, windowSeconds int) int64 {
	windowStart := time.Now().Unix() - int64(windowSeconds)
	return p.counters.sum(entityID, eventType, windowStart)
}

func (w *worker) run(ctx context.Context, in <-chan types.Event, out chan<- types.Result) {
	for event := range in {
		out <- w.processEvent(ctx, event)
	}
}

func (w *worker) processEvent(ctx context.Context, event types.Event) types.Result {
	defer clear(w.memo)

	start := time.Now()

	// Event-level wall-clock bound: one timeout and one cancellation hook
	// per event (per-rule bounding is step-based inside evalRule).
	eventCtx, cancel := context.WithTimeout(ctx, w.pool.eventTimeout)
	defer cancel()
	stop := context.AfterFunc(eventCtx, func() {
		w.thread.Cancel(eventCtx.Err().Error())
	})
	defer stop()

	snap := w.pool.snapshot.Load()
	if snap == nil {
		slog.Warn("nil snapshot, defaulting to approve", "event_id", event.EventID)
		return types.Result{
			EventID:      event.EventID,
			EventType:    event.EventType,
			FinalVerdict: types.VerdictApprove,
			ProcessedAt:  time.Now(),
			LatencyUS:    time.Since(start).Microseconds(),
			Payload:      event.Payload,
			RawPayload:   event.RawJSON,
		}
	}

	matchedRules := snap.RulesForEvent(event.EventType)

	// Convert the event to Starlark once; passed into each rule to avoid repeated conversion.
	starlarkEvent := eventToStarlark(event)
	w.curEntity = event.EntityID

	var triggered []types.RuleResult
	var failed []types.RuleResult

	for i := range matchedRules {
		rule := &matchedRules[i]
		// Tier 1: native prefilter — no Starlark unless every clause passes.
		if !rule.PrefilterMatch(&event) {
			rule.Prefiltered.Add(1)
			continue
		}
		// The event's wall-clock budget is exhausted: remaining rules fail
		// without running rather than evaluating under a dead deadline.
		if ctxErr := eventCtx.Err(); ctxErr != nil {
			rr := types.RuleResult{RuleID: rule.RuleID, Priority: rule.Priority}
			rr.Err = fmt.Errorf("rule %s: %w", rule.RuleID, ctxErr)
			rr.ErrMsg = rr.Err.Error()
			failed = append(failed, rr)
			continue
		}
		rr := w.evalRule(eventCtx, rule, starlarkEvent)
		if rr.Err != nil {
			failed = append(failed, rr)
		} else if rr.Verdict != "" {
			triggered = append(triggered, rr)
		}
	}

	return types.Result{
		EventID:        event.EventID,
		EventType:      event.EventType,
		FinalVerdict:   resolveVerdict(triggered),
		TriggeredRules: triggered,
		FailedRules:    failed,
		Payload:        event.Payload,
		RawPayload:     event.RawJSON,
		LatencyUS:      time.Since(start).Microseconds(),
		ProcessedAt:    time.Now(),
	}
}

func (w *worker) evalRule(ctx context.Context, rule *rules.Rule, starlarkEvent starlark.Value) (rr types.RuleResult) {
	rr.RuleID = rule.RuleID
	rr.Priority = rule.Priority
	start := time.Now()

	defer func() {
		rr.Elapsed = time.Since(start)
		if r := recover(); r != nil {
			rr.Err = fmt.Errorf("rule %s panicked: %v", rule.RuleID, r)
		}
		if rr.Err != nil {
			rr.ErrMsg = rr.Err.Error()
		}
	}()

	// Reuse the worker's thread: clear any prior cancellation, grant this
	// rule a fresh step budget on top of the accumulated count, and point
	// the print hook at this rule.
	w.curRule = rule.RuleID
	w.thread.Name = rule.RuleID
	w.thread.Uncancel()
	w.thread.SetMaxExecutionSteps(w.thread.ExecutionSteps() + ruleStepBudget)

	w.args[0] = starlarkEvent
	retVal, err := starlark.Call(w.thread, rule.Evaluate, w.args, nil)
	if err != nil {
		switch {
		case ctx.Err() != nil:
			// Keep the real cause (cancellation vs deadline) and the
			// underlying eval error rather than relabelling both.
			rr.Err = fmt.Errorf("rule %s: %w: %v", rule.RuleID, ctx.Err(), err)
		case strings.Contains(err.Error(), "too many steps"):
			// Step budget exceeded — the per-rule analogue of a deadline.
			rr.Err = fmt.Errorf("rule %s: step budget exceeded: %w: %v", rule.RuleID, context.DeadlineExceeded, err)
		default:
			rr.Err = fmt.Errorf("rule %s: %w", rule.RuleID, err)
		}
		return
	}

	verdict, reason, err := interpretVerdict(retVal)
	if err != nil {
		rr.Err = fmt.Errorf("rule %s: %w", rule.RuleID, err)
		return
	}

	rr.Verdict = verdict
	rr.Reason = reason
	return
}
