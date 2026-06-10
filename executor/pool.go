package executor

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vinaysrao1/fruitfly/rules"
	"github.com/vinaysrao1/fruitfly/types"
	"go.starlark.net/starlark"
)

const counterBucketSeconds int64 = 60
const counterMaxWindowSeconds int64 = 3600 // must be >= max window used by any rule
const counterSlots = counterMaxWindowSeconds / counterBucketSeconds

type counterKey struct {
	EntityID  string
	EventType string
}

// counterSlot is one time bucket in a series ring. bucket holds the bucket
// start (unix seconds, 0 = empty); both fields are atomic because workers
// read each other's series via CounterSum.
type counterSlot struct {
	bucket atomic.Int64
	count  atomic.Int64
}

// counterSeries is a fixed ring of buckets covering counterMaxWindowSeconds.
// Only the owning worker writes; any worker may read.
type counterSeries struct {
	slots [counterSlots]counterSlot
}

type worker struct {
	id         int
	pool       *Pool
	memo       map[string]any
	regexCache map[string]*regexp.Regexp
	// counters uses sync.Map because CounterSum reads across all workers concurrently.
	// Each worker only writes to its own map, but cross-worker reads require thread safety.
	// This is a pragmatic deviation from the no-locks design constraint.
	counters  sync.Map // counterKey -> *counterSeries
	evtCount  int
	evalCache map[string]starlark.Callable // ruleID -> cached evaluate fn
	evalSnap  string                       // snapshot ID that populated the cache
	udfs      starlark.StringDict          // worker-lifetime UDFs (built once)
}

// Pool manages worker goroutines that evaluate rules against events.
type Pool struct {
	workerCount  int
	snapshot     *atomic.Pointer[rules.Snapshot]
	eventTimeout time.Duration
	ruleTimeout  time.Duration
	workers      []*worker
}

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

// Run starts N worker goroutines. Blocks until in is closed and all events processed.
// Closes out before returning.
func (p *Pool) Run(ctx context.Context, in <-chan types.Event, out chan<- types.Result) {
	p.workers = make([]*worker, p.workerCount)
	for i := range p.workers {
		w := &worker{
			id:         i,
			pool:       p,
			memo:       make(map[string]any),
			regexCache: make(map[string]*regexp.Regexp),
			evalCache:  make(map[string]starlark.Callable),
		}
		w.udfs = buildUDFs(w)
		p.workers[i] = w
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

	wg.Wait()
	close(out)
}

// CounterSum returns the sum of counter values across all workers.
func (p *Pool) CounterSum(entityID, eventType string, windowSeconds int) int64 {
	now := time.Now().Unix()
	windowStart := now - int64(windowSeconds)

	var total int64
	for _, w := range p.workers {
		total += w.counterQuery(entityID, eventType, windowStart)
	}
	return total
}

func (w *worker) counterQuery(entityID, eventType string, windowStart int64) int64 {
	v, ok := w.counters.Load(counterKey{EntityID: entityID, EventType: eventType})
	if !ok {
		return 0
	}
	s := v.(*counterSeries)
	var total int64
	for i := range s.slots {
		if s.slots[i].bucket.Load() >= windowStart {
			total += s.slots[i].count.Load()
		}
	}
	return total
}

func (w *worker) run(ctx context.Context, in <-chan types.Event, out chan<- types.Result) {
	for event := range in {
		out <- w.processEvent(ctx, event)
	}
}

func (w *worker) processEvent(ctx context.Context, event types.Event) types.Result {
	defer clear(w.memo)

	start := time.Now()

	// Apply event-level timeout so total rule evaluation time is bounded.
	eventCtx, cancel := context.WithTimeout(ctx, w.pool.eventTimeout)
	defer cancel()

	w.evtCount++
	if w.evtCount >= 1000 {
		w.evtCount = 0
		w.gcCounters()
	}

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

	// Invalidate eval cache when snapshot changes.
	if snap.ID != w.evalSnap {
		clear(w.evalCache)
		w.evalSnap = snap.ID
	}

	matchedRules := snap.RulesForEvent(event.EventType)

	// Convert the event to Starlark once; passed into each rule to avoid repeated conversion.
	starlarkEvent := eventToStarlark(event)

	var triggered []types.RuleResult
	var failed []types.RuleResult

	for _, rule := range matchedRules {
		rr := w.evalRule(eventCtx, rule, starlarkEvent, w.udfs)
		if rr.Err != nil {
			failed = append(failed, rr)
		} else if rr.Verdict != "" {
			triggered = append(triggered, rr)
		}
	}

	return types.Result{
		EventID:        event.EventID,
		EventType:      event.EventType,
		FinalVerdict:   resolveVerdict(triggered, matchedRules),
		TriggeredRules: triggered,
		FailedRules:    failed,
		Payload:        event.Payload,
		RawPayload:     event.RawJSON,
		LatencyUS:      time.Since(start).Microseconds(),
		ProcessedAt:    time.Now(),
	}
}

func (w *worker) evalRule(ctx context.Context, rule rules.Rule, starlarkEvent starlark.Value, predeclared starlark.StringDict) (rr types.RuleResult) {
	rr.RuleID = rule.RuleID
	start := time.Now()

	defer func() {
		rr.Elapsed = time.Since(start)
		if r := recover(); r != nil {
			rr.Err = fmt.Errorf("rule %s panicked: %v", rule.RuleID, r)
		}
	}()

	ruleCtx, cancel := context.WithTimeout(ctx, w.pool.ruleTimeout)
	defer cancel()

	thread := &starlark.Thread{
		Name: fmt.Sprintf("worker-%d/rule-%s", w.id, rule.RuleID),
		Print: func(_ *starlark.Thread, msg string) {
			slog.Info("rule log", "rule_id", rule.RuleID, "message", msg)
		},
	}

	// Cancel Starlark thread when context expires. context.AfterFunc avoids
	// spawning a watcher goroutine per rule evaluation.
	stop := context.AfterFunc(ruleCtx, func() {
		thread.Cancel(ruleCtx.Err().Error())
	})
	defer stop()

	evalFn, cached := w.evalCache[rule.RuleID]
	if !cached {
		globals, err := rule.Program.Init(thread, predeclared)
		if err != nil {
			if ruleCtx.Err() != nil {
				rr.Err = fmt.Errorf("rule %s: %w", rule.RuleID, context.DeadlineExceeded)
			} else {
				rr.Err = fmt.Errorf("rule %s init: %w", rule.RuleID, err)
			}
			return
		}

		fn, ok := globals["evaluate"]
		if !ok {
			rr.Err = fmt.Errorf("rule %s: missing evaluate after init", rule.RuleID)
			return
		}
		callable, ok := fn.(starlark.Callable)
		if !ok {
			rr.Err = fmt.Errorf("rule %s: evaluate is not callable", rule.RuleID)
			return
		}
		w.evalCache[rule.RuleID] = callable
		evalFn = callable
	}

	retVal, err := starlark.Call(thread, evalFn, starlark.Tuple{starlarkEvent}, nil)
	if err != nil {
		if ruleCtx.Err() != nil {
			rr.Err = fmt.Errorf("rule %s: %w", rule.RuleID, context.DeadlineExceeded)
		} else {
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

// gcCounters evicts series whose buckets have all aged out, bounding memory
// when entity IDs have high cardinality.
func (w *worker) gcCounters() {
	cutoff := time.Now().Unix() - counterMaxWindowSeconds
	w.counters.Range(func(k, v any) bool {
		s := v.(*counterSeries)
		for i := range s.slots {
			if s.slots[i].bucket.Load() >= cutoff {
				return true
			}
		}
		w.counters.Delete(k)
		return true
	})
}

func (w *worker) counterIncrement(entityID, eventType string, unixNow int64) {
	bucket := (unixNow / counterBucketSeconds) * counterBucketSeconds
	key := counterKey{EntityID: entityID, EventType: eventType}
	v, ok := w.counters.Load(key)
	if !ok {
		v, _ = w.counters.LoadOrStore(key, &counterSeries{})
	}
	s := v.(*counterSeries)
	slot := &s.slots[(bucket/counterBucketSeconds)%counterSlots]
	if slot.bucket.Load() != bucket {
		// Reset count before publishing the new bucket so concurrent readers
		// never attribute a stale count to the new bucket.
		slot.count.Store(0)
		slot.bucket.Store(bucket)
	}
	slot.count.Add(1)
}
