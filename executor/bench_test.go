package executor

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vinaysrao1/fruitfly/rules"
	"github.com/vinaysrao1/fruitfly/types"
	"go.starlark.net/starlark"
)

// setupCounterPool builds a pool and populates counters for `entities`
// distinct entity IDs, three buckets each.
func setupCounterPool(workers, entities int) *Pool {
	var ptr atomic.Pointer[rules.Snapshot]
	pool := NewPool(workers, &ptr, 5*time.Second, time.Second)
	now := time.Now().Unix()
	for e := 0; e < entities; e++ {
		id := fmt.Sprintf("entity-%d", e)
		pool.counters.increment(id, "post", now)
		pool.counters.increment(id, "post", now-120)
		pool.counters.increment(id, "post", now-600)
	}
	return pool
}

// benchCounter measures the cost of one counter() UDF call:
// one increment plus one CounterSum read.
func benchCounter(b *testing.B, entities int) {
	pool := setupCounterPool(4, entities)
	now := time.Now().Unix()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.counters.increment("entity-42", "post", now)
		pool.CounterSum("entity-42", "post", 3600)
	}
}

func BenchmarkCounter_100Entities(b *testing.B)  { benchCounter(b, 100) }
func BenchmarkCounter_1kEntities(b *testing.B)   { benchCounter(b, 1000) }
func BenchmarkCounter_10kEntities(b *testing.B)  { benchCounter(b, 10000) }
func BenchmarkCounter_100kEntities(b *testing.B) { benchCounter(b, 100000) }

// benchSnapshot compiles n rules spread across 10 event types plus one
// wildcard rule, mirroring CompileDir's snapshot construction.
func benchSnapshot(b *testing.B, n int) *rules.Snapshot {
	b.Helper()
	dir := b.TempDir()
	for i := 0; i < n; i++ {
		src := fmt.Sprintf(`
rule_id = "rule-%d"
event_type = "type-%d"
priority = %d
def evaluate(event):
    return verdict("approve")
`, i, i%10, i)
		writeBenchFile(b, fmt.Sprintf("%s/rule_%03d.star", dir, i), src)
	}
	writeBenchFile(b, dir+"/wildcard.star", `
rule_id = "wildcard"
event_type = "*"
priority = 1
def evaluate(event):
    return verdict("approve")
`)
	c := &rules.Compiler{UDFs: rules.DefaultUDFs()}
	snap, err := c.CompileDir(dir)
	if err != nil {
		b.Fatal(err)
	}
	return snap
}

// BenchmarkProcessEvent measures the full per-event hot path with 100 rules
// loaded (10 matching the event type + 1 wildcard execute per event):
// rule matching, Starlark conversion, and rule evaluation with timeout setup.
func BenchmarkProcessEvent_100Rules(b *testing.B) {
	snap := benchSnapshot(b, 100)
	var ptr atomic.Pointer[rules.Snapshot]
	ptr.Store(snap)
	pool := NewPool(1, &ptr, 5*time.Second, time.Second)
	pool.workers = make([]*worker, 1)
	w := &worker{
		id:         0,
		pool:       pool,
		memo:       make(map[string]any),
		regexCache: nil,
		evalCache:  make(map[string]starlark.Callable),
	}
	w.udfs = buildUDFs(w)
	pool.workers[0] = w

	event := types.Event{
		EventID:   "bench-event",
		EventType: "type-5",
		Timestamp: time.Now(),
		Payload: map[string]any{
			"entity_id": "user-1",
			"text":      "some moderately sized text payload for the benchmark",
			"count":     float64(42),
			"nested":    map[string]any{"a": float64(1), "b": "two"},
		},
		ReceivedAt: time.Now(),
	}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.processEvent(ctx, event)
	}
}

// BenchmarkEvalRuleOnly isolates a single cached-rule evaluation, which is
// dominated by per-eval timeout/cancellation setup plus the Starlark call.
func BenchmarkEvalRuleOnly(b *testing.B) {
	snap := benchSnapshot(b, 1)
	var ptr atomic.Pointer[rules.Snapshot]
	ptr.Store(snap)
	pool := NewPool(1, &ptr, 5*time.Second, time.Second)
	w := &worker{
		id:        0,
		pool:      pool,
		memo:      make(map[string]any),
		evalCache: make(map[string]starlark.Callable),
	}
	w.udfs = buildUDFs(w)

	rule := snap.Rules[0]
	evt := eventToStarlark(types.Event{EventID: "e", EventType: "type-0", Timestamp: time.Now()})
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr := w.evalRule(ctx, rule, evt, w.udfs)
		if rr.Err != nil {
			b.Fatal(rr.Err)
		}
	}
}

func writeBenchFile(b *testing.B, path, content string) {
	b.Helper()
	if err := writeFileHelper(path, content); err != nil {
		b.Fatal(err)
	}
}

func writeFileHelper(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}
