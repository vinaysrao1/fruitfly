# Scaling Fruitfly to 10M events/sec

**Status**: Design proposal
**Baseline**: measured on commit `36d6e04` (see `executor/bench_test.go`, `output/bench_test.go`)

This document describes how fruitfly evolves from a single-binary rules engine
(~30k events/sec/core measured) to a system that sustains 10M events/sec with
10,000 rules — without giving up the property that makes it worth using: a
single, dependency-free binary that is trivial to run on one CPU.

---

## 1. Design principles

1. **One binary, every scale.** `./fruitfly` on a laptop and a 64-core
   Kubernetes pod run the same code path. There is no "distributed mode"
   codebase; a cluster is N identical processes plus a routing rule.
2. **Entity affinity is the only coordination primitive.** All shared state
   (counters) is keyed by entity ID. If `hash(entity_id)` deterministically
   picks the worker within a process and the pod within a cluster, counters
   never need locks, atomics across owners, or external storage. Scaling is
   sharding, nothing else.
3. **Pay per byte touched, not per byte received.** Events are kept as raw
   bytes; parsing happens lazily, only for fields a rule actually reads.
4. **Rules declare their cheap part.** Every rule can be split into a native
   prefilter (tier 1) and a Starlark body (tier 2). The engine's job is to
   make tier 2 rare.
5. **The hot path allocates (almost) nothing.** Today: ~21KB and ~330
   allocations per event. Target: <500B and single-digit allocations,
   steady-state, via pooling and lazy views.

### Performance budget

| Stage | Today (measured) | Target | How |
|---|---|---|---|
| Decode | ~2µs + 18KB (full `map[string]any`) | ~200ns | lazy raw-bytes view |
| Rule match | ~50ns (indexed) | ~50ns | unchanged |
| Tier-1 prefilter | — | ~10ns/rule | native predicates |
| Tier-2 eval | ~2.5µs/rule × all matched | ~1.5µs/rule × few | thread reuse, no per-eval timers |
| Counters | ~450ns/call, 0 alloc | ~100ns | single-owner shards (no sync.Map) |
| Result + output | ~µs + slices per event | ~100ns amortized | pooled results, interesting-only emission |

Net: **~37µs/event/core today → 3–6µs/event/core**, i.e. 150–300k events/sec
per core. 10M events/sec lands at **40–70 cores** (one or two large pods, or
a handful of small ones) when rules are well-partitioned, degrading
gracefully (more cores) when many rules pass tier 1.

---

## 2. Input

### Today
One HTTP POST per event; `io.ReadAll` + `json.Unmarshal` into
`map[string]any`; a single shared `chan types.Event` (cap 100); 429 on
overflow. The map materialization is the largest single allocation in the
system, and one-request-per-event caps any node at the HTTP stack long before
the rules engine matters.

### Design

**Transport: batches first, streams second, single events forever.**

- `POST /events` — unchanged (compatibility, debugging, low-rate users).
- `POST /events/batch` — NDJSON or a JSON array; one request carries
  thousands of events. This alone removes HTTP framing as the bottleneck and
  is the only transport change most users ever need.
- `GET /events/stream` (upgrade) or gRPC client-stream — for sustained
  producers; persistent connection, length-prefixed frames, periodic acks.

All three feed the same internal path; the single-event endpoint is just a
batch of one.

**Decode: the `RawEvent`.** The ingest layer stops producing
`map[string]any`. It produces:

```go
type RawEvent struct {
    Bytes      []byte    // the original JSON, owned by a pooled arena
    EventType  string    // scanned, not parsed
    EntityID   string    // scanned, not parsed (declared field path, default "entity_id")
    Timestamp  int64     // scanned, not parsed
    ReceivedAt int64
}
```

A streaming scanner (hand-rolled or `simdjson`-style) extracts only the three
envelope fields; everything else stays bytes. The full parse happens lazily
inside the executor, and only if a tier-2 rule runs (§4).

**Dispatch: entity-sharded SPSC rings.** The shared channel is replaced by
one single-producer/single-consumer ring buffer per worker. The ingest
goroutine routes each event with `shard = hash(EntityID) % workers`. Two
consequences:

- No channel contention at high rates.
- Every event for a given entity is processed by the same worker — the
  foundation for lock-free counters (§4) and the same rule that routes
  between pods (§6).

**Backpressure: explicit shed policy, not per-event 429s.** Rings report
fill level; when a ring is over the high-water mark the ingest layer applies
a configured policy: `reject` (429 the whole batch), `drop-oldest`, or
`degrade` (skip tier-2, verdict from tier-1 only, marked in the result).
Single-CPU default stays `reject` — today's behavior.

---

## 3. Rules: parsing & compiling

### Today
`CompileDir` reads `*.star` files serially, extracts `rule_id`/`event_type`/
`priority` globals, builds the per-event-type index. 10k rules compile fine
but serially, and every rule is opaque: the only native-speed filter is
`event_type`.

### Design

**Rule files gain an optional declarative prefilter.** Backwards compatible —
a rule without `match` behaves exactly as today:

```python
rule_id    = "spam-short-post"
event_type = "post"
priority   = 100

# Tier 1: compiled to native Go predicates, ~10ns each. No Starlark executes
# unless every clause passes.
match = {
    "all": [
        ["payload.char_count", "<", 20],
        ["payload.lang", "in", ["en", "es"]],
    ],
}

def evaluate(event):   # Tier 2: runs only for events that survive `match`
    if counter(event["entity_id"], "post", 60) > 5:
        return verdict("block", reason="short-post flood")
    return verdict("approve")
```

The compiler lowers `match` into a small predicate struct (field path,
comparison op, constant) evaluated against the lazy event view with no
Starlark and no allocation. Supported ops stay deliberately tiny: `==`, `!=`,
`<`, `<=`, `>`, `>=`, `in`, `exists`, `prefix`. Anything fancier belongs in
`evaluate()`.

This is the single most important lever for 10k rules: the per-event cost
becomes `(matched rules × 10ns) + (surviving rules × ~1.5µs)`, and rule
authors control the surviving set.

**Compilation parallelizes; snapshots pre-warm.**

- `CompileDir` compiles files across `GOMAXPROCS` goroutines (compilation is
  pure). 10k rules compile in roughly the time 10k/N took before.
- The snapshot pre-runs `Program.Init` once per rule at compile time and
  stores the extracted `evaluate` callables, so a snapshot swap costs workers
  nothing — today each worker re-inits each rule on first use after a swap,
  which at 10k rules × 64 workers is a visible warmup spike.
- The index extends to two levels: `event_type → []ruleRef` exactly as now,
  with each `ruleRef` carrying its tier-1 predicate inline so matching and
  prefiltering are one cache-friendly scan over a flat slice.

**Validation gets stricter as a side effect of scale.** With 10k rules,
"every rule is wildcard" is an outage, not a style choice. `CompileDir` warns
(configurably errors) when wildcard rules exceed a threshold or when a single
event type accumulates more than N tier-2-only rules.

---

## 4. Execution

### Today
Worker pool over a shared channel; per-event `context.WithTimeout`, per-rule
`context.WithTimeout` + `context.AfterFunc`; full event→Starlark conversion
per event; counters in per-worker `sync.Map` with cross-worker reads
(~450ns, already flat); verdict resolution over triggered rules.

### Design

**Workers own entity shards.** Because ingest routes by `hash(EntityID)`
(§2), each worker is the *only* goroutine that ever touches its counters.
The `sync.Map` + atomics design — which exists solely because any worker
could read any other's counters — collapses into a plain
`map[counterKey]*counterSeries` with zero synchronization. `counter()`
becomes a map lookup plus 60 int reads, ~100ns, and the code gets *simpler*
than what we have today. The ring-buffer bucket design from `36d6e04` is
kept unchanged; only the container changes.

(`CounterSum` as a cross-worker API disappears; a `counter()` call can only
be answered by the owning worker, and the owning worker is, by construction,
the one asking.)

**The lazy Starlark view replaces eventToStarlark.** A `*LazyEvent`
implements `starlark.Mapping` over `RawEvent.Bytes`. `event["payload"]`
returns a lazy sub-view; leaf access parses just that value and memoizes it
in a small per-event scratch (pooled). Rules that touch three fields pay for
three fields. Tier-1 predicates use the same field scanner without entering
Starlark at all.

**Timeouts: steps first, wall clock second, timers never.**

- Primary bound: `thread.SetMaxExecutionSteps(n)` — deterministic,
  allocation-free, and catches runaway rules regardless of clock.
- Backstop: one watchdog goroutine per process scans a fixed array of
  per-worker deadline slots (one atomic store per *event*, not per rule)
  every ~10ms and cancels overrunning threads.
- Deleted: both `context.WithTimeout` calls and the `AfterFunc` per rule
  eval — at 10M events/sec those are tens of millions of timer allocations
  per second for a path that fires approximately never.

**The hot loop allocates nothing.** Per worker, reused across events: one
Starlark thread, one lazy-view scratch, one `Result` (pooled), one
`triggered []RuleResult` backing array. `RuleResult.Elapsed` becomes
sampled (1-in-N events carry timing) instead of two `time.Now()` calls per
rule per event. Verdict resolution already operates on priority-sorted
rules; it short-circuits on the first triggered rule of the highest
priority class with `block` (nothing can outrank it).

**Single-CPU story.** `workers = GOMAXPROCS` (today's default). With one
CPU there is one worker, one ring, one counter map, no watchdog
contention — the degenerate case is the simple case, with zero
configuration.

---

## 5. Output

### Today
Every result is batch-inserted into DuckDB and individually POSTed to a
webhook (bounded at 100 in flight, drops beyond). Measured: DuckDB insert
dominates (~600µs/row amortized); webhook-per-result caps out around
thousands/sec.

### Design

**Output becomes a `Sink` interface with an emission policy in front.**

```go
type Sink interface {
    Emit(batch []*Result) error   // called with pooled batches; Sink must copy what it keeps
    Close() error
}
```

**Emission policy is the scaling decision.** At 10M events/sec, "store
everything" is a data-platform problem, not a rules-engine problem. The
engine's contract becomes:

- `emit: interesting` (default at scale): full `Result` for block/review
  verdicts and rule errors — typically a small fraction of traffic — plus a
  per-(event_type, verdict) counter flushed every second. Approvals cost one
  counter increment, ~zero bytes.
- `emit: all` (default for single-CPU/dev): today's behavior, every result.
- `emit: sample(p)`: `interesting` plus a p% sample of approvals for offline
  rule QA.

**Built-in sinks, all optional, none required:**

| Sink | Role |
|---|---|
| `webhook` | batched NDJSON POST (one request per batch, not per result), HMAC-signed |
| `stdout` | NDJSON to stdout — composes with anything, the OSS-friendly default |
| `duckdb` | kept as the local-analytics nice-to-have, behind a build tag; never on the 10M path |
| `none` | counters in `/admin/metrics` only |

The writer goroutine, retention loop, and webhook semaphore all live behind
the sink boundary; the engine core no longer knows DuckDB exists.

---

## 6. Horizontal scaling (Kubernetes)

The cluster is the same binary repeated, with entity affinity preserved one
level up: **`hash(entity_id)` chooses the pod exactly the way it chooses the
worker inside a pod.** No coordinator, no shared database, no gossip. Two
deployment shapes, both using a plain `Deployment` + headless `Service`:

**Shape A — affinity at the edge (preferred).** The load balancer in front
(Envoy/Istio `RING_HASH` on an `X-Entity-Id` header, or NGINX
`hash $http_x_entity_id consistent`) routes by entity. Fruitfly pods are
rule-evaluation workers and nothing else. Producers (or a thin gateway) set
the header from the payload.

**Shape B — self-routing (zero infra dependencies).** Any fruitfly pod
accepts any event; if `hash(entity_id)` maps to a peer (discovered via the
headless service DNS, consistent-hash ring over ready endpoints), it
forwards the *raw bytes* over a pooled connection. Costs one extra network
hop for (N-1)/N of traffic; buys a cluster with literally nothing but
Kubernetes. This is the `fruitfly`-only story for OSS users.

**Counters during rebalance.** Counters are sliding-window approximations by
design. When the ring changes (scale-out, pod restart), ~1/N of entities
move owners and their windows restart from zero — a brief undercount,
equivalent to what a process restart already does today. v1 documents this
honestly instead of building state migration. If a deployment can't tolerate
it, the escape hatch is sticky pre-partitioning at the producer (Shape A
with stable shards), not a distributed counter store.

**Rules in a cluster.** Rules ship as a ConfigMap/volume (today's rules-dir
reloader works unchanged) or via `POST /admin/rules/reload` per pod after a
rollout. Snapshot IDs are exposed at `/admin/rules`, so a simple check
confirms the fleet converged.

**Sizing at the target.** 10M events/sec ÷ ~200k events/sec/core ≈ 50–70
cores → e.g. 2× 32-core pods with headroom, or 9× 8-core pods. HPA on CPU
works because the workload is CPU-bound and shard movement is cheap by
design.

---

## 7. What does *not* change

- Immutable snapshot + `atomic.Pointer` swap (the reload model is already
  the right one).
- The counter ring-buffer bucket layout (`36d6e04`).
- The per-event-type rule index.
- Starlark as the rule language, `verdict()`/`memo()`/`counter()` API,
  priority-then-weight resolution.
- The admin surface (`/admin/health`, `/admin/ready`, `/admin/rules`).
- Single-binary, CGO-free-by-default builds (DuckDB moves behind a tag).

## 8. Delivery phases

Each phase is independently shippable and benchmarked against
`executor/bench_test.go` before/after:

1. **Output decoupling** — `Sink` interface, emission policy, batched
   webhook, DuckDB behind a build tag. (Removes the first wall; no hot-path
   changes.)
2. **Entity-sharded workers** — hash dispatch, SPSC rings, plain-map
   counters, watchdog + step-limit timeouts, pooled results. (Hot path goes
   lock-free and timer-free; the code gets smaller.)
3. **Lazy events** — `RawEvent`, envelope scanner, `LazyEvent` Starlark
   view, batch/stream ingest endpoints. (Kills the allocation problem.)
4. **Tier-1 prefilters** — `match` lowering, flat index with inline
   predicates, parallel compile, pre-warmed snapshots. (Makes 10k rules
   cheap.)
5. **Cluster mode** — consistent-hash self-routing, peer discovery via
   headless service, deployment manifests + Envoy example. (Makes 10M/sec a
   replica count.)
