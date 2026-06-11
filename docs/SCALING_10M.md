# Scaling Fruitfly to 10M events/sec

**Status**: Design proposal — rev 2, incorporating external code-review
findings (counter-key affinity, batch-vs-affinity routing, shed policy,
frozen globals, sizing arithmetic)
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
2. **Key affinity is the only coordination primitive.** All shared state
   (counters) is keyed. Within a process, counter storage shards by counter
   key; across pods, events route by a declared routing key. Exact counters
   are guaranteed exactly where key and routing affinity coincide — a
   constraint the design states up front (§4, §6) instead of discovering in
   production. Scaling is sharding, nothing else.
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

Net: **~37µs/event/core today → 3–6µs/event/core**, i.e. 167–333k
events/sec per core. 10M events/sec is then **30–60 cores of evaluation;
provision 50–70 for headroom** (one or two large pods, or a handful of small
ones) when rules are well-partitioned, degrading gracefully (more cores)
when many rules pass tier 1. The 200ns decode target assumes SIMD-grade
field scanning; a portable hand-rolled scanner at ~500ns still fits the
budget. Batch transport (§2) amortizes HTTP/syscall cost to noise per event;
it is not free and is why batching is the flagship endpoint.

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
- `POST /events/batch` — NDJSON (one event per line); one request carries
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

Events without the declared entity field must not collapse onto one shard
(`hash("")` is a guaranteed hot spot at 10M/s): routing falls back to
`hash(event_id)` — server-generated UUIDv7 when the client omits it — which
spreads them uniformly. Such events evaluate normally; they simply have no
routing affinity, so the counter-affinity rule in §6 treats any `counter()`
key they use as non-affine.

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
a configured policy: `reject` (429 the whole batch) or `drop-oldest` (shed
the queue head; every shed event increments an exposed metric). A third
policy — "degrade to tier-1-only evaluation" — was considered and rejected:
tier-1 prefilters (§3) are boolean gates, not verdicts, so a degraded
verdict has no sound definition for a block/approve engine. Single-CPU
default stays `reject` — today's behavior.

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

The lowering semantics are fixed, not implementation-defined:

- `match` is lowered from the module global's value after initialization;
  whatever dict the module produces is validated and lowered at compile
  time (a non-dict or malformed shape is a compile error).
- A missing or type-mismatched field makes that clause **false** — the rule
  is skipped, never errored. (`exists` is the explicit way to test
  presence.)
- All JSON numbers compare as float64; integer constants in clauses are
  widened. `in` lists must be homogeneous scalars, checked at compile time.
  `prefix` on a non-string field is false.
- Observability: a prefiltered rule never appears in triggered/failed
  accounting, so each rule exposes a `prefiltered` counter — an
  over-aggressive `match` must be visible, not silent.

This is the single most important lever for 10k rules: the per-event cost
becomes `(matched rules × 10ns) + (surviving rules × ~1.5µs)`, and rule
authors control the surviving set.

**Compilation parallelizes; snapshots pre-warm.**

- `CompileDir` compiles files across `GOMAXPROCS` goroutines (compilation is
  pure). 10k rules compile in roughly the time 10k/N took before.
- Module globals are frozen after `Program.Init` (starlark-go freezes only
  via `ExecFile`), deliberately tightening the contract: mutating
  module-level state is a runtime error instead of silent per-worker
  mutable globals (whose value would depend on which worker an event
  happens to land on). *Future work:* pre-run `Init` once per rule at
  compile time and share the frozen `evaluate` callables across workers,
  eliminating the per-worker re-init warmup after a snapshot swap (now
  rare, since unchanged content no longer republishes).
- The reloader stops recompiling on a timer: poll ticks hash the rules
  directory contents and skip publication when nothing changed, so snapshot
  IDs change only when rules do. (Today every 10-second poll mints a new
  snapshot ID and silently invalidates every worker's eval cache.)
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

**Counters shard by counter key, not by who's asking.** An earlier revision
of this document claimed entity routing lets each worker keep counters in a
plain private map with zero synchronization. That is wrong for the actual
API: `counter(entity_id, …)` takes an *arbitrary* key — a rule processing a
sender-routed event may legitimately count by recipient, by IP, or by a
global literal (the docs showcase exactly this) — so increments cannot be
assumed local to the processing worker, and per-worker private maps would
silently undercount. The corrected design:

- Counter storage is a fixed array of shards (e.g. 256), selected by
  `hash(counter key)`. There is **one home shard per key**, not one copy
  per worker.
- The per-slot `bucket`/`count` atomics from `36d6e04` carry all read/write
  traffic, exactly as today; the shard map itself synchronizes only on
  series creation/GC.
- Any worker may increment any key; a `counter()` read touches exactly one
  shard — one map lookup plus 60 atomic reads, ~100–450ns measured.
  `CounterSum`'s scan-every-worker shape disappears, but because keys have
  a single home shard, not because reads are worker-local.

Within a single process, arbitrary counter keys therefore stay **exact** —
identical semantics to today. What event routing buys is cache locality in
the common case (most rules count by the event's own entity) and the
cross-pod story in §6, which is where keys genuinely become constrained.

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

The cluster is the same binary repeated, with key affinity preserved one
level up: **the routing key chooses the pod exactly the way it chooses the
worker inside a pod.** No coordinator, no shared database, no gossip. The
routing key is explicit configuration: each event type declares its routing
field (default `payload.entity_id`, falling back to `event_id` when absent —
§2), and that one declaration drives in-process dispatch, pod selection, and
the counter-affinity rule below.

Two deployment shapes, both using a plain `Deployment` + headless `Service`:

**Shape A — affinity at the edge.** The load balancer in front (Envoy/Istio
`RING_HASH` on an `X-Entity-Id` header, or NGINX
`hash $http_x_entity_id consistent`) routes by entity. Fruitfly pods are
rule-evaluation workers and nothing else. Producers (or a thin gateway) set
the header from the payload. The honest caveat: **batches and Shape A only
compose if the producer partitions each batch by the ring** — one request
per target shard — since a mixed-entity batch has no single routing header.
That is real producer-side coupling; producers that can't pre-partition
should use Shape B.

**Shape B — self-routing (zero infra dependencies).** Any fruitfly pod
accepts any event or batch; it splits batches by the ring (discovered via
the headless service DNS, consistent-hash over ready endpoints) and forwards
*raw bytes* to owning peers over pooled connections. Costs one extra network
hop for (N-1)/N of traffic; buys a cluster with literally nothing but
Kubernetes. Forwarded events carry a forwarded marker and are processed
wherever they land — **never re-forwarded**. During ring divergence
(rollouts, scale events, DNS propagation) pods briefly disagree about
ownership; the marker bounds every event to at most one hop and makes
routing loops impossible, at the cost that a misrouted event's counter
increments land on the temporarily-wrong home — the same class of
approximation as rebalance below.

**Counter keys in a cluster.** Exactness requires affinity. A `counter()`
key derived from the event's routing key (the key itself, or a declared
derivation such as a prefixed scope of it) is exact: every increment and
read for that key happens on its home pod. Any other key — recipient on a
sender-routed event, an IP, a global literal — would scatter increments
across pods' private shards and silently undercount, which is the one
failure mode a rate-limiting primitive must never have. Cluster mode
therefore **rejects non-affine counter keys at evaluation** (the rule lands
in `FailedRules` with a clear error; statically derivable keys are linted at
compile time). The supported pattern for multi-perspective counting is
producer-side fan-out: emit a second event routed by the other perspective —
e.g. a `message-received` event routed by recipient alongside the
`message-sent` event routed by sender — and each perspective gets exact
counters with zero coordination. Single-process deployments are unaffected:
with one node, every key is affine, and today's semantics hold unchanged.

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

**Sizing at the target.** At 167–333k events/sec/core (§1), 10M events/sec
is 30–60 cores of evaluation; provision 50–70 for headroom → e.g. 2×
32-core pods, or 9× 8-core pods. HPA on CPU works because the workload is
CPU-bound and shard movement is cheap by design — but pair it with a
stabilization window (~5 minutes) so ring churn, and the 1/N counter resets
each change causes, tracks sustained load rather than noise.

---

## 7. What does *not* change

- Immutable snapshot + `atomic.Pointer` swap (the reload model is already
  the right one).
- The counter ring-buffer bucket layout (`36d6e04`).
- The per-event-type rule index.
- Starlark as the rule language, `verdict()`/`memo()`, priority-then-weight
  resolution. `counter()` keeps its signature; single-process semantics are
  unchanged, with one new cluster-mode constraint — keys must be affine to
  the event's routing key (§6).
- The admin surface (`/admin/health`, `/admin/ready`, `/admin/rules`).
- Single-binary, CGO-free-by-default builds (DuckDB moves behind a tag).

## 8. Delivery phases

Each phase is independently shippable and benchmarked against
`executor/bench_test.go` before/after:

1. **Output decoupling + reload hygiene** — `Sink` interface, emission
   policy, batched webhook; DuckDB stays in the default build but becomes
   excludable via a build tag (and is simply not configured on the 10M
   path). Reloader skips republishing when rule content is unchanged.
   (Removes the first wall; no hot-path changes.)
2. **Key-sharded execution** — hash dispatch, SPSC rings, counter shards
   keyed by counter key (§4, replacing per-worker maps + `CounterSum`),
   watchdog + step-limit timeouts, pooled results. The counter-affinity
   check ships here behind a flag (a no-op for single-node, so the cluster
   constraint is tested long before the cluster exists). (Hot path goes
   timer-free and mostly lock-free; the code gets smaller.)
3. **Lazy events** — `RawEvent`, envelope scanner, declared routing field
   with `event_id` fallback, `LazyEvent` Starlark view, batch/stream ingest
   endpoints. (Kills the allocation problem.)
4. **Tier-1 prefilters** — `match` lowering with the fixed semantics of §3,
   per-rule `prefiltered` metrics, flat index with inline predicates,
   parallel compile, pre-warmed frozen snapshots. (Makes 10k rules cheap.)
5. **Cluster mode** — consistent-hash self-routing with the one-hop
   forwarded marker, batch splitting, peer discovery via headless service,
   affinity check enforced, deployment manifests + Envoy example. (Makes
   10M/sec a replica count.)
