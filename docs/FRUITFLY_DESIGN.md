# Fruitfly - Rules Engine Design

## Problem Statement

Fruitfly is a self-contained, embedded rules engine that evaluates Starlark rules against JSON events in real time. It has zero external dependencies: no Kafka, no PostgreSQL, no Redis. All storage is embedded (DuckDB), all caching is in-process, and the only external interaction is sending results to a webhook endpoint. It targets modest scale (100 events/second, O(100) rules) on a single multi-core machine with strict concurrency constraints: no locks, full core utilization, and hot-reloadable rules.

### Design Lineage

Fruitfly inherits the core pipeline shape from Hummingbird (Input -> Executor -> Output) but strips away all distributed infrastructure. Where Hummingbird uses Kafka, PostgreSQL, and Redis, Fruitfly embeds everything. Where Hummingbird scales horizontally across instances, Fruitfly scales vertically across CPU cores on a single machine.

---

## Feasibility Assessment

The three hardest constraints are: (1) no locks, (2) hot-reloadable rules, and (3) multi-core utilization. These are feasible together, and here is why.

**No locks + hot reload**: Hummingbird already solves this with `atomic.Pointer` for the rules map. A background goroutine builds a new immutable rules snapshot, then atomically swaps the pointer. Workers read the pointer on each event. No mutex required. This is a proven pattern.

**No locks + multi-core**: Go channels are the synchronization primitive. The pipeline is structured as bounded channels between stages. Workers are independent: each reads from a shared input channel (Go's channel receive is lock-free from the goroutine's perspective -- the runtime handles scheduling), processes the event with its own local state, and writes to a shared output channel. No shared mutable state between workers.

**No locks + DuckDB**: DuckDB supports concurrent reads but serialized writes. Since only the output stage writes, and we funnel all writes through a single dedicated writer goroutine that receives from a channel, no mutex is needed. The channel serializes write access naturally.

**Honest caveat**: "No locks" means no locks in *our* code. Go channels, `atomic.Pointer`, and DuckDB internals all use locks or CAS operations under the hood. The constraint is interpreted as: the application code contains zero `sync.Mutex`, zero `sync.RWMutex`, zero `sync.Pool`, and zero manual lock management. All synchronization is via channels and atomic operations.

---

## Language Recommendation: Go

**Recommendation: Use Go, not Rust.**

### Reasoning

| Factor | Go | Rust |
|--------|-----|------|
| Starlark support | `go-starlark` is the reference implementation, battle-tested at Google/Bazel. First-class, maintained, production-grade. | `starlark-rust` exists (used by Buck2) but is a less common choice. Viable, but less ecosystem support for embedding use cases. |
| DuckDB embedding | `go-duckdb` via CGO. Works. DuckDB's C API is stable. | `duckdb-rs` via FFI. Also works. Slightly more ergonomic Rust bindings. |
| Goroutines / concurrency | Native goroutines, channels, `select` -- all first-class. The requirement literally says "goroutines." | Tokio async tasks + channels achieve similar results. But the requirement explicitly names goroutines, which is a Go concept. |
| No-lock channel patterns | `chan`, `atomic.Pointer`, `atomic.Int64` -- idiomatic Go. Hummingbird already proves this pattern works. | `crossbeam` channels, `Arc<AtomicPtr>` -- achievable but more ceremony. |
| Developer productivity | Faster iteration for a system of this complexity. Simple concurrency model. Easy to onboard new developers. | Slower iteration, but stronger compile-time guarantees. More boilerplate for async patterns. |
| Binary deployment | Single static binary. `CGO_ENABLED=1` for DuckDB (acceptable). | Single static binary. Smaller, no GC pauses. |
| GC pauses at 100 evt/s | Irrelevant. At 100 events/second, GC pauses (sub-millisecond with modern Go) are noise. This is not a latency-critical HFT system. | No GC. Irrelevant advantage at this scale. |

**The decisive factors are**: (1) `go-starlark` is the canonical Starlark implementation, (2) the requirement explicitly names goroutines, (3) Hummingbird already validates the architecture in Go, and (4) developer productivity wins at this scale. Rust's advantages (memory safety guarantees, no GC) do not justify the productivity cost for a 100 evt/s system.

---

## Architecture

### High-Level Data Flow

```
JSON Events (HTTP :8080)
        |
        v
  +------------+    bounded    +-----------+    bounded    +------------+
  |   Ingest   | ---channel--> |  Executor | ---channel--> |   Output   |
  |   (HTTP)   |               |  (Workers)|               |  (1 goro)  |
  +------------+               +-----------+               +-----+------+
                                    |                        |        |
                                    v                     DuckDB   Webhook
                              atomic.Pointer             (batch)   (HTTP POST)
                              (rules snapshot)
                                    ^
                                    |
                              +-----------+
                              |   Rule    |
                              |  Reloader |
                              +-----------+
                              (file watch /
                               poll / HTTP)
```

### Single HTTP Server

Fruitfly runs one `http.Server` on a single port (default `:8080`) with route-based separation:

| Route | Method | Purpose |
|-------|--------|---------|
| `/events` | POST | Event ingestion |
| `/admin/health` | GET | Liveness check |
| `/admin/ready` | GET | Readiness check |
| `/admin/metrics` | GET | Prometheus metrics |
| `/admin/rules` | GET | Currently loaded rules |
| `/admin/rules/reload` | POST | Trigger rule reload (202 Accepted) |

At 100 evt/s there is no contention between event and admin routes. Security assumption: the ingestion port is on a trusted internal network. If untrusted clients can reach :8080, add middleware that restricts /admin/* routes by source IP or authentication token.

### Component Responsibilities

| Component | Responsibility |
|-----------|---------------|
| Ingest | Accept JSON events via HTTP, validate, push to executor channel |
| Executor | Worker pool evaluates Starlark rules, produces results |
| Output | Single goroutine: batch-writes to DuckDB, sends webhooks (best-effort) |
| Rule Reloader | Watch for rule changes, compile, atomically swap snapshot |

---

## Input Stage

### Event Ingestion

Fruitfly accepts JSON events via `POST /events` on `:8080`. This is the only input interface. For testing and replay, use `curl` or a script that POSTs events. Stdin and file-watcher modes were considered but rejected as unnecessary abstractions -- HTTP covers all production and testing use cases.

### Event Validation

Events are validated for:
- Valid JSON
- Required fields: `event_type` (string), `timestamp` (RFC3339). `event_id` is optional (auto-generated UUIDv7 if missing)
- **Field type enforcement**: Required fields must have the correct type. For example, `event_type` supplied as an integer is rejected with HTTP 400. This is validated at the Ingest boundary, not deferred to Starlark evaluation.
- Size limit: 256KB default (configurable). At 100 evt/s this is generous.
- `event_id` generated (UUIDv7) if missing

Invalid events are logged with the error reason and dropped. At this scale, a structured log line per invalid event is sufficient -- no dead-letter queue needed.

### Event Schema

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| event_id | string | yes (auto-generated if missing) | Unique event identifier |
| event_type | string | yes | Category of event (must be a string, not int/bool/null) |
| timestamp | string (RFC3339) | yes | When the event occurred |
| payload | object | no | Arbitrary event data accessible to rules |

### Backpressure

The input stage writes to a bounded channel (capacity: 100, ~1 second of buffer at 100 evt/s). If the channel is full, the HTTP handler returns `429 Too Many Requests` immediately (non-blocking send). No events are silently dropped.

---

## Rules Module

### Rule Format

Rules are Starlark files stored in a configured directory on the local filesystem. Each file is one rule.

```python
# rules/spam_check.star

# Metadata (parsed from top-level assignments)
rule_id = "spam-check-v1"
event_type = "post"
priority = 100

def evaluate(event):
    if event["payload"].get("spam_score", 0) > 0.9:
        return verdict("block", reason="spam score too high")
    return verdict("approve")
```

### Rule Metadata

| Field | Type | Description |
|-------|------|-------------|
| rule_id | string | Unique rule identifier (from Starlark global) |
| event_type | string | Which event types this rule applies to ("*" for all) |
| priority | int | Higher priority rules take precedence in verdict resolution |

### Rule Types

A single `rules.Rule` type contains all rule data: metadata (`RuleID`, `EventType`, `Priority`) and Starlark runtime state (`Program`, `Globals`). Only the `executor` package imports `rules`. The `ingest` and `output` packages depend only on `types` (Event, Result, Verdict) and have no transitive dependency on `go-starlark`.

### Pre-Compilation

Same strategy as Hummingbird:
1. Parse Starlark source to AST
2. Compile to reusable `starlark.Program`
3. Store compiled program in an immutable `Snapshot`
4. At evaluation time, only program initialization + execution occurs

### Hot Reload

```
Filesystem (rules/*.star)
        |
        v
  Rule Reloader goroutine
  (fsnotify + periodic poll fallback)
        |
        | 1. Detect change (fsnotify / poll / HTTP signal)
        | 2. Read all .star files
        | 3. Parse + compile each (via Compiler.CompileDir)
        | 4. Validate (syntax, required globals)
        | 5. Sort by event_type, then priority
        | 6. Build new immutable Snapshot
        | 7. Log snapshot metadata (structured log)
        |
        v
  atomic.Pointer.Store(&newSnapshot)
        |
        v
  Workers see new rules on next event
  (atomic.Pointer.Load())
```

**Reload trigger**: `fsnotify` for immediate detection, plus a periodic poll (default: 10s) as a fallback in case fsnotify misses an event (e.g., on NFS mounts).

**Atomicity**: The reloader builds the entire new snapshot before swapping. If any rule fails to compile, the entire reload is rejected and the old snapshot remains active. This is all-or-nothing.

**No locks**: `atomic.Pointer` is the only synchronization. Workers never block.

**Snapshot logging**: On each successful reload, the Reloader emits a structured log line with the snapshot ID, loaded-at timestamp, rule count, and rule IDs. This provides an audit trail of which rules were active at any point in time. No dedicated DuckDB table or channel needed -- structured logs are the audit trail.

### Rule Reload via HTTP

`POST /admin/rules/reload` triggers an immediate reload. The implementation is channel-based fire-and-forget:

1. `Reload()` performs a non-blocking send on an internal `reloadCh` (capacity 1). If a reload is already pending, the signal is coalesced (no queue buildup).
2. The `Run()` goroutine is the sole consumer of `reloadCh`. All `CompileDir` calls happen inside `Run`'s `select` loop, so recompilation is fully serialized. There is no race between HTTP-triggered and fsnotify-triggered reloads.
3. The HTTP handler returns **202 Accepted** immediately. The caller does not wait for compilation to complete. Callers can poll `GET /rules` to verify the new snapshot is active.

---

## Executor Module

### Worker Pool

```
Input Channel (bounded, cap=100)
        |
        +---> Worker 0 --+
        +---> Worker 1 --+--> Output Channel (bounded, cap=100)
        +---> Worker 2 --+
        +---> ...        --+
        +---> Worker N --+
```

**Worker count**: `runtime.NumCPU()` by default. At 100 events/second with no I/O-bound UDFs (no network calls during evaluation), 8 workers on 8 cores gives ~800 evt/s capacity at 10ms/event -- 8x headroom. If I/O-bound rules are added later, increase the worker count then.

### Worker Isolation

Each worker owns:
- Its own Starlark thread (go-starlark `Thread` struct, not reentrant)
- A pre-allocated memoization map, reused across events, cleared via `defer clear()` between events
- A local counter map (time-bucketed)
- A predeclared UDF dict (built once at worker init, stable for worker lifetime)
- An eval cache (`map[string]starlark.Callable`) that caches the `evaluate` function per rule, invalidated on snapshot change

Workers share (read-only):
- Rules snapshot (via `atomic.Pointer.Load()`)
- Input channel (receive)
- Output channel (send)

No shared mutable state between workers. No locks.

### Snapshot-Per-Event Isolation

Workers load the `Snapshot` pointer **once per event** via `atomic.Pointer.Load()`. If the Reloader swaps the snapshot during evaluation, the in-flight event continues with the snapshot it loaded at the start. This guarantees consistent rule evaluation within a single event -- no partial rule set application.

### Memoization

Each worker owns a pre-allocated `map[string]any` for single-event memoization. The pattern uses a deferred clear to guarantee cleanup even if a rule panics:

```go
func (w *worker) processEvent(event types.Event) types.Result {
    defer w.clearMemo()
    // ... evaluate rules using w.memo ...
}

func (w *worker) clearMemo() {
    for k := range w.memo {
        delete(w.memo, k)
    }
}
```

This avoids `sync.Pool` entirely. Per-worker maps are simpler (no internal locks, no GC interaction, no type assertions) and sufficient at 100 evt/s. The deferred clear guarantees the memo map is always clean for the next event, even if a rule panics during evaluation.

**Single-event memoization only.** There is no cross-event caching of memo values. No persistent cache. No shared cache between workers. Each event evaluation starts with an empty memo map and discards all cached values when evaluation completes. This is a deliberate simplification over Hummingbird's Redis-backed UDF caching.

**Eval cache is cross-event.** The one exception to "no cross-event caching" is the eval cache: each worker caches the Starlark `evaluate` callable per rule ID, avoiding repeated `Program.Init` calls. This cache is invalidated whenever the rules snapshot changes. UDFs are built once per worker at init and reused across all events and rules.

### Rule Evaluation Flow

For each event:
1. `atomic.Pointer.Load()` to get current `Snapshot` (loaded once, used for all rules on this event)
2. If `snap.ID` differs from the worker's cached snapshot ID, invalidate the eval cache
3. `snapshot.RulesForEvent(event.EventType)` to get indices of applicable rules (pre-indexed in snapshot)
4. **All matching rules are evaluated** -- there is no short-circuit. For each matching rule, sorted by priority (descending):
   a. Create Starlark thread with timeout (via `context.WithTimeout`)
   b. Check eval cache for a cached `evaluate` callable for this rule
   c. On cache miss: execute compiled program via `Program.Init` with worker UDFs, extract and cache the `evaluate` function
   d. Call `evaluate(event)` (cached or freshly initialized)
   e. Collect verdict
5. Resolve final verdict (see Verdict Resolution below)
6. Build result and send to output channel

### Verdict Resolution

All matching rules are evaluated (no short-circuit). The final verdict is resolved explicitly:

1. Collect all successful rule results (ignore rules with errors).
2. Group by priority (highest first).
3. Among rules at the **highest priority** that returned a verdict: resolve ties by verdict weight -- **block (3) > review (2) > approve (1)**.
4. If no rules returned a verdict (all failed or none matched): default to `approve`.

This means: if rules at priority 100 all return "approve" but a rule at priority 50 returns "block", the priority-100 "approve" wins. Verdict weight only breaks ties among rules at the same priority level.

| Scenario | Result |
|----------|--------|
| No rules match event type | approve (default) |
| Single rule triggers | That rule's verdict |
| Multiple rules, different priorities | Highest priority verdict wins |
| Multiple rules, same priority, conflicting verdicts | block (3) > review (2) > approve (1) |
| Rule execution error | Rule is marked as failed, does not contribute a verdict |
| All rules fail | approve (default), with failures logged |

### In-Memory Counters

Each worker maintains its own time-bucketed counter map using `atomic.Int64` values (`map[counterKey]*atomic.Int64`). Workers increment their local counters atomically.

The `counter()` UDF does **not** query a single worker's shard. Instead, it calls a pool-level `CounterSum` method that reads across all workers:

```go
func (p *Pool) CounterSum(entityID, eventType string, windowSeconds int) int64 {
    var total int64
    for _, w := range p.workers {
        total += w.counterQuery(entityID, eventType, windowSeconds)
    }
    return total
}
```

Each `counterQuery` reads `atomic.Int64` values -- no mutex, no locks, ~1ns per atomic read. At 8 workers this summation takes nanoseconds. This preserves the no-locks constraint while giving correct cross-worker counts. Without this, per-worker-only counters undercount by a factor of N (worker count), making rate-limiting rules like `counter("user:123", "post", 3600) > 10` effectively useless.

### Timeouts

| Level | Default | Description |
|-------|---------|-------------|
| Event | 5s | Total time for all rules on one event |
| Rule | 1s | Maximum time for a single rule evaluation |

These are deliberately lower than Hummingbird's 30s/5s because Fruitfly has no network UDFs in the hot path (no Redis, no external DB lookups during evaluation).

### Backpressure

If the output channel is full, the worker blocks. This is intentional: it propagates backpressure from the output stage (slow webhook, slow DuckDB) back through the executor to the input stage. The entire pipeline slows down together rather than dropping results.

---

## Output Stage

A single output goroutine reads from the executor result channel, batch-writes to DuckDB, and fires off webhook goroutines. No FanOut module. No separate DuckDB Writer and Webhook Sender modules.

### DuckDB Writer

**Why a single writer**: DuckDB supports concurrent reads but concurrent writes require internal locking. By funneling all writes through one goroutine that reads from a channel, we avoid contention entirely. At 100 events/second, a single writer with batch inserts is more than sufficient.

**Batching**: Flush when batch reaches 100 results OR 500ms elapses (whichever first). Uses prepared statements in a transaction for batch inserts. At steady-state 100 evt/s, the timer fires first, yielding ~50-row batches at 2 flushes/second.

**Schema**:

```sql
CREATE TABLE results (
    event_id       VARCHAR PRIMARY KEY,
    event_type     VARCHAR NOT NULL,
    verdict        VARCHAR NOT NULL,  -- approve, block, review
    triggered_rules JSON,
    failed_rules   JSON,
    payload        JSON,              -- original event payload
    latency_us     BIGINT,
    processed_at   TIMESTAMP DEFAULT current_timestamp
);
```

**DuckDB configuration**: Database file path is configurable (default: `fruitfly.duckdb`). WAL mode enabled for crash recovery. Memory limit 256MB. At 100 evt/s, the dataset grows at ~30MB/day -- trivial.

**Retention**: Daily `DELETE FROM results WHERE processed_at < now() - interval '30 days'`.

### Webhook Delivery

Each result is sent via a goroutine: `go sendWebhook(result)`. At 100 results/second, this is 100 short-lived goroutines/second -- trivial for Go. Each does one HTTP POST with up to 3 retries (exponential backoff from 100ms). No webhook channel, no worker pool.

**DuckDB writes are guaranteed. Webhook sends are best-effort.** If the webhook endpoint is down, retries exhaust and the failure is logged. The result is always in DuckDB for manual replay.

**Failure handling**: After exhausting retries, the failure is logged (via `log.Warn`) with the event_id and the result is available in DuckDB for manual replay.

**Payload format**:

```json
{
  "event_id": "evt-123",
  "event_type": "post",
  "verdict": "block",
  "triggered_rules": [
    {"rule_id": "spam-check-v1", "verdict": "block", "reason": "spam score too high"}
  ],
  "failed_rules": [],
  "latency_us": 1234,
  "processed_at": "2026-02-19T12:00:00Z"
}
```

---

## Built-in UDFs

UDFs are registered at startup and available to all Starlark rules. Fruitfly ships with a minimal set. Since there are no external services, UDFs are pure functions or use in-process state.

| UDF | Description | Usage |
|-----|-------------|-------|
| `verdict(type, reason="")` | Return a verdict (approve/block/review) | `verdict("block", reason="spam")` |
| `counter(entity_id, event_type, window_seconds)` | In-memory sliding-window counter | `counter("user-123", "post", 3600)` |
| `memo(key, func)` | Single-event memoization. Returns cached value if `key` was already computed during this event's evaluation; otherwise calls `func`, caches, and returns the result. | `memo("spam_score", lambda: compute_spam_score(event))` |
| `now()` | Current Unix timestamp | `now()` |
| `log(message)` | Structured log output from rule | `log("score=" + str(score))` |
| `hash(value)` | SHA256 hash of a string value | `hash(event["payload"]["email"])` |
| `regex_match(pattern, text)` | Check if text matches a regex pattern | `regex_match("^spam.*", subject)` |

### In-Memory Counters (UDF detail)

Counters replace Hummingbird's PostgreSQL-backed counters with an in-process implementation:

- **Data structure**: Per-worker time-bucketed counter maps using `atomic.Int64` values. Each worker increments its own counters atomically. The `counter()` UDF calls a pool-level `CounterSum` that reads all workers' counters atomically, giving correct cross-worker totals with no locks (~1ns per atomic read, nanoseconds total at 8 workers).
- **Expiry**: Buckets older than the maximum configured window (default: 1 hour) are garbage collected periodically within each worker.

---

## Configuration

Single YAML file (default: `fruitfly.yaml`). Most settings are hardcoded constants -- only values that genuinely vary between deployments are configurable.

```yaml
address: ":8080"            # single server for events + admin
rules_dir: "./rules"
duckdb_path: "fruitfly.duckdb"
webhook_url: "https://example.com/hook"
workers: 0                  # 0 = NumCPU
log_level: info             # debug | info | warn | error
```

Everything else (channel buffers, timeouts, batch sizes, retry counts, DuckDB memory limits, retention period) is a hardcoded constant defined in their respective packages.

---

## Concurrency Model Summary

| Component | Goroutines | Synchronization | Shared State |
|-----------|-----------|-----------------|--------------|
| HTTP server | Go stdlib (goroutine per request) | Writes to bounded input channel | None |
| Executor workers | NumCPU | Read from input channel, write to output channel | Rules snapshot (atomic.Pointer, read-only) |
| Output writer | 1 | Reads result channel, batch-writes DuckDB | DuckDB connection (single-writer) |
| Webhook sends | 1 per result (short-lived) | HTTP POST, independent | HTTP client (stateless) |
| Rule reloader | 1 | atomic.Pointer.Store | Rules snapshot (write-only via atomic) |

**Total long-lived goroutines at default config (8 cores)**: ~12. Webhook goroutines are short-lived (one per result, ~100/second).

**Lock inventory**: Zero `sync.Mutex`, `sync.RWMutex`, or `sync.Pool` in application code. All synchronization via:
- `chan` (two bounded channels: eventChan, resultChan)
- `atomic.Pointer` (rules snapshot)
- `atomic.Bool` (ready flags)
- `atomic.Int64` (per-worker event counters, read cross-worker by `CounterSum`)
- `atomic.Uint64` (metrics counters)
- `sync.Map` (per-worker counter maps — pragmatic deviation for cross-worker counter reads)

---

## Performance Characteristics

### Throughput Budget

At 100 events/second with 100 rules per event:

| Stage | Per-event cost | Throughput headroom |
|-------|---------------|-------------------|
| JSON validation | ~10us | 100K evt/s |
| Rule lookup (atomic load + linear scan) | ~1us | Millions/s |
| Starlark execution (100 rules, ~0.1ms each, Init cached) | ~3-5ms | With 8 workers: 1600+ evt/s |
| DuckDB batch insert (amortized) | ~50us | 20K evt/s |
| Webhook POST | ~5ms (network) | 100 goroutines/s: trivial |

**Bottleneck**: Starlark rule evaluation. Each worker caches the `evaluate` callable per rule, skipping `Program.Init` (the most expensive operation) on the hot path. Init is only called once per (worker, rule) pair and again on snapshot change. With caching, per-event cost drops to ~3-5ms across 100 rules, and 8 workers provide 8x parallelism = 1600+ evt/s capacity — well over 10x headroom above the 100 evt/s target.

**DuckDB batch reality**: With `batch_size=100` and `flush_interval=500ms` at a steady 100 evt/s, the 500ms timer fires before the batch fills, yielding ~50-row batches at 2 flushes/second. The batch size cap matters under burst traffic.

### Memory Budget

| Component | Estimate |
|-----------|----------|
| Go runtime + binary | ~50MB |
| DuckDB embedded | 256MB (configured limit) |
| Input channel (100 events * 256KB max) | 25MB worst case, ~1MB typical |
| Rules (100 compiled programs) | ~10MB |
| Per-worker state (16 workers, incl. memo maps) | ~5MB |
| Counter state (1 hour window) | ~20MB |
| **Total** | **~340MB typical, 360MB peak** |

Fits comfortably on any modern server.

### Latency

| Percentile | Expected | Driven by |
|------------|----------|-----------|
| p50 | <5ms | Starlark evaluation |
| p99 | <20ms | Starlark evaluation + channel contention |
| p999 | <100ms | GC pause (rare at this allocation rate) |

---

## Admin / Observability

### HTTP Endpoints

Single server on `:8080`:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/events` | POST | Submit an event for evaluation |
| `/admin/health` | GET | Liveness check (always 200) |
| `/admin/ready` | GET | Readiness: 200 when reloader and DuckDB are ready |
| `/admin/metrics` | GET | Prometheus metrics |
| `/admin/rules` | GET | Currently active rules |
| `/admin/rules/reload` | POST | Trigger rule reload (202 Accepted) |

**`/admin/ready` semantics**: Returns 200 when `reloader.ready.Load() && writer.ready.Load()`. Used by load balancers to gate traffic.

### Key Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `fruitfly_events_received_total` | Counter | Events received by input stage |
| `fruitfly_events_invalid_total` | Counter | Events that failed validation |
| `fruitfly_events_processed_total` | Counter | Events evaluated by executor |
| `fruitfly_events_backpressure_total` | Counter | Events rejected due to full input channel |
| `fruitfly_verdict_total` | Counter (labeled) | Verdicts by type |
| `fruitfly_rule_errors_total` | Counter | Rule execution failures |
| `fruitfly_eval_latency_seconds` | Histogram | End-to-end event evaluation latency |
| `fruitfly_duckdb_write_latency_seconds` | Histogram | DuckDB batch write latency |
| `fruitfly_webhook_sent_total` | Counter | Successful webhook deliveries |
| `fruitfly_webhook_errors_total` | Counter | Webhook delivery failures (retries exhausted) |
| `fruitfly_rules_loaded` | Gauge | Number of currently loaded rules |
| `fruitfly_rules_reload_total` | Counter | Successful rule reloads |
| `fruitfly_rules_reload_errors_total` | Counter | Failed rule reloads |

---

## Graceful Shutdown

Shutdown is initiated by SIGTERM/SIGINT and coordinated via context cancellation and channel closure. Each channel has exactly one closer (the goroutine that writes to it). Channel closure cascades left-to-right through the pipeline.

**Total shutdown timeout**: 10s (configurable). After timeout, goroutines are abandoned (logged).

### 5-Step Shutdown Sequence

| Step | Action | Owner | Mechanism |
|------|--------|-------|-----------|
| 1 | Stop accepting new events | Ingest | Cancel ctx, `http.Server.Shutdown()` |
| 2 | Close input channel | `main()` | `close(eventChan)` after Ingest returns |
| 3 | Workers drain in-flight events | Executor | Workers `range` over eventChan; exit on close |
| 4 | Close output channel | Executor | `Pool.Run()` closes resultChan after `WaitGroup.Wait()` |
| 5 | Output drains, flushes DuckDB, closes | Output | `Writer.Run()` ranges over resultChan, flushes, closes DB |

After step 5, `main()` shuts down the HTTP server and exits. In-flight webhook goroutines get a 5-second grace period via context cancellation.

### Channel Ownership

| Channel | Created by | Written by | Closed by |
|---------|-----------|-----------|----------|
| eventChan | main() | Ingest | main() (after Ingest returns) |
| resultChan | main() | Executor workers | Executor (Pool.Run, after all workers exit) |

**Invariants**: Two channels. Two closers. Channel closure cascades left-to-right. No goroutine is abandoned without logging.

---

## Comparison with Hummingbird

| Aspect | Hummingbird | Fruitfly |
|--------|-------------|----------|
| Input | Kafka consumer | HTTP |
| Rules storage | PostgreSQL | Local filesystem |
| Rules language | Starlark | Starlark |
| Rule reload | Poll PostgreSQL + atomic swap | fsnotify + atomic swap (channel-based fire-and-forget) |
| Executor | Worker pool + channels | Worker pool + channels (same pattern) |
| Deduplication | Per-worker Bloom filter | None (simpler input model, caller is responsible) |
| UDF caching | Redis | In-process (single-event memoization only, no cross-event cache) |
| Counters | PostgreSQL time-bucketed | In-memory time-bucketed (per-worker atomic increment, on-demand cross-worker sum) |
| Output: durable | Kafka topic | DuckDB (embedded) |
| Output: real-time | Webhooks | Webhooks |
| Audit | Kafka -> PostgreSQL consumer | DuckDB (results) + structured logs (rule snapshots) |
| Scaling model | Horizontal (Kafka partitions) | Vertical (goroutines on one machine) |
| External dependencies | Kafka, PostgreSQL, Redis | None |
| Lock-free hot path | Mostly (UDF registry uses RWMutex) | Fully (no mutexes, no sync.Pool in application code) |

---

## Known Limitations and Tradeoffs

1. **Single-machine only**: No horizontal scaling story. If 100 evt/s is exceeded, the answer is "buy a bigger machine" or "run multiple independent instances with a load balancer" (accepting split counters).

2. **In-memory counters**: Counters are in-process only (no cross-instance sharing). The `counter()` UDF sums across all workers on a single machine via atomic reads, so counts are exact within one instance. Hummingbird's PostgreSQL counters are exact and cross-instance. For multi-instance deployments, each instance sees only its own traffic.

3. **No dead-letter queue**: Invalid events are logged and dropped. If you need reprocessing of invalid events, ship the logs to a file and replay.

4. **Webhook delivery is best-effort**: If the webhook endpoint is down and retries are exhausted, the delivery is lost. The result is always in DuckDB, so manual replay is possible.

5. **DuckDB single-writer**: One writer goroutine caps write throughput. At 100 evt/s with batching, this is ~2 batch inserts per second (~50 rows each). DuckDB handles this trivially. At 10K evt/s, this would need revisiting.

6. **DuckDB disk full**: If the disk fills, DuckDB flush operations fail. Backpressure propagates upstream (Output blocks, Executor blocks, Ingest returns 429). The operator must intervene (add disk space or reduce retention).

7. **CGO required**: DuckDB and go-starlark both work with CGO. The binary is not pure Go. This means cross-compilation requires a C toolchain for the target platform.

8. **No encryption at rest**: DuckDB data is unencrypted on disk. If needed, use filesystem-level encryption (LUKS, FileVault, etc.).

---

## Open Questions

1. **Event replay from DuckDB**: Should there be a built-in mechanism to replay events from DuckDB through the rules engine (e.g., for testing new rules against historical data)?

2. **Rule testing CLI**: Should Fruitfly include a `fruitfly test` subcommand that runs rules against sample events without starting the full pipeline?

3. **Multi-tenant**: Should rules be scoped to tenants, or is this always single-tenant? Single-tenant is assumed for now.

4. **Webhook authentication**: Should the webhook sender support HMAC signing of payloads, OAuth2 bearer tokens, or mutual TLS? Minimum viable: a static `Authorization` header.
