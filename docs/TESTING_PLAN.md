# Fruitfly Testing Plan

**Status:** v3 -- 2026-02-27

## System Overview

Starlark rules engine: HTTP events in, verdicts out (approve/block/review), persisted to DuckDB with optional webhook delivery.

```
POST /events -> Ingest -> [eventChan] -> Pool (N workers) -> [resultChan] -> Writer (DuckDB + Webhook)
                                              |
                                        atomic.Pointer[Snapshot] <- Reloader (fsnotify/poll)
```

**Core contracts:** every 202 event produces exactly one DuckDB row; verdict resolution is priority-then-weight; counters are accurate cross-worker; hot reload is atomic; backpressure returns 429 (no silent drops); graceful shutdown drains in-flight events.

---

## Conventions

**Tags:**
- **[GAP]** -- No existing test coverage. Must write from scratch.
- **[PARTIAL]** -- Some coverage exists but does not test the specific scenario described. Augments existing tests.
- **[EXISTS]** -- Covered by existing tests. Listed for inventory completeness.
- Items without a tag are new tests with no pre-existing equivalent.

**Existing test inventory:** `config/config_test.go` (7 tests: defaults, valid YAML, invalid YAML, missing file, workers=0, invalid log_level, partial YAML), `ingest/server_test.go` (8 tests: valid event, missing event_id auto-gen, invalid JSON, missing fields, oversized payload, 429 backpressure, content-type checks, event_id too long), `executor/executor_test.go`, `rules/rules_test.go`, `output/writer_test.go`, `integration/*_test.go`. Plan items below are marked "new" or "augments existing" accordingly.

**time.Now() determinism:** Several components call `time.Now()` directly (counters, `now` UDF, `processedAt`). Tests requiring deterministic time should inject a clock function or use narrow time windows with tolerance. This is an accepted trade-off for simplicity; refactor to injectable clock only if flaky tests emerge.

---

## Layer 1: Record and Replay

Capture real traffic as traces, replay to detect regressions.

| ID | Test | Target | Details |
|----|------|--------|---------|
| T1 | Trace recorder | `output/` | Tap `resultChan`, write JSONL: `{event, result, snapshot_id, timestamp}`. Off by default, config flag `trace_enabled`. |
| T2 | Replay harness | CLI | `fruitfly replay --traces <file> --rules <dir>` -- replay events, diff verdicts, report changes. |
| T3 | Trace corpus | `jetstream_test/` | Record from existing jetstream shim: posts, likes, spam bursts, mixed traffic. |

---

## Layer 2: Mutation and Chaos

Perturb inputs, rules, and environment to find edge-case failures.

### Input Mutations

| ID | Test | Target | Details |
|----|------|--------|---------|
| T4 | Payload boundaries | `executor/event.go` | Empty payload, null fields, type coercion (`char_count="5"`), extreme values, unicode (`"X" * 10000`), 255KB body, deep nesting (50 levels in `anyToStarlark`). New. |
| T5 | `anyToStarlark` default case | `executor/event.go:47` | Pass unrecognized type (e.g., `time.Time`). Verify silent `fmt.Sprintf("%v")` coercion. Assert log warning or explicit error -- current behavior silently coerces. **[GAP]** New. |
| T6 | Event ordering | `executor/pool.go` | Burst from one entity (100 events/1s), interleaved entities across workers, duplicate `event_id` (INSERT OR IGNORE), timestamp regression, rapid type switching. New. |
| T7 | Empty `entity_id` in counter UDF | `executor/udfs.go` | `entity_id` is extracted in Starlark and passed to `counter()`. Test at Starlark level: rule calling `counter("", event_type, window)` -- verify no cross-contamination between entities. Empty string becomes a shared bucket in `counterKey`. **[GAP]** New. |
| T8 | `event_id` at 256-char boundary | `ingest/server.go:107` | 256-char `event_id` (accepted), 257-char (rejected 400). Existing `TestEventID_TooLong_Returns400` tests 300-char rejection. Real gap is exact boundary. **[PARTIAL]** Augments existing. |

### Rule Mutations

| ID | Test | Target | Details |
|----|------|--------|---------|
| T9a | Invalid verdict (error path) | `executor/udfs.go:36-40` | Rule returns `verdict(type="quarantine")`. `verdictUDF` returns error (not panic). Rule goes to `failedRules`. New. |
| T9b | Rule returns None | `executor/verdict.go:48` | `evaluate()` returns `None`. `interpretVerdict` fails (`*starlarkstruct.Struct` assertion). Rule goes to `failedRules`. New. |
| T9c | Conflicting same-priority verdicts | `executor/verdict.go` | Two rules at priority=100, one approve, one block. Block wins (weight 3 > 1). New. |
| T9d | Rule panic (recovery path) | `executor/pool.go:191-196` | Rule panics during execution. `evalRule`'s `defer recover()` catches it. Rule goes to `failedRules`. New. |
| T9e | Rule timeout (infinite loop) | `executor/pool.go:198-217` | Rule with infinite loop. 1s `ruleTimeout` via `thread.Cancel`. Error contains `DeadlineExceeded`. New. |
| T10 | `memo` UDF panic in callable | `executor/udfs.go:57` | Memoized callable that panics. `starlark.Call` propagates panic -- verify `evalRule`'s `defer recover()` catches it. **[GAP]** New. |
| T11 | `regex_match` pathological pattern | `executor/udfs.go:113` | Pattern like `(a+)+$` with adversarial input. Go's `regexp` is RE2 (linear time guaranteed). Verify no hang. **[GAP]** New. |
| T12 | `hash` UDF | `executor/udfs.go:96` | Basic correctness: `hash("hello")` == known SHA-256 hex. Empty string. Large input. Trivial but untested. **[GAP]** New. |

### Environment/Chaos Mutations

| ID | Test | Target | Details |
|----|------|--------|---------|
| T13 | DuckDB failures | `output/writer.go` | Write failure (mock db), disk full (tmpfs). Verify retry-once-then-drop in `flush()`. Backpressure propagation to 429. New. |
| T14 | Webhook failures | `output/writer.go:245-272` | Endpoint down, slow (5s), 500 (retry 3x). No retry on 4xx except 429. Retries on 5xx, 429, and 3xx (3xx falls through retry loop since only `<300` exits successfully and `>=400 && <500 && !=429` exits without retry). Verify events still persist to DuckDB regardless. New. |
| T15 | Channel backpressure | `ingest/server.go:130` | `eventChan` cap=1 + rapid POSTs -> 429. `resultChan` blocked (slow writer) -> workers block -> ingest 429. No goroutine leaks. New. |
| T16 | `runRetention` cleanup | `output/writer.go:218` | Insert rows with `processed_at` 31 days ago, call `runRetention()`, verify deleted. Insert 29-day-old rows, verify retained. **[GAP]** New. |
| T27 | Webhook semaphore saturation | `output/writer.go:106-114` | Fill `webhookSem` (capacity 100), send event. Verify webhook silently dropped with log warning. Verify DuckDB insert still succeeds. **[GAP]** New. |
| T28 | Writer drain path timeout | `output/writer.go:129-147` | Cancel context while results are in-flight. Verify 5-second drain timeout flushes remaining batch. Verify behavior when drain timeout expires with items still in channel. **[GAP]** New. |

### State Mutations

| ID | Test | Target | Details |
|----|------|--------|---------|
| T17 | Hot reload under traffic | `executor/pool.go:153` | Send 1000 events, swap rules at event 500. Verify snapshot-per-event isolation (no mixed rule sets). New. |
| T18 | Hot reload with bad rules | `rules/reloader.go` | Replace rule file with syntax error mid-traffic. Reloader rejects, keeps old snapshot. No disruption. New. |
| T19 | Counter state across reloads | `executor/pool.go` | Counters persist across snapshots (they are per-worker, not per-snapshot). Verify `evalCache` invalidation on snapshot change. New. |
| T20 | Counter concurrency stress | `executor/pool.go:95` | N goroutines calling `CounterSum` while workers call `counterIncrement` concurrently. Verify no data race (`-race`), no lost increments. `sync.Map` + `atomic.Int64` should be safe but untested under contention. **[GAP]** New. |
| T29 | Counter GC threshold | `executor/pool.go:134-137,266-274` | Focused test: send exactly 1000 events, verify `gcCounters()` fires and removes expired buckets. Verify buckets within `counterMaxWindowSeconds` are retained. **[GAP]** New. |
| T30 | Nil snapshot during event processing | `executor/pool.go:140-150` | Set `atomic.Pointer[Snapshot]` to nil, send event. Verify default approve verdict returned with warning. **[GAP]** New. |
| T31 | `evalCache` rule swap | `executor/pool.go:153-156,219-243` | Snapshot with rules A,B,C swapped to snapshot with rules A,D. Verify `evalCache` is cleared (no stale callable for B served). Verify rule D is freshly compiled. New. |
| T32 | `regexCache` unbounded growth | `executor/udfs.go:111-119` | Send events triggering 10K unique regex patterns. Verify memory behavior. Note: no eviction policy exists -- this test documents the risk. **[GAP]** New. |

---

## Layer 3: Continuous Measurement

Monitor behavioral invariants post-test and in production.

| ID | Test | Target | Details |
|----|------|--------|---------|
| T21 | Invariant: lossless pipeline | DuckDB | `count(202 responses) == count(DuckDB rows)` after graceful shutdown. |
| T22 | Invariant: valid verdicts | DuckDB | No empty verdict. Verdict always in `{approve, block, review}`. |
| T23 | Invariant: no duplicates | DuckDB | `GROUP BY event_id HAVING COUNT(*) > 1` returns 0 rows. |
| T24 | Invariant: bounded latency | DuckDB | All `latency_us < 5_000_000` (event timeout). |
| T25 | Invariant: webhook parity | metrics | `events_processed == webhook_sent + webhook_errors` (when webhook configured). **Blocked:** metrics endpoint is a placeholder (`main.go:153`). Unblock when real metrics are implemented. |
| T26 | Shadow mode diff | CLI | `fruitfly shadow --old-rules <v1> --new-rules <v2> --traces <file>` -- report verdict change count, breakdown, top rule causing changes. |

---

## Layer 4: Specification as Oracle

Define correctness as executable contracts. Where an L3 invariant measures the same property, the L4 contract is the authoritative definition.

### Verdict Resolution Contract

| ID | Contract | Assertion |
|----|----------|-----------|
| T33 | Highest priority wins | `priority(100) approve + priority(50) block -> approve` |
| T34 | Same priority: weight breaks tie | `priority(100) approve + priority(100) block -> block` (block weight=3 > approve weight=1) |
| T35 | No matching rules -> default | No matching rules -> approve |
| T36 | All rules fail -> default | All matching rules fail -> approve (failedRules non-empty) |

### interpretVerdict Edge Cases

| ID | Contract | Assertion |
|----|----------|-----------|
| T37 | Non-struct return | `evaluate()` returns string/int/None -> error "must return a verdict()" |
| T38 | Struct missing `type` | Starlark struct without `type` attr -> error "missing 'type'" |
| T39 | `type` not a string | `type` attr is int -> error "must be string" |
| T40 | Struct missing `reason` | Starlark struct with `type` but no `reason` -> error "missing 'reason'" |

### Per-Rule Contracts (table-driven Go tests)

| Rule | Priority | Condition | Expected Verdict |
|------|----------|-----------|-----------------|
| `spam_like_flood.star` | 100 | entity <= 5 likes in 600s | approve |
| `spam_like_flood.star` | 100 | entity > 5 likes in 600s | block, reason contains "spam: like flood" |
| `spam_short_post.star` | 100 | post, char_count >= 10 | approve |
| `spam_short_post.star` | 100 | post, char_count < 10, entity <= 5 posts in 300s | approve |
| `spam_short_post.star` | 100 | post, char_count < 10, entity > 5 posts in 300s | block, reason contains "spam: short post flood" |
| `numeric_content.star` | 100 | post, text has <= 10 digits | approve |
| `numeric_content.star` | 100 | post, text has > 10 digits | review, reason contains "high numeric content" |
| `catchall.star` | 1 | any event | approve (overridden by higher-priority rules) |

### DefaultUDFs Stub Behavior

| ID | Contract | Assertion |
|----|----------|-----------|
| T41 | Stub counter at compile time | Rule calling `counter()` at module scope (outside `evaluate()`) gets `None` from `stubBuiltin`. Rule loads without error. |
| T42 | Stub memo at compile time | Rule calling `memo()` at module scope gets `None`. |

### System-Level Contracts

| ID | Contract | Assertion | See also |
|----|----------|-----------|----------|
| T43 | Lossless pipeline | `count(202) == count(DuckDB rows)` after shutdown | L3: T21 |
| T44 | Atomic reload | Every event uses exactly one snapshot | -- |
| T45 | Backpressure, not loss | Overload -> 429. Never drop an accepted event. | -- |
| T46 | Bounded latency | Event < `eventTimeout` (5s). Rule < `ruleTimeout` (1s). | L3: T24 |
| T47 | Counter consistency | `counter()` returns true cross-worker sum, not per-worker undercount | -- |
| T48 | Idempotent writes | Duplicate `event_id` -> 1 row, no error. First row preserved (INSERT OR IGNORE). | L3: T23 |
| T49 | Graceful shutdown | SIGTERM -> drain in-flight -> flush DuckDB -> exit 0 | -- |

### DuckDB INSERT OR IGNORE

| ID | Contract | Assertion |
|----|----------|-----------|
| T50 | Duplicate event_id at writer level | Insert result with event_id "X", insert different result with same event_id "X". Verify 1 row, first row's data preserved. **[GAP]** New. |

### Admin Endpoints

| ID | Contract | Assertion |
|----|----------|-----------|
| T51 | `/admin/health` always 200 | GET returns 200 "ok" regardless of system state. **[GAP]** New. |
| T52 | `/admin/ready` reflects state | 200 when reloader+writer ready; 503 otherwise. **[GAP]** New. |
| T53 | `/admin/rules` returns snapshot | JSON with id, rule_count, loaded_at, rules array. 503 when nil snapshot. **[GAP]** New. |
| T54 | `/admin/rules/reload` triggers reload | POST returns 202. Verify reload actually fires. **[GAP]** New. |
| T55 | `/admin/metrics` placeholder | GET returns 200 text/plain. **[GAP]** New. |

---

## Execution Order

Start with Layer 4 (cheapest, defines the oracle everything else depends on), then fill gaps, then build infrastructure.

| Phase | Items | Effort | Rationale |
|-------|-------|--------|-----------|
| **1. Verdict + rule contracts** | T33-T50 (L4 contracts), per-rule table tests | 2-3d | Defines "correct." Every other layer depends on this. |
| **2. Admin + config acknowledgment** | T51-T55 (admin endpoints) | 1d | Zero-coverage endpoints. Config already has tests ([EXISTS]). |
| **3. Gap coverage** | T5, T7, T8, T10, T11, T12, T16, T20, T27-T32, T50 | 2-3d | Low-effort, high-signal. Covers all identified gaps. |
| **4. Input + rule mutations** | T4, T6, T9a-T9e | 2-3d | Explores edge cases around real usage patterns. |
| **5. Chaos + state mutations** | T13-T15, T17-T19 | 3-4d | Failure injection and hot reload correctness. |
| **6. Trace infrastructure** | T1-T3 | 2-3d | Record/replay for regression detection. |
| **7. Continuous measurement** | T21-T26 (automated) | 1-2d | SQL invariant checks on cron + shadow mode CLI. T25 blocked on metrics. |

---

## Tracking

| ID | Item | Layer | Pri | Status |
|----|------|-------|-----|--------|
| T1 | Trace recorder (JSONL on resultChan) | L1 | P2 | [ ] |
| T2 | Replay harness (CLI diff tool) | L1 | P2 | [ ] |
| T3 | Jetstream trace corpus | L1 | P2 | [ ] |
| T4 | Payload boundary mutations | L2 | P1 | [ ] |
| T5 | `anyToStarlark` default case silent coercion **[GAP]** | L2 | P1 | [ ] |
| T6 | Event ordering mutations | L2 | P1 | [ ] |
| T7 | Empty `entity_id` counter cross-contamination **[GAP]** | L2 | P1 | [ ] |
| T8 | `event_id` 256-char boundary **[PARTIAL]** | L2 | P1 | [ ] |
| T9a | Invalid verdict error path | L2 | P1 | [ ] |
| T9b | Rule returns None | L2 | P1 | [ ] |
| T9c | Conflicting same-priority verdicts | L2 | P1 | [ ] |
| T9d | Rule panic recovery | L2 | P1 | [ ] |
| T9e | Rule timeout (infinite loop) | L2 | P1 | [ ] |
| T10 | `memo` UDF panic in callable **[GAP]** | L2 | P1 | [ ] |
| T11 | `regex_match` pathological pattern **[GAP]** | L2 | P2 | [ ] |
| T12 | `hash` UDF basic correctness **[GAP]** | L2 | P2 | [ ] |
| T13 | DuckDB failure injection | L2 | P1 | [ ] |
| T14 | Webhook failure injection | L2 | P1 | [ ] |
| T15 | Channel backpressure (429, no leaks) | L2 | P1 | [ ] |
| T16 | `runRetention` 30-day cleanup **[GAP]** | L2 | P1 | [ ] |
| T17 | Hot reload snapshot isolation | L2 | P1 | [ ] |
| T18 | Hot reload with bad rules | L2 | P1 | [ ] |
| T19 | Counter state across reloads + evalCache invalidation | L2 | P1 | [ ] |
| T20 | Counter concurrency stress (multi-goroutine) **[GAP]** | L2 | P1 | [ ] |
| T21 | Invariant: lossless pipeline | L3 | P1 | [ ] |
| T22 | Invariant: valid verdicts | L3 | P1 | [ ] |
| T23 | Invariant: no duplicate event_id | L3 | P1 | [ ] |
| T24 | Invariant: bounded latency | L3 | P1 | [ ] |
| T25 | Invariant: webhook parity **[BLOCKED: metrics placeholder]** | L3 | P2 | [ ] |
| T26 | Shadow mode diff tool | L3 | P2 | [ ] |
| T27 | Webhook semaphore saturation **[GAP]** | L2 | P1 | [ ] |
| T28 | Writer drain path timeout **[GAP]** | L2 | P1 | [ ] |
| T29 | Counter GC threshold **[GAP]** | L2 | P1 | [ ] |
| T30 | Nil snapshot during event processing **[GAP]** | L2 | P1 | [ ] |
| T31 | `evalCache` rule swap | L2 | P1 | [ ] |
| T32 | `regexCache` unbounded growth **[GAP]** | L2 | P2 | [ ] |
| T33 | Verdict: highest priority wins | L4 | P1 | [ ] |
| T34 | Verdict: same priority weight tiebreak | L4 | P1 | [ ] |
| T35 | Verdict: no matching rules -> approve | L4 | P1 | [ ] |
| T36 | Verdict: all rules fail -> approve | L4 | P1 | [ ] |
| T37 | interpretVerdict: non-struct return | L4 | P1 | [ ] |
| T38 | interpretVerdict: missing `type` | L4 | P1 | [ ] |
| T39 | interpretVerdict: `type` not string | L4 | P1 | [ ] |
| T40 | interpretVerdict: missing `reason` | L4 | P1 | [ ] |
| T41 | DefaultUDFs: stub counter at compile time | L4 | P1 | [ ] |
| T42 | DefaultUDFs: stub memo at compile time | L4 | P1 | [ ] |
| T43 | Contract: lossless pipeline | L4 | P1 | [ ] |
| T44 | Contract: atomic reload | L4 | P1 | [ ] |
| T45 | Contract: backpressure not loss | L4 | P1 | [ ] |
| T46 | Contract: bounded latency | L4 | P1 | [ ] |
| T47 | Contract: counter consistency | L4 | P1 | [ ] |
| T48 | Contract: idempotent writes (INSERT OR IGNORE) | L4 | P1 | [ ] |
| T49 | Contract: graceful shutdown | L4 | P1 | [ ] |
| T50 | DuckDB duplicate event_id at writer level **[GAP]** | L4 | P1 | [ ] |
| T51 | Admin: `/admin/health` **[GAP]** | L4 | P1 | [ ] |
| T52 | Admin: `/admin/ready` **[GAP]** | L4 | P1 | [ ] |
| T53 | Admin: `/admin/rules` **[GAP]** | L4 | P1 | [ ] |
| T54 | Admin: `/admin/rules/reload` **[GAP]** | L4 | P1 | [ ] |
| T55 | Admin: `/admin/metrics` **[GAP]** | L4 | P2 | [ ] |
| -- | `config` package | -- | -- | [EXISTS] 7 tests in config_test.go |
