# Fruitfly Test Confidence Report

**Status:** v1 -- 2026-02-27

## System Under Test

Starlark rules engine: HTTP events in, verdicts out (approve/block/review), persisted to DuckDB with optional webhook delivery.

```
POST /events -> Ingest -> [eventChan] -> Pool (N workers) -> [resultChan] -> Writer (DuckDB + Webhook)
                                              |
                                        atomic.Pointer[Snapshot] <- Reloader (fsnotify/poll)
```

55 test items across 4 layers. All tests pass with `go test -race -count=1 ./...`.

---

## What Passing Tests Prove

### Verdict Resolution is Correct

The verdict pipeline resolves results through a priority-then-weight system. The test suite proves:

- **Highest priority wins.** A priority-100 approve overrides a priority-50 block. (T33)
- **Same priority: weight breaks ties.** Block (weight 3) beats approve (weight 1) at the same priority level. (T34)
- **No matching rules default to approve.** An event that matches zero rules gets the safe default. (T35)
- **All rules failing defaults to approve.** If every matched rule errors out, the system falls back rather than blocking. (T36)
- **Malformed verdicts are caught.** Non-struct returns, missing `type`, non-string `type`, missing `reason` — all produce clear errors routed to `failedRules`. (T37-T40)
- **Each production rule behaves per spec at exact thresholds.** Table-driven tests for `spam_like_flood.star`, `spam_short_post.star`, `numeric_content.star`, and `catchall.star` verify boundary conditions (e.g., 5th like approves, 6th blocks).

### Pipeline is Lossless

The core invariant: every accepted event produces exactly one persisted row. The suite validates this end-to-end:

- **count(202) == count(DuckDB rows)** after graceful shutdown. (T21, T43)
- **Duplicate `event_id` produces 1 row** via INSERT OR IGNORE. The first write wins; the second is silently discarded. (T23, T48, T50)
- **Verdicts are always valid.** Every row contains a verdict in `{approve, block, review}` — never empty, never an unexpected value. (T22)
- **Graceful shutdown drains all in-flight events** before flushing the final batch to DuckDB and exiting. (T49)

### Backpressure Works Without Silent Drops

When the system is overloaded, it pushes back with HTTP 429 rather than accepting and dropping:

- **Full `eventChan` returns 429.** Rapid POSTs against a cap-1 channel produce 429s. No goroutine leaks after shutdown. (T15, T45)
- **Blocked `resultChan` propagates backpressure.** A slow writer blocks workers, which blocks ingest, which returns 429. (T15)
- **Webhook failures never block or lose DuckDB writes.** Endpoint down, slow endpoints, 500s with retries, 4xx without retries — DuckDB rows are always persisted regardless of webhook outcome. (T14, T27)
- **Webhook semaphore saturation is graceful.** When all 100 semaphore slots are full, the webhook is silently dropped with a log warning. The DuckDB insert still succeeds. (T27)

### Rules Engine is Resilient to Bad Rules

Starlark rules are user-authored and can fail in many ways. The engine handles all of them:

- **Invalid verdict type** (e.g., `verdict(type="quarantine")`) returns an error, not a panic. Rule goes to `failedRules`. (T9a)
- **Rule returning None** fails the struct type assertion and routes to `failedRules`. (T9b)
- **Go-level panics** in UDFs are caught by `evalRule`'s `defer recover()`. (T9d)
- **Infinite loops** hit the 1-second `ruleTimeout` via `thread.Cancel`. Error contains `DeadlineExceeded`. (T9e)
- **Memo UDF panics** propagate through `starlark.Call` and are caught by the same recovery path. (T10)

### Hot Reload is Atomic

Rules can be swapped at runtime via fsnotify or the admin endpoint. The suite proves:

- **Every event uses exactly one snapshot.** During a 1000-event burst with a mid-stream rule swap, no event sees a mixed rule set. (T17, T44)
- **Bad rule files are rejected.** A syntax error mid-traffic causes the reloader to keep the old snapshot. No disruption to in-flight events. (T18)
- **`evalCache` invalidates on snapshot change.** When rules A,B,C swap to A,D, stale compiled code for B is cleared and D is freshly compiled. (T31)

### Counters are Accurate Across Workers

Rate-limiting depends on `counter()` returning the true cross-worker sum:

- **`CounterSum()` is accurate under contention.** N goroutines incrementing while readers call `CounterSum` concurrently — no lost increments, no data races. Validated with `-race`. (T20, T47)
- **Counters survive hot reloads.** Counter state is per-worker, not per-snapshot. A snapshot swap does not reset counts. (T19)
- **Empty `entity_id` is a shared bucket, not cross-contamination.** An empty string becomes a distinct counter key — it doesn't pollute other entities. (T7)
- **Counter GC fires at threshold and only removes expired buckets.** After 1000 events, `gcCounters()` runs. Buckets within `counterMaxWindowSeconds` are retained; expired ones are removed. (T29)

### Edge Cases Don't Break the Engine

The executor handles adversarial and boundary inputs:

- **Payload boundaries.** Empty payloads, null fields, type coercion (`char_count="5"`), 10000-char unicode strings, deep nesting (50 levels in `anyToStarlark`). (T4)
- **`anyToStarlark` silently coerces unknown types.** Unrecognized Go types (e.g., `time.Time`) are converted via `fmt.Sprintf("%v")`. (T5)
- **`event_id` at exact 256/257-char boundary.** 256 chars accepted, 257 rejected with 400. (T8)
- **RE2 guarantees no regex hang.** Pathological patterns like `(a+)+$` with adversarial input complete in bounded time. (T11)
- **`hash` UDF correctness.** `hash("hello")` matches known SHA-256 hex. Empty string and large inputs work. (T12)
- **Stub UDFs at compile time.** Rules calling `counter()` or `memo()` at module scope (outside `evaluate()`) get `None` from `stubBuiltin` and load without error. (T41, T42)

### Writer Internals Work Correctly

The DuckDB writer has several internal mechanisms validated independently:

- **Retention cleanup.** Rows with `processed_at` older than 30 days are deleted. Rows at 29 days are retained. (T16)
- **DuckDB write failures.** Retry-once-then-drop behavior on flush failure. No panics. (T13)
- **Drain path on shutdown.** Context cancellation triggers a 5-second drain window that flushes the remaining batch. (T28)
- **Bounded latency.** All events complete within the 5-second `eventTimeout`. (T24, T46)

### Admin Endpoints Respond Correctly

- **`/admin/health`** always returns 200 `"ok"` regardless of system state. (T51)
- **`/admin/ready`** returns 200 when reloader and writer are ready, 503 otherwise. (T52)
- **`/admin/rules`** returns a JSON snapshot with `id`, `rule_count`, `loaded_at`, and `rules` array. Returns 503 when the snapshot is nil. (T53)
- **`/admin/rules/reload`** accepts POST, returns 202, and triggers an actual reload. (T54)
- **`/admin/metrics`** placeholder returns 200 `text/plain`. (T55)

---

## What Passing Tests Do NOT Prove

### Known Gaps

| Area | Gap | Reason |
|------|-----|--------|
| **Webhook parity** (T25) | `events_processed == webhook_sent + webhook_errors` is not validated | Blocked on metrics endpoint being a placeholder (`main.go:153`). Unblock when real metrics are implemented. |
| **`regexCache` eviction** (T32) | Cache grows without bound. 500 unique patterns accumulate with no eviction. | Documented risk, not a crash. No eviction policy exists in production code. Test confirms the behavior and documents the risk. |
| **Shadow mode** (T26) | CLI implemented but not tested as an integration test | `cmd/replay` is a standalone binary. Tested manually via trace corpus. |

### Structural Limitations

| Limitation | What it means |
|------------|---------------|
| **Single-process only** | All tests run in one Go process. No validation of distributed counter consistency, multi-instance DuckDB contention, or network partition behavior. |
| **Small event volumes** | Tests use tens to hundreds of events. Sustained throughput at thousands/sec, memory pressure under load, and GC pause impact are not measured. |
| **No real disk exhaustion** | T13 tests DB failure via mock/closed connection, not actual disk-full conditions on a real filesystem. |
| **No real network failures** | Webhook tests use `httptest.Server` returning errors. Real network behavior (TCP timeouts, DNS failures, partial writes) is not exercised. |
| **Admin handlers are duplicated in tests** | `integration/admin_test.go` recreates the admin mux from `main.go` rather than importing shared handlers. A divergence in `main.go` would not be caught by these tests. |
| **Time-dependent tests use tolerances** | Components call `time.Now()` directly. Tests use narrow time windows rather than injectable clocks. Flaky results are possible under heavy CI load. |

---

## Running the Tests

```bash
# Full suite with race detection (no caching)
go test -race -count=1 ./...

# By package
go test -race -count=1 ./executor/...
go test -race -count=1 ./output/...
go test -race -count=1 ./ingest/...
go test -race -count=1 ./integration/...

# Single test
go test -race -run TestContract_LosslessPipeline ./integration/...

# Replay CLI
go run ./cmd/replay --traces <file> --rules <dir>
go run ./cmd/replay --shadow --old-rules <v1> --new-rules <v2> --traces <file>
```
