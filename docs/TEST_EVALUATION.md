# Fruitfly Test Suite Evaluation

**How the tests evaluate the codebase for correctness, scale, invariance, and robustness.**

**Branch:** TESTING | **Date:** 2026-03-11

---

## 1. System Under Test

Fruitfly is a Starlark rules engine that evaluates JSON events via HTTP, produces verdicts (approve/block/review), persists results to DuckDB, and optionally delivers them via webhook. The architecture is a three-stage pipeline:

```
POST /events -> Ingest (HTTP validation)
             -> [eventChan, cap=100]
             -> Pool (N workers, Starlark eval)
             -> [resultChan, cap=100]
             -> Writer (DuckDB batch + Webhook)
                    |
              atomic.Pointer[Snapshot] <- Reloader (fsnotify + poll)
```

**Packages under test:** `config/`, `types/`, `ingest/`, `rules/`, `executor/`, `output/`, `integration/`, and production Starlark rules in `jetstream_test/`.

---

## 2. Test Architecture

The test suite follows a four-layer strategy defined in `docs/TESTING.md`:

| Layer | Purpose | Implementation Status |
|-------|---------|----------------------|
| **L1: Record and Replay** | Capture traffic as traces, replay to detect regressions | Partial -- trace recorder and corpus generator implemented; replay CLI exists but lacks its own tests |
| **L2: Mutation and Chaos** | Perturb inputs, rules, and environment to find edge-case failures | Thoroughly implemented across unit and integration tests |
| **L3: Continuous Measurement** | Monitor behavioral invariants post-test | Implemented via `integration/invariant_test.go`; webhook parity (T25) blocked on metrics |
| **L4: Specification as Oracle** | Define correctness as executable contracts | Fully implemented across verdict, rule, and system-level contract tests |

The suite contains **20 test files** (including `integration/integration_test.go` as shared test infrastructure) with approximately **140 test functions** (including sub-tests and table-driven cases).

---

## 3. Correctness

Correctness tests verify that the system produces the expected output for known inputs. These form the largest category (~55 test functions).

### 3.1 Verdict Resolution

The verdict resolution algorithm (priority-then-weight) is tested at three levels:

**Unit level** (`executor/verdict_test.go`, 13 tests):
- `TestResolveVerdict_HighestPriorityWins`: A priority-100 approve rule beats a priority-50 block rule, confirming priority takes precedence over verdict severity.
- `TestResolveVerdict_SamePriorityWeightTiebreak`: When two rules share a priority, verdict weight breaks the tie (block=3 > review=2 > approve=1).
- `TestResolveVerdict_NoTriggeredRules_DefaultApprove`: No triggered rules produce a default approve verdict.
- `TestResolveVerdict_AllRulesFail_DefaultApprove`: When all rules error during evaluation, the system defaults to approve rather than failing closed.
- `TestVerdict_AllMultipleRulesFail_DefaultApprove`: Tests via pool: two rules both fail with division by zero, final verdict is approve, 2 failed rules reported, 0 triggered rules.
- `TestInterpretVerdict_ValidStruct`: A correctly-formed Starlark verdict struct is parsed into the correct Go Verdict with type and reason.

**Component level** (`executor/executor_test.go`):
- `TestSingleRule_CorrectVerdict`: A single block rule produces a result with the correct verdict, reason, and rule ID.
- `TestPriorityResolution`: Confirms priority resolution through the full executor path (compile rules, create pool, process event).
- `TestSamePriority_TieBreaking`: Confirms weight-based tiebreaking through the full executor path.
- `TestMemo_CalledOnce`: Two rules sharing the same memo key call the memoized function only once per event. Both rules see the same counter value.
- `TestCounterSum_CrossWorker`: Two workers processing events increment the same counter key. `CounterSum` returns the correct cross-worker total.
- `TestOutputChannelClosed_AfterAllWorkers`: After all input events are processed, the output channel is closed, allowing the consumer to drain via `range`.
- `TestCounterGC_Threshold`: Directly exercises `gcCounters()` to verify that expired counter buckets are deleted while fresh buckets are retained.

**Integration level** (`integration/contract_test.go`):
- `TestContract_AtomicReload` (T44): Sends an event under rule v1 (approve), hot-reloads to rule v2 (block), sends another event. Verifies each event was evaluated against exactly one snapshot.

### 3.2 Per-Rule Contracts

`executor/rules_contract_test.go` (11 tests) validates each production Starlark rule against its specification:

- **`spam_like_flood.star`**: Events 1-5 from a single entity approve; event 6 blocks with reason containing "spam: like flood". Different entities do not cross-contaminate counters (`TestSpamLikeFlood_DifferentEntities` sends 6 events alternating between 2 entities -- neither reaches the threshold).
- **`spam_short_post.star`**: Posts with `char_count >= 10` always approve. Short posts (char_count < 10) approve up to 5 within the counter window; the 6th blocks.
- **`numeric_content.star`**: Posts with > 10 digit characters in the text field trigger a review verdict; <= 10 digits approve. A priority interaction test (`TestNumericContent_WithProductionRules_PriorityInteraction`) documents that `spam_short_post` at priority 100 overrides `numeric_content` at priority 90 when both match.
- **`catchall.star`**: The wildcard rule (priority 1) approves all event types including empty strings.
- **`TestProductionRules_TableDriven`**: A comprehensive table-driven test evaluates the full production rule set against multiple input scenarios, asserting expected verdicts and reasons.

### 3.3 Pipeline Correctness

**Ingest** (`ingest/server_test.go`):
- `TestValidEvent_ProducesCorrectEvent`: A valid POST produces an Event with the correct `event_id`, `event_type`, `timestamp`, and `payload` (including `Payload["user_id"]`), with `ReceivedAt` within a tolerance window of the current time.
- `TestMissingEventID_AutoGenerates`: When `event_id` is omitted, the server generates a UUIDv7 and returns it in the 202 response body.

**Output** (`output/writer_test.go`):
- `TestWriteAndQuery_SingleResult`: A single result inserted into DuckDB produces a row with correct `verdict`, `event_type`, and `latency_us`.
- `TestBatchFlush_OnSize`: 100 results trigger a batch flush by size threshold.
- `TestBatchFlush_OnTimer`: 10 results (below the batch size threshold) are flushed by the 500ms timer.
- `TestFinalFlush_OnChannelClose`: 5 results are flushed when the result channel closes (shutdown path).
- `TestWebhook_SuccessfulDelivery`: The webhook endpoint receives a JSON body with the correct `EventID` field.

**Trace** (`output/trace_test.go`, 7 tests):
- `TestTraceRecorder_Disabled`: A disabled recorder is a complete no-op -- `Record` does not create any file.
- `TestTraceRecorder_Enabled_WritesEntries`: 10 trace entries are written and read back with correct field values.
- `TestTraceRecorder_AllFields`: All `TraceEntry` fields are serialized and deserialized correctly using a nested structure: `Event.EventID`, `Event.EventType`, `Result.FinalVerdict`, `Result.TriggeredRules[0].RuleID`, and `SnapshotID`.
- `TestTraceRecorder_Append`: Two recording sessions append to the same JSONL file, producing 10 total entries.
- `TestTraceRecorder_InvalidPath`: An invalid file path returns an error from `NewTraceRecorder`.
- `TestTraceRecorder_CloseDisabled`: Calling `Close` on a disabled recorder is safe and returns no error.

**Trace corpus generation** (`output/trace_corpus_test.go`):
- `TestGenerateTraceCorpus` (T3): Generates a JSONL trace corpus from realistic traffic patterns (normal posts, short posts, like floods, spam bursts, numeric content, mixed traffic) using the production rules. The corpus is written to `jetstream_test/traces/corpus.jsonl` for use with the replay CLI.

**End-to-end** (`integration/e2e_test.go`):
- `TestE2E_HappyPath`: POST a spam event -> webhook receives a block verdict -> DuckDB contains 1 row with the correct event_id and verdict.

**Admin endpoints** (`integration/admin_test.go`, 5 tests):
- `TestAdminHealth_Always200`: GET `/admin/health` returns 200 with body "ok".
- `TestAdminReady_ReflectsState`: Returns 200 when both reloader and writer report ready; returns 503 when either reports not ready.
- `TestAdminRules_ReturnsSnapshot`: Returns JSON with `id`, `rule_count`, and `rules` array fields; returns 503 when the snapshot pointer is nil.
- `TestAdminRulesReload_Triggers`: POST to `/admin/rules/reload` triggers a reload; the snapshot ID changes afterward.
- `TestAdminMetrics_Placeholder`: GET `/admin/metrics` returns 200 with `Content-Type: text/plain` and a non-empty body. This is a placeholder endpoint -- the webhook parity invariant (T25) is blocked until real metrics are implemented here.

### 3.4 Configuration

`config/config_test.go` (7 tests):
- `TestLoad_Defaults`: Loading with an empty path returns all default values (port 8080, `runtime.NumCPU()` workers, rules_dir "./rules", duckdb_path "fruitfly.duckdb", log_level "info").
- `TestLoad_ValidYAML`: All YAML fields override their defaults.
- `TestLoad_WorkersZero`: `workers: 0` falls back to `runtime.NumCPU()`.
- `TestLoad_PartialYAML`: Setting only one field leaves all others at defaults.

### 3.5 UDF Correctness

`executor/udfs_test.go`:
- `TestAnyToStarlark_KnownTypes`: nil -> `starlark.None`, bool -> `starlark.Bool`, float64 -> `starlark.Float`, string -> `starlark.String`.
- `TestHash_Correctness`: SHA-256 of "hello" matches the known hex digest `2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824`. SHA-256 of "" matches `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
- `TestDefaultUDFs_StubCounterAtCompileTime`: Calling `counter()` at module scope (outside `evaluate()`) returns `None` without a compile error, because the default UDFs provide stub implementations during compilation.
- `TestDefaultUDFs_StubMemoAtCompileTime`: Same behavior for `memo()` at module scope.

### 3.6 Type System

`types/types_test.go`:
- `TestVerdictWeight_Ordering`: Validates the verdict weight ordering: block(3) > review(2) > approve(1) > unknown(0). This ordering is foundational to the same-priority tiebreaking logic.

---

## 4. Scale

Scale tests evaluate throughput, burst absorption, memory bounds, and concurrent performance. There are approximately 8 dedicated scale tests.

### 4.1 Throughput

`integration/scalability_test.go`:
- **`TestThroughput_100EventsPerSecond`**: Sends 1000 events at a sustained rate of 100 events/second. Asserts:
  - At least 900 of 1000 events are accepted (202 responses) -- allows up to 10% backpressure under load.
  - `accepted count == DuckDB row count` (lossless invariant).
  - P50 latency < 50ms.
  - P99 latency < 200ms.
  - Heap growth < 50MB over the test duration (no memory leak).
- **`TestThroughput_BurstAbsorption`**: Sends 500 events as fast as possible (no rate limiting). Asserts:
  - At least 100 events accepted (the channel buffer absorbs the initial burst, then backpressure kicks in).
  - The pipeline drains all accepted events within 5 seconds.
  - `accepted count == DuckDB row count`.

### 4.2 Burst Traffic

`integration/chaos_test.go`:
- **`TestBurstTraffic_200EventsIn100ms`**: Sends 200 events within 100ms. Asserts at least 100 are accepted and `accepted == DuckDB rows`. This tests the system's ability to absorb traffic spikes without data loss.

`integration/contract_test.go`:
- **`TestEventOrdering_BurstAndDuplicates`** (T6): Sends a 100-event burst, duplicate event_ids, and rapid event-type switching. Verifies the pipeline handles ordering chaos without panics or data corruption.
- **`TestChannelBackpressure_NoGoroutineLeaks`** (T15): Uses a channel capacity of 1 with 50 rapid POSTs. After shutdown, asserts the goroutine count delta is < 5, confirming no goroutine leaks under backpressure.

### 4.3 Concurrent Stress

`executor/executor_test.go`:
- **`TestCounterConcurrency_Stress`**: Launches 8 writer goroutines (each incrementing a counter 100 times) and 4 reader goroutines (each calling `CounterSum` in a loop until writers finish) concurrently. Designed to be run with `-race` to detect data races in `sync.Map` + `atomic.Int64` usage. After completion, verifies `CounterSum` equals 800 (no lost increments).

`output/trace_test.go`:
- **`TestTraceRecorder_ConcurrentWrites`**: 10 goroutines each write 10 trace entries concurrently. Verifies all 100 entries are present in the output file (no lost writes, no corruption).

### 4.4 Scale Documentation Tests

`executor/executor_test.go`:
- **`TestRegexCache_UnboundedGrowth`** (T32): Documents that the `regexCache` has no eviction policy. The test sends events triggering unique regex patterns and observes memory growth. This is a documentation test that flags a known risk rather than asserting a bound.

`executor/udfs_test.go`:
- **`TestHash_LargeInput`**: Hashes a 100,000-character input string. Verifies the output is a 64-character hex string, confirming SHA-256 handles large inputs without issues.

---

## 5. Invariance

Invariance tests verify properties that must hold regardless of input, timing, or system state. There are approximately 20 dedicated invariance tests.

### 5.1 Pipeline Invariants

`integration/invariant_test.go` (3 tests):
- **`TestInvariants_AfterLoad`** (T21-T24): Sends 100 events through the full pipeline, then asserts four invariants:
  1. **Lossless**: `count(202 responses) == count(DuckDB rows)`. Every accepted event produces exactly one database row.
  2. **Valid verdicts**: Every verdict in DuckDB is one of `{approve, block, review}`. No empty or unknown verdicts.
  3. **No duplicates**: `SELECT event_id FROM results GROUP BY event_id HAVING COUNT(*) > 1` returns 0 rows.
  4. **Bounded latency**: All `latency_us` values are below 5,000,000 (the event timeout of 5 seconds).
- **`TestInvariant_Lossless_WithMixedVerdicts`**: Same lossless and no-duplicate invariants tested with mixed event types (posts, likes, follows) that trigger different rules and produce different verdicts.
- **`TestInvariant_NoDuplicates_InsertOrIgnore`**: Sends the same `event_id` twice. Verifies DuckDB contains exactly 1 row (INSERT OR IGNORE deduplication).

### 5.2 System-Level Contracts

`integration/contract_test.go` (7 tests below, plus 2 tests described in Section 4.2: `TestEventOrdering_BurstAndDuplicates` and `TestChannelBackpressure_NoGoroutineLeaks`; 9 test functions total):
- **`TestContract_LosslessPipeline`** (T43): `accepted == DuckDB rows` after graceful shutdown.
- **`TestContract_AtomicReload`** (T44): Each event is evaluated against exactly one snapshot -- no partial or mixed rule evaluation.
- **`TestContract_BackpressureNotLoss`** (T45): Under overload, the system returns 429 (backpressure) but never silently drops an accepted event. `accepted == DuckDB rows` always holds.
- **`TestContract_BoundedLatency`** (T46): All `latency_us` values in DuckDB are below 5,000,000 microseconds.
- **`TestContract_CounterConsistency`** (T47): With 2 workers processing 10 events, `CounterSum` returns 10 -- the true cross-worker sum, not a per-worker undercount.
- **`TestContract_IdempotentWrites`** (T48): Duplicate `event_id` produces 1 DuckDB row. The first row's data is preserved (INSERT OR IGNORE semantics).
- **`TestContract_GracefulShutdown`** (T49): 20 events sent, then SIGTERM-equivalent shutdown. All 20 events are present in DuckDB.

### 5.3 Verdict Invariants

`executor/verdict_test.go`:
- **`TestResolveVerdict_NoTriggeredRules_DefaultApprove`**: The system always produces a verdict, even when no rules match. Default is approve.
- **`TestResolveVerdict_AllRulesFail_DefaultApprove`**: When all rules fail with errors, the default verdict is approve (fail-open policy).

`executor/executor_test.go`:
- **`TestNoMatchingRules_DefaultApprove`**: An event type with no matching rules receives an approve verdict with empty `TriggeredRules` and empty `FailedRules`.

### 5.4 State Isolation Invariants

`executor/executor_test.go`:
- **`TestMemo_IsolationBetweenEvents`**: The memo cache is cleared between events. A memo'd value from event 1 does not leak into event 2.
- **`TestCounterState_AcrossReloads`** (T19): Counters persist across snapshot swaps because they are stored per-worker (in `sync.Map`), not per-snapshot. The test increments a counter, swaps the snapshot, increments again, and verifies the sum reflects both increments.

`executor/rules_contract_test.go`:
- **`TestSpamLikeFlood_DifferentEntities`**: 6 events alternating between 2 entities (3 each). Neither entity reaches the flood threshold of 5, confirming counters are keyed by entity_id with no cross-contamination.

### 5.5 Hot Reload Invariants

`integration/reload_test.go` (2 tests):
- **`TestHotReload_UnderTraffic_SnapshotIsolation`** (T17): Sends 1000 events, swaps rules at event 500. Verifies no event is evaluated against a "mixed" rule set -- each event uses exactly one snapshot.
- **`TestHotReload_BadRules_PreservesOldSnapshot`** (T18): Replaces a rule file with invalid Starlark mid-traffic. The reloader rejects the bad rules and preserves the old snapshot. All events continue to be evaluated correctly.

`rules/rules_test.go` (9 tests total):
- **`TestCompileSource_ValidRule`**: A valid Starlark rule file compiles to a Rule with the correct RuleID, EventType, Priority, and a non-nil Program.
- **`TestCompileDir_SortedByPriority`**: Rules compiled from a directory are sorted by priority in descending order.
- **`TestRulesForEvent_FilterAndWildcard`**: `RulesForEvent` returns rules matching the given event type plus wildcard (`*`) rules, excluding non-matching types.
- **`TestReloader_FileChange_UpdatesSnapshot`**: Modifying a rule file and triggering a reload produces a new snapshot with the updated rule priority.
- **`TestReloader_BadReload_PreservesOldSnapshot`**: Unit-level confirmation that a syntax error during reload preserves the previous snapshot.

### 5.6 Deduplication Invariant

`output/writer_test.go`:
- **`TestDuplicateEventID_InsertOrIgnore`**: Inserts two results with the same `event_id` but different verdicts. Verifies 1 row exists and the first row's verdict is preserved (INSERT OR IGNORE keeps the first insert).

---

## 6. Robustness

Robustness tests evaluate edge cases, error handling, failure injection, and recovery. This is the second-largest category (~45 test functions).

### 6.1 Input Validation

`ingest/server_test.go` (8 robustness tests):
- **`TestInvalidJSON_Returns400`**: Tests 4 sub-cases: empty body, non-JSON body (`"not json"`), malformed JSON (`{"event_type": }`), and JSON array (`[1,2,3]`). All return 400.
- **`TestMissingRequiredFields_Returns400`**: Tests 7 sub-cases: (1) missing `event_type`, (2) missing `timestamp`, (3) `event_type` is integer, (4) `event_type` is empty string, (5) `event_type` is boolean, (6) `timestamp` is invalid RFC3339 string, (7) `timestamp` is integer. All return 400.
- **`TestOversizedPayload_Returns413`**: A body exceeding `maxBytes` returns 413 (Payload Too Large).
- **`TestContentType_NonJSON_Returns415`**: A `text/plain` Content-Type returns 415 (Unsupported Media Type).
- **`TestContentType_Missing_Accepted`**: A missing Content-Type header is accepted (202) -- the server does not require the header.
- **`TestEventID_TooLong_Returns400`**: A 300-character `event_id` is rejected with 400.
- **`TestEventID_256CharBoundary`** (T8): Exact boundary testing -- 256 characters accepted, 257 characters rejected.
- **`TestChannelFull_Returns429`**: When `eventChan` is full, the server returns 429 (Too Many Requests) with a "backpressure" message rather than blocking or dropping the event.

### 6.2 Payload Edge Cases

`executor/executor_test.go`:
- **`TestPayloadBoundaries`** (T4): Table-driven test with 9 sub-cases:
  - Empty payload `{}` -- processes without error.
  - Null field (`"x": null`) -- converts to `starlark.None`.
  - String-to-number coercion (`"char_count": "5"`) -- remains a string in Starlark (no implicit coercion).
  - Extreme float (`1e308`) -- handled without overflow.
  - Unicode string (10,000 repeated characters) -- processed correctly.
  - Deep nesting (50 levels of nested maps) -- `anyToStarlark` handles recursive conversion.
  - JSON array in payload -- converted to Starlark list.
  - Boolean value -- converted to `starlark.Bool`.
  - int64 payload value (`42`) -- handled correctly.

### 6.3 Rule Failure Modes

`executor/executor_test.go`:
- **`TestRuleError_InFailedRules`**: A rule with a division by zero error goes to `FailedRules` (not `TriggeredRules`), and the final verdict defaults to approve.
- **`TestRuleReturnsNone`** (T9b): A rule whose `evaluate()` returns `None` goes to `FailedRules` with an error about expecting a verdict struct.
- **`TestRulePanic_RecoveryViaPanickingUDF`** (T9d): A Go-level panic in a UDF is caught by `defer/recover` in `evalRule`. The rule goes to `FailedRules`, and the worker pool continues processing subsequent events.
- **`TestRulePanic_StarlarkErrorViaPipeline`**: A Starlark-level error (e.g., calling `memo` with a callable that divides by zero) reaches `FailedRules`, and the pool continues.
- **`TestRuleTimeout_DoesNotBlockPipeline`** (T9e): A rule with an effectively infinite loop (`for i in range(1000000000)`) times out at 100ms (test uses a shorter timeout than production's 1s). Starlark does not support `while` loops by default, so a large `range()` is used instead. The timeout error contains "deadline exceeded", and the pipeline continues processing the next event.
- **`TestRuleTimeout_ErrorContainsDeadlineExceeded`**: Specifically asserts the error message format for timed-out rules.

`executor/verdict_test.go` (6 robustness tests for `interpretVerdict`):
- Non-struct return (string) -> error "must return a verdict()".
- None return -> same error.
- Integer return -> same error.
- Struct missing `type` attribute -> error "missing 'type'".
- `type` attribute is an integer -> error "must be string".
- Missing `reason` attribute -> error "missing 'reason'".
- Invalid verdict type ("quarantine") -> error "invalid verdict type".

`executor/udfs_test.go`:
- **`TestVerdictUDF_InvalidType`**: Calling `verdict(type="quarantine")` returns an error (not a panic), and the rule goes to `FailedRules`.
- **`TestAnyToStarlark_UnrecognizedType`** (T5): An unrecognized Go type (`time.Time`) is silently coerced via `fmt.Sprintf("%v")`. The test documents this behavior.
- **`TestCounter_EmptyEntityID`** (T7): An empty `entity_id` string is valid and creates its own counter bucket with no cross-contamination with other entities.
- **`TestMemo_PanicInCallable`** (T10): A memoized callable that divides by zero causes the rule to go to `FailedRules`, but the worker pool continues.
- **`TestRegexMatch_PathologicalPattern`** (T11): A pathological regex pattern like `(a+)+$` with adversarial input runs in linear time because Go's `regexp` package uses RE2 (guaranteed linear-time matching). The test verifies no hang occurs.

### 6.4 Rule Compilation Failures

`rules/rules_test.go`:
- **`TestCompileSource_SyntaxError`**: A Starlark file with a syntax error returns an error that includes the filename.
- **`TestCompileSource_MissingEvaluate`**: A Starlark file without an `evaluate` function returns an error.
- **`TestCompileDir_DuplicateRuleID_Error`**: Two rules with the same `rule_id` in metadata return a duplicate error.
- **`TestCompileDir_EmptyDir`**: An empty rules directory produces an empty snapshot (not an error).

### 6.5 Webhook Failure Modes

`output/writer_test.go` (8 robustness tests):
- **`TestWebhook_RetryOn500`**: A 500 response triggers a retry. The second attempt returns 200. Exactly 2 calls are made.
- **`TestWebhook_No_RetryOn400`**: A 400 response is not retried. Exactly 1 call is made.
- **`TestWebhook_EndpointDown`**: Connection refused on the webhook endpoint. DuckDB write still succeeds (webhook failure does not block persistence).
- **`TestWebhook_SlowEndpoint_Timeout`**: A slow webhook endpoint (exceeds timeout). DuckDB write still succeeds.
- **`TestWebhook_Persistent500_3Attempts`**: All attempts return 500. Exactly 3 attempts are made (initial + 2 retries), then the webhook is abandoned.
- **`TestWebhook_4xx_NoRetry`**: 403, 404, and 422 responses each trigger exactly 1 call (no retry on client errors).
- **`TestWebhook_429Retry`**: A 429 response triggers retries (treated as transient). Two 429s followed by a 200 result in 3 total calls.
- **`TestWebhook_3xx_Retries`**: A 301 with no redirect-following retries 3 times (the response code falls through the retry loop since only `<300` exits successfully).

### 6.6 Writer Failure Modes

`output/writer_test.go`:
- **`TestWebhookSemaphore_Saturation`** (T27): When the webhook semaphore (capacity 100) is full, the webhook delivery is silently dropped with a log warning. DuckDB insert still succeeds.
- **`TestDuckDB_WriteFailure_RetryThenDrop`** (T13): A closed database causes a write failure. The writer retries once, then drops the batch without panicking.
- **`TestWriterDrain_ContextCancel`** (T28): Context cancellation triggers the drain path. All items in the channel are flushed to DuckDB before exit.
- **`TestWriterDrain_TimeoutExpiry`**: A never-closed channel with a drain timeout eventually exits (the 5-second drain timeout fires).
- **`TestRunRetention_DeletesExpiredRows`** (T16): Rows with `processed_at` 31 days ago are deleted by `runRetention()`. Rows 29 days old are retained. This tests the 30-day retention policy boundary.

### 6.7 Nil/Edge State

`executor/executor_test.go`:
- **`TestNilSnapshot_DefaultApprove`** (T30): When the `atomic.Pointer[Snapshot]` is nil (e.g., before rules are loaded), the pool returns a default approve verdict rather than panicking.
- **`TestEvalCache_RuleSwap`** (T31): After a snapshot swap, the `evalCache` is cleared. New rules are freshly compiled and executed, with no stale callables served from the previous snapshot.

### 6.8 Integration-Level Chaos

`integration/chaos_test.go` (5 tests):
- **`TestWebhookDown_DuckDBStillWrites`**: Webhook endpoint refuses connections. 5 events are sent. All 5 are present in DuckDB (persistence is independent of webhook delivery).
- **`TestMalformedEvents_Rejected`**: 9 malformed event variants (empty body, non-JSON, JSON array, event_type as integer, event_type as null, missing event_type, missing timestamp, invalid timestamp string, timestamp as integer) are all rejected with 400. A valid event sent afterward succeeds (the server recovers from malformed input).
- **`TestRuleCompileError_DuringReload`**: Invalid Starlark is written to the rules directory mid-traffic. The reloader rejects the bad rules, preserves the old snapshot, and the pipeline continues processing events.
- **`TestRuleTimeout_DoesNotBlockPipeline`**: An infinite-loop rule times out. The pipeline continues, and subsequent events are processed and persisted to DuckDB. Note: this test exists at both the integration level (here in `chaos_test.go`, using the full HTTP pipeline with a 1s rule timeout) and at the component level (in `executor/executor_test.go`, using a 100ms rule timeout). The integration-level test verifies end-to-end behavior including DuckDB persistence; the component-level test verifies the executor's timeout and error handling in isolation.

### 6.9 Graceful Shutdown

`integration/e2e_test.go`:
- **`TestE2E_GracefulShutdown`**: 10 events are sent, then the pipeline is shut down. All 10 events are present in DuckDB, confirming the drain path flushes in-flight events.

`integration/e2e_test.go`:
- **`TestE2E_Backpressure`**: Uses tiny channel buffers (capacity 1) with 20 rapid POSTs. At least one 429 is returned (backpressure detected), and `accepted == DuckDB rows` (no data loss despite backpressure).

---

## 7. Cross-Cutting Concerns

### 7.1 Module Boundary Tests

`integration/module_test.go` (3 tests) validates the contracts between pipeline stages:
- **`TestIngestToExecutor_EventContract`** (T33): POST -> executor -> correct verdict and event_id in DuckDB. Tests the ingest-to-executor boundary.
- **`TestExecutorToOutput_ResultContract`** (T34): Event pushed directly to `eventChan` -> correct DuckDB row and webhook delivery. Tests the executor-to-output boundary.
- **`TestRulesToExecutor_SnapshotSwap`** (T35): Hot-swaps rules from approve to block. Subsequent events receive the new verdict. Tests the rules-to-executor boundary.

### 7.2 Race Condition Coverage

The test suite is designed to be run with Go's race detector (`-race`). Key concurrency-sensitive tests:
- `TestCounterConcurrency_Stress`: 12 goroutines concurrently reading and writing counters.
- `TestTraceRecorder_ConcurrentWrites`: 10 goroutines writing to the trace recorder.
- `TestHotReload_UnderTraffic_SnapshotIsolation`: 1000 events with a mid-stream snapshot swap.
- `TestChannelBackpressure_NoGoroutineLeaks`: 50 rapid POSTs with capacity-1 channels.

### 7.3 Deterministic Time

The testing plan acknowledges that several components call `time.Now()` directly (counters, `now` UDF, `processedAt`). Tests that depend on time use tolerance windows (e.g., `ReceivedAt` within 1 second of test start). The plan notes this is an accepted trade-off for simplicity, with a refactor to an injectable clock only if flaky tests emerge.

---

## 8. Coverage Gaps

The following areas have limited or no test coverage:

| Gap | Severity | Details |
|-----|----------|---------|
| **Webhook parity invariant (T25)** | Medium | Cannot verify `events_processed == webhook_sent + webhook_errors` because the metrics endpoint is a placeholder. Blocked until real metrics are implemented. |
| **Replay CLI** | Medium | `cmd/replay/` exists but has no automated tests. The trace recorder and corpus generator are tested, but the replay/shadow diffing logic is not. |
| **`main.go` function** | Low | The admin handler logic is duplicated between `main.go` and `integration/admin_test.go` (which builds its own `buildAdminMux`). The test file warns about this duplication. The actual `main()` function is tested indirectly through integration tests. |
| **Webhook concurrency under sustained load** | Low | `TestWebhookSemaphore_Saturation` pre-fills the semaphore, but does not exercise the `maxConcurrentWebhooks=100` limit under sustained traffic with slow webhooks. |
| **DuckDB disk full** | Low | Only closed-DB failure is tested (`TestDuckDB_WriteFailure_RetryThenDrop`). No test simulates disk-full conditions. |
| **Counter window boundary precision** | Low | Tests verify counters accumulate and GC works, but do not test the exact window boundary (events at time T with window W, queried at T+W+1). |
| **`jetstream_test/` harness programs** | Low | The `shim/`, `receiver/`, and `validate/` Go programs have no Go tests. These are infrastructure for generating test data, not production code. |

---

## 9. Summary

The Fruitfly test suite evaluates the codebase through a disciplined four-layer strategy:

- **Correctness** (~55 tests): Every component has unit tests validating expected behavior. Verdict resolution is tested at unit, component, and integration levels. Per-rule contracts verify production Starlark rules against their specifications. Pipeline correctness is validated end-to-end.

- **Scale** (~8 tests): Throughput is measured at 100 events/second sustained and 500-event bursts. Latency percentiles (P50 < 50ms, P99 < 200ms) and memory bounds (< 50MB heap growth) are asserted. Counter concurrency is stress-tested with 12 concurrent goroutines.

- **Invariance** (~20 tests): Four pipeline invariants (lossless, valid verdicts, no duplicates, bounded latency) are tested directly. Seven system-level contracts formalize these as executable specifications. State isolation (memo, counters, snapshots) and deduplication (INSERT OR IGNORE) invariants are verified.

- **Robustness** (~45 tests): Input validation covers malformed JSON, missing fields, oversized payloads, and boundary values. Rule failure modes cover errors, panics, timeouts, invalid verdicts, and None returns. Webhook and DuckDB failures are injected to verify the system degrades gracefully. Nil state, cache invalidation, and graceful shutdown are tested.

The testing plan (`docs/TESTING.md`) serves as a living specification. Of the 55 planned test items (T1-T55), nearly all have been implemented. The primary gaps are the replay CLI tests, the webhook parity invariant (blocked on metrics), and some edge cases around DuckDB disk-full and counter window boundaries.
