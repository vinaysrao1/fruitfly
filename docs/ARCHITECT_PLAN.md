# Fruitfly Test Implementation Plan -- Architect Plan

**Status:** Final | **Date:** 2026-02-27

This plan divides 55 test items across 4 parallel workstreams. Each workstream targets a distinct package boundary to minimize merge conflicts. Agents should work top-down within their workstream, following the order listed.

---

## Architecture Quick Reference

```
POST /events -> ingest.Server.handleEvent()
    -> eventChan (cap 100)
    -> executor.Pool.Run() [N workers, each a goroutine]
        worker.processEvent():
            snap := atomic.Pointer[rules.Snapshot].Load()
            snap.RulesForEvent(eventType) -> matched rules
            for each rule: worker.evalRule() -> types.RuleResult
            resolveVerdict(triggered, matchedRules) -> FinalVerdict
    -> resultChan (cap 100)
    -> output.Writer.Run()
        batch insert to DuckDB (INSERT OR IGNORE)
        async webhook via goroutine pool (semaphore cap 100)
```

Key types:
- `types.Event` -- EventID, EventType, Timestamp, Payload (map[string]any), RawJSON, ReceivedAt
- `types.Result` -- EventID, EventType, FinalVerdict, TriggeredRules, FailedRules, Payload, LatencyUS, ProcessedAt
- `types.RuleResult` -- RuleID, Verdict, Reason, Err, Elapsed
- `rules.Rule` -- RuleID, EventType, Priority, Program (*starlark.Program)
- `rules.Snapshot` -- ID, Rules []Rule, LoadedAt

Key functions under test:
- `executor.resolveVerdict(triggered []RuleResult, matchedRules []Rule) Verdict` -- unexported, in verdict.go
- `executor.interpretVerdict(v starlark.Value) (Verdict, string, error)` -- unexported, in verdict.go
- `executor.anyToStarlark(v any) starlark.Value` -- unexported, in event.go
- `executor.buildUDFs(w *worker) starlark.StringDict` -- unexported, in udfs.go
- `output.Writer.flush(batch)`, `output.Writer.insertBatch(batch)`, `output.Writer.runRetention()`, `output.Writer.sendWebhook()` -- unexported, in writer.go

---

## Workstream A: executor package tests

**Owner focus:** `executor/` package only. All new tests go in `executor/executor_test.go` (or a new file `executor/verdict_test.go` and `executor/udfs_test.go` if the file gets large).

**Existing helpers to reuse:**
- `newTestCompiler()` -- creates `rules.Compiler` with `DefaultUDFs()`
- `compileRule(t, source)` -- compiles a single starlark source into a `*rules.Snapshot`
- `compileRules(t, sources)` -- compiles multiple sources into one snapshot
- `makePool(snap, workers)` -- creates Pool + stores snapshot in atomic pointer
- `runSingleEvent(t, pool, event)` -- sends one event, runs pool, returns result
- `testEvent(eventType, payload)` -- builds a `types.Event`

### Phase 1: Verdict resolution contracts (T33-T36)

These tests exercise `resolveVerdict` and the pool's `processEvent` flow. Existing tests `TestPriorityResolution`, `TestSamePriority_TieBreaking`, `TestNoMatchingRules_DefaultApprove`, and `TestRuleError_InFailedRules` already cover T33-T36 respectively. **Verify coverage is exact, then add any missing edge cases.**

| Test ID | Test Name | Implementation |
|---------|-----------|----------------|
| T33 | `TestVerdict_HighestPriorityWins` | Already covered by `TestPriorityResolution`. Verify: priority(100) approve + priority(50) block -> approve. **EXISTS.** |
| T34 | `TestVerdict_SamePriorityWeightTiebreak` | Already covered by `TestSamePriority_TieBreaking`. Verify: priority(100) approve + priority(100) block -> block (weight 3 > 1). **EXISTS.** |
| T35 | `TestVerdict_NoMatchingRules_DefaultApprove` | Already covered by `TestNoMatchingRules_DefaultApprove`. Event type "unknown" with only "post" rules -> approve. **EXISTS.** |
| T36 | `TestVerdict_AllRulesFail_DefaultApprove` | Already covered by `TestRuleError_InFailedRules`. Division by zero -> failedRules non-empty, verdict approve. **EXISTS.** Add a multi-rule variant: two rules both fail, verify approve. |

**New code for T36 augmentation:**
```go
func TestVerdict_AllMultipleRulesFail_DefaultApprove(t *testing.T) {
    snap := compileRules(t, []struct{ filename, source string }{
        {"fail1.star", `
rule_id = "fail-1"
event_type = "post"
priority = 100
def evaluate(event):
    return 1 / 0
`},
        {"fail2.star", `
rule_id = "fail-2"
event_type = "post"
priority = 50
def evaluate(event):
    return 1 / 0
`},
    })
    pool, _ := makePool(snap, 1)
    result := runSingleEvent(t, pool, testEvent("post", nil))
    // Assert: FinalVerdict == approve, len(FailedRules) == 2, len(TriggeredRules) == 0
}
```

### Phase 2: interpretVerdict edge cases (T37-T40)

These test the unexported `interpretVerdict` function. Since it is unexported, tests must be in `executor` package. Write rules that return specific bad values from `evaluate()`, then assert the rule lands in `FailedRules` with the expected error substring.

| Test ID | Test Name | Rule Source | Assertion |
|---------|-----------|-------------|-----------|
| T37 | `TestInterpretVerdict_NonStructReturn` | `def evaluate(event): return "not a struct"` | `FailedRules[0].Err` contains `"must return a verdict()"` |
| T37b | `TestInterpretVerdict_NoneReturn` (=T9b) | `def evaluate(event): return None` | `FailedRules[0].Err` contains `"must return a verdict()"` |
| T37c | `TestInterpretVerdict_IntReturn` | `def evaluate(event): return 42` | `FailedRules[0].Err` contains `"must return a verdict()"` |
| T38 | `TestInterpretVerdict_MissingType` | Return a raw starlark struct: `def evaluate(event): return struct(reason="x")` -- but `struct` is not predeclared in starlark by default. Instead, use Starlark dict trick: can't easily construct a struct without `verdict()`. **Alternative approach:** Test `interpretVerdict` directly by calling it with a hand-built `*starlarkstruct.Struct`. |
| T39 | `TestInterpretVerdict_TypeNotString` | Build struct with `type = 42` via starlarkstruct. |
| T40 | `TestInterpretVerdict_MissingReason` | Build struct with `type = "approve"` but no `reason` attr. |

**Implementation detail for T38-T40:** Since `interpretVerdict` is unexported but in the same package, write unit tests that call it directly:

```go
func TestInterpretVerdict_MissingType(t *testing.T) {
    s := starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
        "reason": starlark.String("test"),
    })
    _, _, err := interpretVerdict(s)
    if err == nil || !strings.Contains(err.Error(), "missing 'type'") {
        t.Errorf("expected error about missing type, got: %v", err)
    }
}

func TestInterpretVerdict_TypeNotString(t *testing.T) {
    s := starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
        "type":   starlark.MakeInt(42),
        "reason": starlark.String("test"),
    })
    _, _, err := interpretVerdict(s)
    if err == nil || !strings.Contains(err.Error(), "must be string") {
        t.Errorf("expected error about type being string, got: %v", err)
    }
}

func TestInterpretVerdict_MissingReason(t *testing.T) {
    s := starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
        "type": starlark.String("approve"),
    })
    _, _, err := interpretVerdict(s)
    if err == nil || !strings.Contains(err.Error(), "missing 'reason'") {
        t.Errorf("expected error about missing reason, got: %v", err)
    }
}
```

### Phase 3: UDF tests (T5, T7, T9a, T10, T11, T12, T41, T42)

| Test ID | Test Name | Details |
|---------|-----------|---------|
| T9a | `TestVerdictUDF_InvalidType` | Rule: `return verdict(type="quarantine")`. Assert: `FailedRules[0].Err` contains `"invalid verdict type"` or `"invalid type"`. The error comes from `verdictUDF` line 39: `fmt.Errorf("verdict: invalid type %q", verdictType)`. |
| T5 | `TestAnyToStarlark_UnrecognizedType` | Call `anyToStarlark(time.Now())` directly. Assert result is `starlark.String` containing the time string representation (current behavior: `fmt.Sprintf("%v", val)`). |
| T7 | `TestCounter_EmptyEntityID` | Rule calling `counter("", "like", 600)`. Send 3 events with entity_id="" and 3 events from a different rule with entity_id="user1". Assert the empty-string counter returns 3, not 6. Verify no cross-contamination. Use `pool.CounterSum("", "like", 600)` after running events. |
| T10 | `TestMemo_PanicInCallable` | Rule: `def evaluate(event): return memo("k", lambda: 1/0)`. Assert rule lands in FailedRules. Verify pool continues (send a second event after). |
| T11 | `TestRegexMatch_PathologicalPattern` | Rule: `regex_match("(a+)+$", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!")`. Go regexp uses RE2, so this should complete in bounded time. Assert no hang (use 2s timeout on the test). |
| T12 | `TestHash_Correctness` | Rule: `return verdict("approve", reason=hash("hello"))`. Assert reason == `"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"`. Also test empty string: SHA256("") = `"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"`. |
| T41 | `TestDefaultUDFs_StubCounterAtCompileTime` | Starlark source with `x = counter("a", "b", 60)` at module scope (outside evaluate). Compile with `DefaultUDFs()`. Assert compilation succeeds. Assert `x` would be `None` (stub returns None). |
| T42 | `TestDefaultUDFs_StubMemoAtCompileTime` | Same pattern: `x = memo("k", lambda: 42)` at module scope. Compile with `DefaultUDFs()`. Assert no error. |

**Implementation for T41/T42:**
```go
func TestDefaultUDFs_StubCounterAtCompileTime(t *testing.T) {
    c := &rules.Compiler{UDFs: rules.DefaultUDFs()}
    _, err := c.CompileSource("stub_test.star", `
rule_id = "stub-counter"
event_type = "post"
priority = 1
x = counter("entity", "type", 60)
def evaluate(event):
    return verdict("approve")
`)
    if err != nil {
        t.Fatalf("expected compilation to succeed with stub counter, got: %v", err)
    }
}
```

### Phase 4: Rule mutations (T9b-T9e, T4)

| Test ID | Test Name | Details |
|---------|-----------|---------|
| T9b | `TestRuleReturnsNone` | `def evaluate(event): return None`. Assert FailedRules, error contains "must return a verdict()". (Covered in T37b above -- combine.) |
| T9c | `TestConflictingSamePriorityVerdicts` | Already covered by T34. Priority=100 approve + priority=100 block -> block wins. **EXISTS.** |
| T9d | `TestRulePanic_Recovery` | `def evaluate(event): fail("deliberate panic")`. `fail()` is a Starlark builtin that raises an error (not a Go panic). For a true Go panic, we need a UDF that panics. **Instead:** Test the `defer recover()` in `evalRule` by verifying that a rule calling a crashing UDF lands in FailedRules. Use `memo("k", lambda: 1/0)` which triggers starlark error, caught by evalRule. For a true Go-level panic test, we would need to inject a panicking UDF -- skip unless we can modify buildUDFs. **Alternative:** Verify the existing `defer recover()` works by confirming the FailedRules path with a division-by-zero rule (already tested in `TestRuleError_InFailedRules`). Mark T9d as covered by existing test + T10. |
| T9e | `TestRuleTimeout_InfiniteLoop` | Already covered by `TestRuleTimeout_DoesNotBlockPipeline`. Verify error contains "DeadlineExceeded". Existing test checks FailedRules[0].Err != nil but does not check the specific error string. **Augment:** add assertion `strings.Contains(result.FailedRules[0].Err.Error(), "DeadlineExceeded")`. |
| T4 | `TestPayloadBoundaries` | Table-driven test of `anyToStarlark` and event processing with edge-case payloads. Sub-cases: (a) empty payload `{}`, (b) null fields `{"key": null}`, (c) type coercion `{"char_count": "5"}` (string not float64), (d) extreme float64 `math.MaxFloat64`, (e) unicode `{"text": strings.Repeat("X", 10000)}`, (f) deep nesting (50 levels of `map[string]any`). For each, send through pool and verify no panic, result has a verdict. |

### Phase 5: Pool/counter state tests (T19, T20, T29, T30, T31, T32)

| Test ID | Test Name | Details |
|---------|-----------|---------|
| T20 | `TestCounterConcurrency_Stress` | Launch N goroutines calling `pool.CounterSum` while pool processes events. Use `-race` flag. 2 workers, 100 events. Assert no race, final counter sum == 100. |
| T29 | `TestCounterGC_Threshold` | Send exactly 1000 events to trigger `gcCounters()` (line 134: `if w.evtCount >= 1000`). Before sending, manually insert an expired counter bucket (bucket timestamp < now - counterMaxWindowSeconds). After 1000 events, verify expired bucket was deleted. **Implementation:** Create pool with 1 worker. Access `pool.workers[0].counters` to inject an expired key. Then send 1000 events and verify the expired key is gone. This requires accessing unexported fields -- since tests are in `executor` package, this is fine. |
| T30 | `TestNilSnapshot_DefaultApprove` | Set `atomic.Pointer[rules.Snapshot]` to nil (don't store any snapshot). Send event. Assert FinalVerdict == approve, TriggeredRules empty, FailedRules empty. |
| T31 | `TestEvalCache_RuleSwap` | Start with snapshot containing rules A, B, C. Process one event (populates evalCache). Swap to snapshot with rules A, D. Process another event. Assert evalCache was cleared (verify by checking that rule D executes correctly -- if stale cache were served, D's callable wouldn't exist). |
| T19 | `TestCounterState_AcrossReloads` | Counter increments persist across snapshot swaps (counters are per-worker, not per-snapshot). Send 3 events with rule that calls `counter()`, swap snapshot to different rules that also call `counter()`. Assert counter continues accumulating. |
| T32 | `TestRegexCache_UnboundedGrowth` | Send events triggering 10K unique regex patterns. Measure `len(worker.regexCache)` after. Assert it equals 10K (documenting the unbounded growth risk). This is a documentation test, not a bug fix. |

### File summary for Workstream A

| File | Action |
|------|--------|
| `executor/verdict_test.go` | **CREATE** -- T33-T40 (interpretVerdict + resolveVerdict unit tests) |
| `executor/udfs_test.go` | **CREATE** -- T5, T7, T9a, T10, T11, T12, T41, T42 |
| `executor/executor_test.go` | **MODIFY** -- T4, T9b-T9e, T19, T20, T29, T30, T31, T32 (augment existing tests) |

### Ordering within workstream

1. T33-T36 (verdict resolution -- mostly verify existing, quick)
2. T37-T40 (interpretVerdict -- new unit tests, no dependencies)
3. T9a, T5, T12 (simple UDF tests)
4. T7, T10, T11 (UDF edge cases)
5. T41, T42 (DefaultUDFs stubs)
6. T9b-T9e (rule mutation paths)
7. T4 (payload boundaries)
8. T20, T29, T30, T31, T19, T32 (pool/counter state -- more complex)

---

## Workstream B: output/writer package tests

**Owner focus:** `output/` package only. All new tests go in `output/writer_test.go`.

**Existing helpers to reuse:**
- `makeResult(eventID, eventType, verdict)` -- builds a `types.Result`
- `testDB(t, webhookURL)` -- creates temp DuckDB Writer, returns Writer + dbPath
- `openReadDB(t, dbPath)` -- opens fresh read connection for verification

### Phase 1: DuckDB contract tests (T50, T16)

| Test ID | Test Name | Details |
|---------|-----------|---------|
| T50 | `TestDuplicateEventID_InsertOrIgnore` | Insert result with event_id "X" (verdict=approve). Insert different result with same event_id "X" (verdict=block). Query: assert 1 row, verdict=approve (first row preserved). Uses `INSERT OR IGNORE` semantics. |
| T16 | `TestRunRetention_DeletesOldRows` | Insert rows with `processed_at` 31 days ago directly via SQL. Call `w.runRetention()`. Query: assert old rows deleted. Insert 29-day-old rows, call retention again, assert retained. **Implementation:** `runRetention()` is unexported but tests are in `output` package so callable. Insert old rows via `w.db.Exec(...)` with explicit processed_at. |

**Implementation for T50:**
```go
func TestDuplicateEventID_InsertOrIgnore(t *testing.T) {
    w, dbPath := testDB(t, "")

    in := make(chan types.Result, 2)
    r1 := makeResult("dup-id", "type1", types.VerdictApprove)
    r2 := makeResult("dup-id", "type2", types.VerdictBlock)
    in <- r1
    in <- r2
    close(in)

    if err := w.Run(context.Background(), in); err != nil {
        t.Fatalf("Run: %v", err)
    }

    db := openReadDB(t, dbPath)
    var count int
    db.QueryRow("SELECT COUNT(*) FROM results WHERE event_id = 'dup-id'").Scan(&count)
    if count != 1 {
        t.Errorf("expected 1 row for dup event_id, got %d", count)
    }
    var verdict string
    db.QueryRow("SELECT verdict FROM results WHERE event_id = 'dup-id'").Scan(&verdict)
    if verdict != "approve" {
        t.Errorf("expected first row preserved (approve), got %q", verdict)
    }
}
```

**Implementation for T16:**
```go
func TestRunRetention_DeletesExpiredRows(t *testing.T) {
    w, dbPath := testDB(t, "")

    // Insert old row (31 days ago) directly via SQL.
    oldTime := time.Now().Add(-31 * 24 * time.Hour)
    _, err := w.db.Exec(`INSERT INTO results
        (event_id, event_type, verdict, triggered_rules, failed_rules, payload, latency_us, processed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        "old-event", "test", "approve", "[]", "[]", "{}", 100, oldTime)
    if err != nil {
        t.Fatalf("insert old row: %v", err)
    }

    // Insert recent row (29 days ago).
    recentTime := time.Now().Add(-29 * 24 * time.Hour)
    _, err = w.db.Exec(`INSERT INTO results
        (event_id, event_type, verdict, triggered_rules, failed_rules, payload, latency_us, processed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        "recent-event", "test", "approve", "[]", "[]", "{}", 100, recentTime)
    if err != nil {
        t.Fatalf("insert recent row: %v", err)
    }

    w.runRetention()

    var oldCount, recentCount int
    w.db.QueryRow("SELECT COUNT(*) FROM results WHERE event_id = 'old-event'").Scan(&oldCount)
    w.db.QueryRow("SELECT COUNT(*) FROM results WHERE event_id = 'recent-event'").Scan(&recentCount)

    if oldCount != 0 {
        t.Errorf("expected old row deleted, got count=%d", oldCount)
    }
    if recentCount != 1 {
        t.Errorf("expected recent row retained, got count=%d", recentCount)
    }

    w.Close()
}
```

### Phase 2: Webhook failure tests (T14, T27)

| Test ID | Test Name | Details |
|---------|-----------|---------|
| T14 | `TestWebhook_FailureScenarios` | Table-driven: (a) endpoint down (connection refused) -- assert 3 attempts. (b) slow response (5s) -- use `httptest.Server` with `time.Sleep(6*time.Second)`, assert timeout. (c) 500 retry -- already exists as `TestWebhook_RetryOn500`, augment: assert exactly 3 attempts on persistent 500. (d) 4xx no retry -- already exists as `TestWebhook_No_RetryOn400`. (e) 429 retry -- return 429 twice then 200, assert 3 calls. (f) 3xx behavior -- return 301 (no redirect follow), assert 3 attempts (falls through retry loop). All sub-tests should verify DuckDB row still written. |
| T27 | `TestWebhookSemaphore_Saturation` | Fill `webhookSem` channel to capacity (100). Send one result through the Run loop. Assert: DuckDB insert succeeds, webhook is silently dropped. **Implementation:** Create Writer, fill `w.webhookSem` with 100 dummy values. Send 1 result through `w.Run()`. After shutdown, verify 1 DuckDB row exists. Verify no webhook call was made (use httptest server with counter). |

**Implementation for T27:**
```go
func TestWebhookSemaphore_Saturation(t *testing.T) {
    var webhookCalls atomic.Int32
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        webhookCalls.Add(1)
        w.WriteHeader(http.StatusOK)
    }))
    defer srv.Close()

    w, dbPath := testDB(t, srv.URL)

    // Fill the semaphore to capacity.
    for i := 0; i < maxConcurrentWebhooks; i++ {
        w.webhookSem <- struct{}{}
    }

    in := make(chan types.Result, 1)
    in <- makeResult("sem-test", "test", types.VerdictApprove)
    close(in)

    if err := w.Run(context.Background(), in); err != nil {
        t.Fatalf("Run: %v", err)
    }

    // Drain the semaphore so deferred operations can complete.
    for i := 0; i < maxConcurrentWebhooks; i++ {
        <-w.webhookSem
    }

    db := openReadDB(t, dbPath)
    var count int
    db.QueryRow("SELECT COUNT(*) FROM results").Scan(&count)
    if count != 1 {
        t.Errorf("expected 1 DuckDB row despite webhook drop, got %d", count)
    }
    if webhookCalls.Load() != 0 {
        t.Errorf("expected 0 webhook calls (semaphore full), got %d", webhookCalls.Load())
    }
}
```

### Phase 3: DuckDB failure and drain tests (T13, T28)

| Test ID | Test Name | Details |
|---------|-----------|---------|
| T13 | `TestDuckDB_WriteFailure_RetryOnceThenDrop` | Close the underlying `w.db` before sending results. Send 1 result. `flush()` should log error, retry once, then drop. Assert no panic. Assert Writer does not block. **Implementation challenge:** closing `w.db` may not be clean. **Alternative:** Use a DuckDB file on a read-only filesystem (tmpfs). **Simplest approach:** After `NewWriter`, close `w.db` and replace with a bad connection, then verify flush handles the error gracefully. |
| T28 | `TestWriterDrain_Timeout` | Cancel context while results are in-flight. Send 5 results, cancel context immediately. Assert the 5-second drain timeout allows flushing. Then test: send results into a channel that is never closed, cancel context, assert drain timeout (5s) fires and Writer exits. |

**Implementation for T13:**
```go
func TestDuckDB_WriteFailure_RetryThenDrop(t *testing.T) {
    w, _ := testDB(t, "")

    // Close the DB to simulate failure.
    w.db.Close()

    // Call flush directly -- it should log errors but not panic.
    batch := []types.Result{makeResult("fail-event", "test", types.VerdictApprove)}
    w.flush(batch) // Should retry once, then drop. No panic.

    // If we get here without panic, the test passes.
}
```

**Implementation for T28:**
```go
func TestWriterDrain_ContextCancel(t *testing.T) {
    w, dbPath := testDB(t, "")

    in := make(chan types.Result, 10)
    for i := 0; i < 5; i++ {
        in <- makeResult(fmt.Sprintf("drain-%d", i), "test", types.VerdictApprove)
    }

    ctx, cancel := context.WithCancel(context.Background())

    done := make(chan error, 1)
    go func() {
        done <- w.Run(ctx, in)
    }()

    // Give writer time to read some results.
    time.Sleep(100 * time.Millisecond)

    // Cancel context -- drain path should flush remaining.
    cancel()

    // Close channel after a delay to simulate in-flight results.
    go func() {
        time.Sleep(500 * time.Millisecond)
        close(in)
    }()

    select {
    case <-done:
    case <-time.After(10 * time.Second):
        t.Fatal("writer did not exit within 10s after context cancel")
    }

    db := openReadDB(t, dbPath)
    var count int
    db.QueryRow("SELECT COUNT(*) FROM results").Scan(&count)
    if count != 5 {
        t.Errorf("expected 5 rows after drain, got %d", count)
    }
}
```

### Phase 4: Webhook retry behavior (T14 sub-tests)

Implement as table-driven sub-tests within `TestWebhook_FailureScenarios`:

```go
func TestWebhook_429Retry(t *testing.T) {
    var callCount atomic.Int32
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        n := callCount.Add(1)
        if n <= 2 {
            w.WriteHeader(http.StatusTooManyRequests)
            return
        }
        w.WriteHeader(http.StatusOK)
    }))
    defer srv.Close()

    w, _ := testDB(t, srv.URL)
    w.sendWebhook(context.Background(), makeResult("retry-429", "test", types.VerdictApprove))

    if n := callCount.Load(); n != 3 {
        t.Errorf("expected 3 calls (2x429 + 1x200), got %d", n)
    }
}

func TestWebhook_PersistentFailure_3Attempts(t *testing.T) {
    var callCount atomic.Int32
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        callCount.Add(1)
        w.WriteHeader(http.StatusInternalServerError)
    }))
    defer srv.Close()

    w, _ := testDB(t, srv.URL)
    w.sendWebhook(context.Background(), makeResult("fail-all", "test", types.VerdictApprove))

    if n := callCount.Load(); n != 3 {
        t.Errorf("expected 3 attempts, got %d", n)
    }
}
```

### File summary for Workstream B

| File | Action |
|------|--------|
| `output/writer_test.go` | **MODIFY** -- Add T50, T16, T14, T27, T13, T28 |

### Ordering within workstream

1. T50 (duplicate event_id -- simple, foundational)
2. T16 (retention -- simple SQL test)
3. T14 (webhook failures -- table-driven, extends existing)
4. T27 (semaphore saturation)
5. T13 (DuckDB failure injection)
6. T28 (drain timeout path)

---

## Workstream C: ingest/admin + integration/system-level contracts

**Owner focus:** `ingest/` package tests + admin endpoint tests (in a new file) + system-level contract tests in `integration/`.

### Phase 1: Ingest boundary tests (T8)

| Test ID | Test Name | File | Details |
|---------|-----------|------|---------|
| T8 | `TestEventID_256CharBoundary` | `ingest/server_test.go` | 256-char event_id (accepted, 202). 257-char event_id (rejected, 400). Existing test uses 300 chars. This tests the exact boundary. |

**Implementation:**
```go
func TestEventID_256CharBoundary(t *testing.T) {
    srv, ch := makeServer(1, 4096)

    // 256 chars: accepted.
    id256 := strings.Repeat("a", 256)
    body256 := `{"event_id": "` + id256 + `", "event_type": "test", "timestamp": "2024-01-15T10:30:00Z"}`
    rr := post(srv, body256)
    if rr.Code != http.StatusAccepted {
        t.Errorf("256-char event_id: want 202, got %d", rr.Code)
    }
    <-ch

    // 257 chars: rejected.
    id257 := strings.Repeat("a", 257)
    body257 := `{"event_id": "` + id257 + `", "event_type": "test", "timestamp": "2024-01-15T10:30:00Z"}`
    rr2 := post(srv, body257)
    if rr2.Code != http.StatusBadRequest {
        t.Errorf("257-char event_id: want 400, got %d", rr2.Code)
    }
}
```

### Phase 2: Admin endpoint tests (T51-T55)

Create a new test file: `admin/admin_test.go` -- **but there is no admin package**. Admin handlers are defined inline in `main.go`. To test them without starting the full server, extract the handler logic or test via the integration framework.

**Recommended approach:** Create `integration/admin_test.go` that builds the same HTTP mux as `main.go` and tests each endpoint.

| Test ID | Test Name | Details |
|---------|-----------|---------|
| T51 | `TestAdminHealth_Always200` | `GET /admin/health` -> 200, body "ok". |
| T52 | `TestAdminReady_ReflectsState` | Set `reloader.Ready` and `writer.Ready` to true -> 200. Set either to false -> 503. |
| T53 | `TestAdminRules_ReturnsSnapshot` | Store a snapshot, `GET /admin/rules` -> 200 JSON with id, rule_count, loaded_at, rules array. Nil snapshot -> 503. |
| T54 | `TestAdminRulesReload_Triggers` | `POST /admin/rules/reload` -> 202. Verify `reloader.Reload()` was called (snapshot ID changes after reload). |
| T55 | `TestAdminMetrics_Placeholder` | `GET /admin/metrics` -> 200, Content-Type text/plain. |

**Implementation approach:** Build the mux in the test by recreating the handler registrations from `main.go`. This avoids needing to refactor main.go.

```go
// integration/admin_test.go
func buildAdminMux(t *testing.T, snapshotPtr *atomic.Pointer[rules.Snapshot],
    reloader *rules.Reloader, writer *output.Writer) *http.ServeMux {
    mux := http.NewServeMux()

    mux.HandleFunc("GET /admin/health", func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
        w.Write([]byte("ok"))
    })

    mux.HandleFunc("GET /admin/ready", func(w http.ResponseWriter, r *http.Request) {
        if reloader.Ready.Load() && writer.Ready.Load() {
            w.WriteHeader(http.StatusOK)
            w.Write([]byte("ok"))
        } else {
            http.Error(w, "not ready", http.StatusServiceUnavailable)
        }
    })

    // ... (same as main.go for /admin/rules, /admin/rules/reload, /admin/metrics)

    return mux
}

func TestAdminHealth_Always200(t *testing.T) {
    tp := newTestPipeline(t, approveAllRule, defaultOpts(), "")
    mux := buildAdminMux(t, tp.snapshotPtr, tp.reloader, tp.writer)
    req := httptest.NewRequest("GET", "/admin/health", nil)
    rr := httptest.NewRecorder()
    mux.ServeHTTP(rr, req)
    if rr.Code != 200 {
        t.Errorf("want 200, got %d", rr.Code)
    }
    if rr.Body.String() != "ok" {
        t.Errorf("want 'ok', got %q", rr.Body.String())
    }
}
```

### Phase 3: System-level contract tests (T43-T49)

These are integration tests that verify end-to-end contracts. Most are already partially covered by existing integration tests. Add/verify in `integration/`.

| Test ID | Test Name | File | Details |
|---------|-----------|------|---------|
| T43 | `TestContract_LosslessPipeline` | `integration/contract_test.go` | Send N events via HTTP, all get 202. Shutdown. Assert DuckDB count == N. (Already covered by `TestE2E_GracefulShutdown` and `TestBurstTraffic_200EventsIn100ms`. Add explicit contract test with 50 events.) |
| T44 | `TestContract_AtomicReload` | `integration/contract_test.go` | Send event, swap rules, send event. Verify each event used exactly one snapshot (no mixed rule sets). Check triggered_rules: event1 has rule-A, event2 has rule-B. (Already covered by `TestRulesToExecutor_SnapshotSwap`.) |
| T45 | `TestContract_BackpressureNotLoss` | `integration/contract_test.go` | Tiny channel, burst traffic. Every 202-accepted event in DuckDB, 429 for rejected. No silent drops. (Already covered by `TestE2E_Backpressure`.) |
| T46 | `TestContract_BoundedLatency` | `integration/contract_test.go` | Send 100 events. Assert all `latency_us < 5_000_000` (eventTimeout = 5s). Also assert all rule latencies < 1_000_000 (ruleTimeout = 1s). Query DuckDB: `SELECT MAX(latency_us) FROM results`. |
| T47 | `TestContract_CounterConsistency` | `integration/contract_test.go` | Send 10 events from same entity. Rule calls `counter()`. Assert counter returns true cross-worker sum. Use 2 workers. After all events, `pool.CounterSum()` should return 10. |
| T48 | `TestContract_IdempotentWrites` | `integration/contract_test.go` | Send same event_id twice. Assert 1 DuckDB row. First row's data preserved. |
| T49 | `TestContract_GracefulShutdown` | `integration/contract_test.go` | Send 10 events. Shutdown (cancel ctx, close eventChan, wait pool, wait writer). Assert 10 DuckDB rows. Assert writer exited cleanly (err == nil or db closed). (Covered by `TestE2E_GracefulShutdown`.) |

**For existing coverage, add explicit contract test wrappers** that are clearly named and assert the specific contract property. Even if the implementation is similar to existing tests, the naming makes the contract explicit.

### Phase 4: Event ordering and backpressure (T6, T15)

| Test ID | Test Name | File | Details |
|---------|-----------|------|---------|
| T6 | `TestEventOrdering_BurstAndDuplicates` | `integration/contract_test.go` | (a) Burst 100 events from one entity in 1s. Assert all processed. (b) Duplicate event_id: send same event_id twice via HTTP. Both get 202. DuckDB has 1 row (INSERT OR IGNORE). (c) Rapid type switching: alternate "post" and "like" events. Assert correct verdicts per type. |
| T15 | `TestChannelBackpressure_NoGoroutineLeaks` | `integration/contract_test.go` | eventChan cap=1. Rapid POSTs -> some 429. After shutdown, verify no goroutine leaks: capture `runtime.NumGoroutine()` before and after, assert delta < 5. |

### File summary for Workstream C

| File | Action |
|------|--------|
| `ingest/server_test.go` | **MODIFY** -- Add T8 |
| `integration/admin_test.go` | **CREATE** -- T51-T55 |
| `integration/contract_test.go` | **CREATE** -- T43-T49, T6, T15 |

### Ordering within workstream

1. T8 (ingest boundary -- simple, quick)
2. T51-T55 (admin endpoints -- standalone, no pipeline needed for T51/T55)
3. T43, T49 (lossless + shutdown -- foundational contracts)
4. T48 (idempotent writes)
5. T44, T45, T46 (atomic reload, backpressure, latency)
6. T47 (counter consistency)
7. T6, T15 (event ordering, goroutine leaks)

---

## Workstream D: rules/reloader + per-rule contracts + trace + measurement

**Owner focus:** `rules/` package tests, per-rule table-driven tests, trace infrastructure, continuous measurement.

### Phase 1: Per-rule contract tests (from Testing Plan table)

Create `executor/rules_contract_test.go` with table-driven tests for each production rule in `jetstream_test/rules/`.

**Important:** These tests exercise real production rules. Read each `.star` file and write tests matching the contract table.

| Rule | Test Cases |
|------|-----------|
| `spam_like_flood.star` (priority 100, event_type "like") | (a) entity with <= 5 likes in 600s -> approve. (b) entity with > 5 likes in 600s -> block, reason contains "spam: like flood". **Implementation:** Send 6 events with same entity_id. Events 1-5 should approve. Event 6 should block. Use 1 worker for deterministic counter behavior. |
| `spam_short_post.star` (priority 100, event_type "post") | (a) char_count >= 10 -> approve. (b) char_count < 10, entity <= 5 posts in 300s -> approve. (c) char_count < 10, entity > 5 posts in 300s -> block "spam: short post flood". |
| `numeric_content.star` (priority 90, event_type "post") | (a) text with <= 10 digits -> approve. (b) text with > 10 digits -> review "high numeric content". |
| `catchall.star` (priority 1, event_type "*") | Any event -> approve. |

**Implementation approach:** Load real `.star` files from `jetstream_test/rules/` directory. Compile them into a snapshot. Run events through the pool.

```go
func loadProductionRules(t *testing.T) *rules.Snapshot {
    t.Helper()
    c := &rules.Compiler{UDFs: rules.DefaultUDFs()}
    // Use relative path to jetstream_test/rules from the executor package.
    snap, err := c.CompileDir("../jetstream_test/rules")
    if err != nil {
        t.Fatalf("CompileDir: %v", err)
    }
    return snap
}

func TestSpamLikeFlood_ThresholdBehavior(t *testing.T) {
    snap := loadProductionRules(t)
    pool, _ := makePool(snap, 1)

    // Send 6 like events from same entity.
    in := make(chan types.Event, 6)
    out := make(chan types.Result, 6)
    for i := 0; i < 6; i++ {
        in <- types.Event{
            EventID:   fmt.Sprintf("like-%d", i),
            EventType: "like",
            Timestamp: time.Now(),
            Payload:   map[string]any{"entity_id": "user-1"},
            ReceivedAt: time.Now(),
        }
    }
    close(in)
    pool.Run(context.Background(), in, out)

    var results []types.Result
    for r := range out {
        results = append(results, r)
    }

    // First 5: approve. 6th: block.
    for i, r := range results {
        if i < 5 {
            if r.FinalVerdict != types.VerdictApprove {
                t.Errorf("event %d: expected approve, got %s", i, r.FinalVerdict)
            }
        } else {
            if r.FinalVerdict != types.VerdictBlock {
                t.Errorf("event %d: expected block, got %s", i, r.FinalVerdict)
            }
            // Check reason in triggered rules.
            found := false
            for _, rr := range r.TriggeredRules {
                if strings.Contains(rr.Reason, "spam: like flood") {
                    found = true
                }
            }
            if !found {
                t.Errorf("event %d: expected reason containing 'spam: like flood'", i)
            }
        }
    }
}
```

**NOTE:** The `counter()` UDF increments AND queries on each call. For the like flood rule, `counter(entity_id, "like", 600)` returns the count INCLUDING the current event. So:
- Event 1: counter returns 1 (not > 5) -> approve
- Event 5: counter returns 5 (not > 5) -> approve
- Event 6: counter returns 6 (> 5) -> block

With 1 worker, all events go to the same worker, so counter accumulation is deterministic.

### Phase 2: Reloader tests (T17, T18)

| Test ID | Test Name | File | Details |
|---------|-----------|------|---------|
| T17 | `TestHotReload_UnderTraffic_SnapshotIsolation` | `integration/contract_test.go` or `rules/rules_test.go` | Send 1000 events, swap rules at event 500. Verify each event used exactly one snapshot. **Implementation:** Use integration pipeline. Capture snapshot ID per result via webhook body or by inspecting the triggered rule_ids. Before swap: rule "approve-all", after swap: rule "block-all". Assert no result has mixed triggered rules. |
| T18 | `TestHotReload_BadRules_PreservesOldSnapshot` | Already covered by `TestReloader_BadReload_PreservesOldSnapshot` in `rules/rules_test.go` and `TestRuleCompileError_DuringReload` in integration tests. **EXISTS.** Verify existing test matches T18 spec exactly. |

**NOTE on T17:** This is better placed in `integration/` since it needs the full pipeline. Workstream C handles integration tests, but T17 is specifically about the reloader + pool interaction. Coordinate: Workstream D writes the test in a new file `integration/reload_test.go` or adds to `integration/chaos_test.go`. Since workstream C creates `integration/contract_test.go`, workstream D should use a different file to avoid conflicts.

**Recommended:** Workstream D creates `integration/reload_test.go` for T17.

### Phase 3: Continuous measurement tests (T21-T24)

These are SQL-based invariant checks run against a DuckDB populated by integration tests. They verify properties of the data after a test run.

| Test ID | Test Name | Details |
|---------|-----------|---------|
| T21 | `TestInvariant_LosslessPipeline` | Run 100 events through pipeline. Assert `COUNT(*) FROM results` == 100. (Overlaps with T43 contract.) |
| T22 | `TestInvariant_ValidVerdicts` | After running events: `SELECT COUNT(*) FROM results WHERE verdict NOT IN ('approve','block','review')` == 0. And `SELECT COUNT(*) FROM results WHERE verdict = '' OR verdict IS NULL` == 0. |
| T23 | `TestInvariant_NoDuplicates` | `SELECT event_id, COUNT(*) FROM results GROUP BY event_id HAVING COUNT(*) > 1` returns 0 rows. |
| T24 | `TestInvariant_BoundedLatency` | `SELECT COUNT(*) FROM results WHERE latency_us >= 5000000` == 0. |

**Implementation:** These can piggyback on the integration pipeline setup. Create `integration/invariant_test.go`:

```go
func TestInvariants_AfterLoad(t *testing.T) {
    opts := defaultOpts()
    tp := newTestPipeline(t, approveAllRule, opts, "")
    cancel, poolDone, writerDone := tp.start(t)

    // Send 100 events.
    handler := tp.ingestServer.Handler()
    for i := 0; i < 100; i++ {
        postEvent(t, handler, makeEvent("test"))
    }

    // Wait for processing.
    time.Sleep(2 * time.Second)

    tp.shutdown(t, cancel, poolDone, writerDone)

    db, _ := sql.Open("duckdb", tp.dbPath)
    defer db.Close()

    t.Run("T21_Lossless", func(t *testing.T) {
        var count int
        db.QueryRow("SELECT COUNT(*) FROM results").Scan(&count)
        if count != 100 {
            t.Errorf("expected 100 rows, got %d", count)
        }
    })

    t.Run("T22_ValidVerdicts", func(t *testing.T) {
        var bad int
        db.QueryRow("SELECT COUNT(*) FROM results WHERE verdict NOT IN ('approve','block','review')").Scan(&bad)
        if bad != 0 {
            t.Errorf("found %d rows with invalid verdict", bad)
        }
    })

    t.Run("T23_NoDuplicates", func(t *testing.T) {
        var dups int
        db.QueryRow("SELECT COUNT(*) FROM (SELECT event_id FROM results GROUP BY event_id HAVING COUNT(*) > 1)").Scan(&dups)
        if dups != 0 {
            t.Errorf("found %d duplicate event_ids", dups)
        }
    })

    t.Run("T24_BoundedLatency", func(t *testing.T) {
        var slow int
        db.QueryRow("SELECT COUNT(*) FROM results WHERE latency_us >= 5000000").Scan(&slow)
        if slow != 0 {
            t.Errorf("found %d rows with latency >= 5s", slow)
        }
    })
}
```

### Phase 4: Trace infrastructure (T1-T3) and measurement tooling (T25, T26)

These are **new feature implementations**, not just tests. They require new code.

| Test ID | Description | Effort |
|---------|-------------|--------|
| T1 | Trace recorder: tap `resultChan`, write JSONL `{event, result, snapshot_id, timestamp}`. Config flag `trace_enabled`. | New file `output/trace.go` + tests |
| T2 | Replay harness: `fruitfly replay --traces <file> --rules <dir>` CLI | New command, separate binary or subcommand |
| T3 | Trace corpus from jetstream test data | Test data generation |
| T25 | Webhook parity invariant -- **BLOCKED** on metrics endpoint | Skip for now |
| T26 | Shadow mode diff tool | New CLI tool |

**T1 Implementation Plan:**
1. Create `output/trace.go` with a `TraceRecorder` struct:
   ```go
   type TraceRecorder struct {
       file     *os.File
       encoder  *json.Encoder
       enabled  bool
   }

   type TraceEntry struct {
       Event      types.Event  `json:"event"`
       Result     types.Result `json:"result"`
       SnapshotID string       `json:"snapshot_id"`
       Timestamp  time.Time    `json:"timestamp"`
   }

   func (tr *TraceRecorder) Record(event types.Event, result types.Result, snapshotID string)
   ```
2. Add `trace_enabled` and `trace_path` to `config.Config`.
3. Wire into `main.go`: if trace_enabled, wrap resultChan with a tee that writes to TraceRecorder.
4. Tests in `output/trace_test.go`: write 10 results, read back JSONL, verify all 10 entries present with correct fields.

**T2 Implementation Plan:**
1. Create `cmd/replay/main.go` with flag parsing: `--traces <file> --rules <dir>`.
2. Read JSONL trace file, compile rules from dir, replay each event, diff verdicts.
3. Output: count of changed verdicts, breakdown by rule, list of changed event_ids.
4. Tests: create a trace file, compile two rule sets, verify diff output.

**T3:** Record traces from existing jetstream integration test runs. Store as `jetstream_test/traces/corpus.jsonl`.

**T25:** Blocked on metrics. Skip. Add a TODO comment.

**T26:** Build as part of the replay CLI: `fruitfly shadow --old-rules <v1> --new-rules <v2> --traces <file>`. This is essentially T2 run twice and diffed.

### File summary for Workstream D

| File | Action |
|------|--------|
| `executor/rules_contract_test.go` | **CREATE** -- Per-rule table tests (spam_like_flood, spam_short_post, numeric_content, catchall) |
| `integration/reload_test.go` | **CREATE** -- T17 (hot reload under traffic) |
| `integration/invariant_test.go` | **CREATE** -- T21-T24 (SQL invariant checks) |
| `output/trace.go` | **CREATE** -- T1 (trace recorder implementation) |
| `output/trace_test.go` | **CREATE** -- T1 tests |
| `config/config.go` | **MODIFY** -- Add TraceEnabled, TracePath fields |
| `main.go` | **MODIFY** -- Wire trace recorder |
| `cmd/replay/main.go` | **CREATE** -- T2, T26 (replay + shadow mode) |

### Ordering within workstream

1. Per-rule contract tests (spam_like_flood, spam_short_post, numeric_content, catchall) -- no new code, just tests
2. T17 (hot reload under traffic -- integration test)
3. T21-T24 (invariant tests -- SQL queries, straightforward)
4. T1 (trace recorder -- new code + tests)
5. T2 (replay harness -- new code)
6. T3 (trace corpus -- data generation)
7. T26 (shadow mode -- extension of T2)

---

## Cross-Workstream Dependencies

```
Workstream A (executor)     -- no dependencies on other workstreams
Workstream B (output)       -- no dependencies on other workstreams
Workstream C (ingest/admin) -- uses integration helpers from integration_test.go (already exists)
Workstream D (rules/traces) -- per-rule tests use executor helpers (same package)
                            -- T17 uses integration helpers
                            -- T1-T3 add new config fields (minor conflict with main.go)
```

**Conflict zones:**
- `main.go` -- only Workstream D modifies it (for trace wiring). No conflict.
- `config/config.go` -- only Workstream D modifies it. No conflict.
- `integration/` -- Workstreams C and D both create files here. **Use different filenames** (C: `admin_test.go`, `contract_test.go`; D: `reload_test.go`, `invariant_test.go`).
- `executor/` -- Workstreams A and D both create files here. **Use different filenames** (A: `verdict_test.go`, `udfs_test.go`; D: `rules_contract_test.go`).

---

## Test ID to Workstream Mapping

| ID | Workstream | Status |
|----|-----------|--------|
| T1 | D | New code + test |
| T2 | D | New code + test |
| T3 | D | Data generation |
| T4 | A | New test |
| T5 | A | New test |
| T6 | C | New test |
| T7 | A | New test |
| T8 | C | Augment existing |
| T9a | A | New test |
| T9b | A | New test (combined with T37) |
| T9c | A | EXISTS (T34) |
| T9d | A | Covered by existing + T10 |
| T9e | A | Augment existing |
| T10 | A | New test |
| T11 | A | New test |
| T12 | A | New test |
| T13 | B | New test |
| T14 | B | Augment + new sub-tests |
| T15 | C | New test |
| T16 | B | New test |
| T17 | D | New test |
| T18 | D | EXISTS |
| T19 | A | New test |
| T20 | A | New test |
| T21 | D | New test |
| T22 | D | New test |
| T23 | D | New test |
| T24 | D | New test |
| T25 | -- | BLOCKED |
| T26 | D | New code + test |
| T27 | B | New test |
| T28 | B | New test |
| T29 | A | New test |
| T30 | A | New test |
| T31 | A | New test |
| T32 | A | New test |
| T33 | A | EXISTS |
| T34 | A | EXISTS |
| T35 | A | EXISTS |
| T36 | A | EXISTS + augment |
| T37 | A | New test |
| T38 | A | New test |
| T39 | A | New test |
| T40 | A | New test |
| T41 | A | New test |
| T42 | A | New test |
| T43 | C | New test (extends existing) |
| T44 | C | EXISTS (TestRulesToExecutor_SnapshotSwap) |
| T45 | C | EXISTS (TestE2E_Backpressure) |
| T46 | C | New test |
| T47 | C | New test |
| T48 | C | New test |
| T49 | C | EXISTS (TestE2E_GracefulShutdown) |
| T50 | B | New test |
| T51 | C | New test |
| T52 | C | New test |
| T53 | C | New test |
| T54 | C | New test |
| T55 | C | New test |

---

## Running Tests

All workstreams should verify their tests pass independently:

```bash
# Workstream A
go test -v -race ./executor/...

# Workstream B
go test -v -race ./output/...

# Workstream C
go test -v -race ./ingest/...
go test -v -race -run "TestAdmin|TestContract|TestEvent|TestChannel" ./integration/...

# Workstream D
go test -v -race -run "TestSpam|TestNumeric|TestCatchall|TestHotReload|TestInvariant" ./executor/... ./integration/...

# Full suite
go test -v -race ./...
```

Use `-race` on all runs. Use `-count=1` to disable test caching during development.
