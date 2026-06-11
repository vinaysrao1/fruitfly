package executor

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vinaysrao1/fruitfly/rules"
	"github.com/vinaysrao1/fruitfly/types"
)

// --- T9a: Invalid verdict type from verdictUDF ---

// T9a: Rule returns verdict(type="quarantine") -> verdictUDF returns error -> rule in FailedRules.
func TestVerdictUDF_InvalidType(t *testing.T) {
	snap := compileRule(t, `
rule_id = "invalid-verdict-rule"
event_type = "post"
priority = 100
def evaluate(event):
    return verdict(type="quarantine", reason="bad type")
`)
	pool, _ := makePool(snap, 1)
	result := runSingleEvent(t, pool, testEvent("post", nil))

	if len(result.FailedRules) != 1 {
		t.Fatalf("FailedRules len = %d, want 1", len(result.FailedRules))
	}
	if result.FailedRules[0].Err == nil {
		t.Fatal("FailedRules[0].Err is nil, expected error")
	}
	errStr := result.FailedRules[0].Err.Error()
	if !strings.Contains(errStr, "invalid") {
		t.Errorf("error = %q, want substring 'invalid'", errStr)
	}
	if len(result.TriggeredRules) != 0 {
		t.Errorf("TriggeredRules len = %d, want 0", len(result.TriggeredRules))
	}
	if result.FinalVerdict != types.VerdictApprove {
		t.Errorf("FinalVerdict = %q, want approve (default when rule errors)", result.FinalVerdict)
	}
}

// --- T5: lazyConvert default case ---

// T5: lazyConvert with unrecognized type uses fmt.Sprintf("%v") coercion.
func TestAnyToStarlark_UnrecognizedType(t *testing.T) {
	now := time.Now()
	result := lazyConvert(now)

	// The default case in lazyConvert returns starlark.String(fmt.Sprintf("%v", val))
	expected := fmt.Sprintf("%v", now)

	// Verify the result is non-nil
	if result == nil {
		t.Fatal("lazyConvert returned nil for time.Time")
	}

	// starlark.String.String() returns quoted form like `"2006-01-02 ..."`,
	// so unquote for comparison with the expected fmt.Sprintf output.
	resultStr := result.String()
	if len(resultStr) < 2 {
		t.Fatalf("result.String() too short: %q", resultStr)
	}
	unquoted := resultStr[1 : len(resultStr)-1]
	if unquoted != expected {
		t.Errorf("lazyConvert(%T) = %q, want %q", now, unquoted, expected)
	}
}

// Table-driven tests for lazyConvert recognized types.
func TestAnyToStarlark_KnownTypes(t *testing.T) {
	tests := []struct {
		name  string
		input any
		check func(t *testing.T, result interface{ String() string })
	}{
		{
			name:  "nil",
			input: nil,
			check: func(t *testing.T, r interface{ String() string }) {
				if r.String() != "None" {
					t.Errorf("got %q, want None", r.String())
				}
			},
		},
		{
			name:  "bool true",
			input: true,
			check: func(t *testing.T, r interface{ String() string }) {
				if r.String() != "True" {
					t.Errorf("got %q, want True", r.String())
				}
			},
		},
		{
			name:  "float64",
			input: float64(3.14),
			check: func(t *testing.T, r interface{ String() string }) {
				if !strings.Contains(r.String(), "3.14") {
					t.Errorf("got %q, want string containing 3.14", r.String())
				}
			},
		},
		{
			name:  "string",
			input: "hello",
			check: func(t *testing.T, r interface{ String() string }) {
				if r.String() != `"hello"` {
					t.Errorf("got %q, want \"hello\"", r.String())
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := lazyConvert(tt.input)
			tt.check(t, result)
		})
	}
}

// --- T7: Empty entity_id in counter UDF ---

// T7: counter("", event_type, window) — empty string is a valid but distinct bucket.
// Empty entity_id should not contaminate counters for "user1" entity.
func TestCounter_EmptyEntityID(t *testing.T) {
	// Rule that calls counter with entity_id from event payload.
	// We send 3 events with entity_id="" and separately check CounterSum.
	snap := compileRule(t, `
rule_id = "counter-empty-entity"
event_type = "like"
priority = 100
def evaluate(event):
    val = counter("", "like", 3600)
    return verdict("approve", reason=str(val))
`)

	var ptr atomic.Pointer[rules.Snapshot]
	ptr.Store(snap)
	pool := NewPool(1, &ptr, 5*time.Second, 1*time.Second)

	in := make(chan types.Event, 3)
	out := make(chan types.Result, 3)
	in <- testEvent("like", nil)
	in <- testEvent("like", nil)
	in <- testEvent("like", nil)
	close(in)

	pool.Run(context.Background(), in, out)
	<-out
	<-out
	<-out

	// Verify empty entity_id counter is 3.
	emptySum := pool.CounterSum("", "like", 3600)
	if emptySum != 3 {
		t.Errorf("CounterSum(\"\", \"like\", 3600) = %d, want 3", emptySum)
	}

	// Verify user1 counter is 0 (no cross-contamination).
	user1Sum := pool.CounterSum("user1", "like", 3600)
	if user1Sum != 0 {
		t.Errorf("CounterSum(\"user1\", \"like\", 3600) = %d, want 0 (no cross-contamination)", user1Sum)
	}
}

// --- T10: memo UDF panic in callable ---

// T10: Memoized callable that raises a Starlark error (division by zero).
// The error propagates through memo -> evalRule -> FailedRules.
func TestMemo_PanicInCallable(t *testing.T) {
	snap := compileRule(t, `
rule_id = "memo-panic-rule"
event_type = "post"
priority = 100
def evaluate(event):
    return memo("k", lambda: 1 / 0)
`)
	pool, _ := makePool(snap, 1)
	result := runSingleEvent(t, pool, testEvent("post", nil))

	if len(result.FailedRules) != 1 {
		t.Fatalf("FailedRules len = %d, want 1", len(result.FailedRules))
	}
	if result.FailedRules[0].Err == nil {
		t.Fatal("FailedRules[0].Err is nil, expected error")
	}
	if result.FinalVerdict != types.VerdictApprove {
		t.Errorf("FinalVerdict = %q, want approve (default when rule errors)", result.FinalVerdict)
	}

	// Verify pool continues to work after the error.
	result2 := runSingleEvent(t, pool, testEvent("post", nil))
	// Second event should also fail (same rule, same error) but pool should not hang.
	if len(result2.FailedRules) != 1 {
		t.Errorf("second event: FailedRules len = %d, want 1", len(result2.FailedRules))
	}
}

// --- T11: regex_match with pathological pattern ---

// T11: Go's regexp uses RE2 (linear time). Pathological patterns should complete quickly.
func TestRegexMatch_PathologicalPattern(t *testing.T) {
	// (a+)+$ is catastrophic for backtracking engines but RE2 handles it in linear time.
	snap := compileRule(t, `
rule_id = "regex-pathological"
event_type = "post"
priority = 100
def evaluate(event):
    # Pattern that is catastrophic for backtracking engines, safe for RE2.
    matched = regex_match("(a+)+$", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!")
    if matched:
        return verdict("block", reason="matched")
    return verdict("approve", reason="no match")
`)
	pool, _ := makePool(snap, 1)

	done := make(chan types.Result, 1)
	go func() {
		done <- runSingleEvent(t, pool, testEvent("post", nil))
	}()

	select {
	case result := <-done:
		// RE2 should not match because "!" at end breaks the pattern.
		if result.FinalVerdict != types.VerdictApprove {
			t.Errorf("FinalVerdict = %q, want approve (pattern should not match)", result.FinalVerdict)
		}
		if len(result.FailedRules) != 0 {
			t.Errorf("FailedRules len = %d, want 0 (no error expected)", len(result.FailedRules))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("test timed out after 2s -- regex_match hung (RE2 should be linear time)")
	}
}

// --- T12: hash UDF correctness ---

// T12: hash("hello") == known SHA-256 hex.
func TestHash_Correctness(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			input:    "hello",
			expected: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
		},
		{
			input:    "",
			expected: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
	}

	// Verify our expected values match Go's sha256 directly.
	for _, tt := range tests {
		sum := sha256.Sum256([]byte(tt.input))
		goExpected := fmt.Sprintf("%x", sum)
		if goExpected != tt.expected {
			t.Errorf("SHA256(%q): Go computed %q, test expected %q", tt.input, goExpected, tt.expected)
		}
	}

	// Test via pool execution.
	for _, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("input=%q", tt.input), func(t *testing.T) {
			input := tt.input
			expected := tt.expected
			snap := compileRule(t, fmt.Sprintf(`
rule_id = "hash-rule"
event_type = "post"
priority = 100
def evaluate(event):
    return verdict("approve", reason=hash(%q))
`, input))
			pool, _ := makePool(snap, 1)
			result := runSingleEvent(t, pool, testEvent("post", nil))

			if len(result.TriggeredRules) != 1 {
				t.Fatalf("TriggeredRules len = %d, want 1", len(result.TriggeredRules))
			}
			if result.TriggeredRules[0].Reason != expected {
				t.Errorf("hash(%q) = %q, want %q", input, result.TriggeredRules[0].Reason, expected)
			}
		})
	}
}

// T12 large input: hash of a large string should not error.
func TestHash_LargeInput(t *testing.T) {
	snap := compileRule(t, `
rule_id = "hash-large"
event_type = "post"
priority = 100
def evaluate(event):
    big = "a" * 100000
    h = hash(big)
    if len(h) == 64:
        return verdict("approve", reason="ok")
    return verdict("block", reason="wrong hash length")
`)
	pool, _ := makePool(snap, 1)
	result := runSingleEvent(t, pool, testEvent("post", nil))

	if result.FinalVerdict != types.VerdictApprove {
		t.Errorf("FinalVerdict = %q, want approve", result.FinalVerdict)
	}
	if len(result.FailedRules) != 0 {
		t.Errorf("FailedRules len = %d, want 0", len(result.FailedRules))
	}
}

// --- T41: DefaultUDFs stub counter at compile time ---

// T41: Rule calling counter() at module scope gets None from stubBuiltin. No compile error.
func TestDefaultUDFs_StubCounterAtCompileTime(t *testing.T) {
	c := &rules.Compiler{UDFs: rules.DefaultUDFs()}
	_, err := c.CompileSource("stub_counter_test.star", `
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

// --- T42: DefaultUDFs stub memo at compile time ---

// T42: Rule calling memo() at module scope gets None from stubBuiltin. No compile error.
func TestDefaultUDFs_StubMemoAtCompileTime(t *testing.T) {
	c := &rules.Compiler{UDFs: rules.DefaultUDFs()}
	_, err := c.CompileSource("stub_memo_test.star", `
rule_id = "stub-memo"
event_type = "post"
priority = 1
x = memo("k", lambda: 42)
def evaluate(event):
    return verdict("approve")
`)
	if err != nil {
		t.Fatalf("expected compilation to succeed with stub memo, got: %v", err)
	}
}
