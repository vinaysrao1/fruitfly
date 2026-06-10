package executor

import (
	"strings"
	"testing"

	"github.com/vinaysrao1/fruitfly/rules"
	"github.com/vinaysrao1/fruitfly/types"
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// --- T33-T36: resolveVerdict contracts ---

// T33: Highest priority wins (approve at 100 beats block at 50).
// Already covered by TestPriorityResolution in executor_test.go.
// This test exercises resolveVerdict directly for clarity.
func TestResolveVerdict_HighestPriorityWins(t *testing.T) {
	matchedRules := []rules.Rule{
		{RuleID: "high", EventType: "post", Priority: 100},
		{RuleID: "low", EventType: "post", Priority: 50},
	}
	triggered := []types.RuleResult{
		{RuleID: "high", Verdict: types.VerdictApprove},
		{RuleID: "low", Verdict: types.VerdictBlock},
	}
	got := resolveVerdict(triggered, matchedRules)
	if got != types.VerdictApprove {
		t.Errorf("resolveVerdict = %q, want approve (highest priority wins)", got)
	}
}

// T34: Same priority, weight breaks tie — block(3) > approve(1).
// Already covered by TestSamePriority_TieBreaking in executor_test.go.
// Direct unit test for documentation.
func TestResolveVerdict_SamePriorityWeightTiebreak(t *testing.T) {
	matchedRules := []rules.Rule{
		{RuleID: "r-approve", EventType: "post", Priority: 100},
		{RuleID: "r-block", EventType: "post", Priority: 100},
	}
	triggered := []types.RuleResult{
		{RuleID: "r-approve", Verdict: types.VerdictApprove},
		{RuleID: "r-block", Verdict: types.VerdictBlock},
	}
	got := resolveVerdict(triggered, matchedRules)
	if got != types.VerdictBlock {
		t.Errorf("resolveVerdict = %q, want block (block weight > approve weight at same priority)", got)
	}
}

// T35: No triggered rules -> default approve.
func TestResolveVerdict_NoTriggeredRules_DefaultApprove(t *testing.T) {
	got := resolveVerdict(nil, []rules.Rule{{RuleID: "r", EventType: "post", Priority: 100}})
	if got != types.VerdictApprove {
		t.Errorf("resolveVerdict = %q, want approve (no triggered rules)", got)
	}
}

// T36: All rules fail -> default approve (triggered is empty).
func TestResolveVerdict_AllRulesFail_DefaultApprove(t *testing.T) {
	// triggered is empty because failed rules don't go into triggered
	matchedRules := []rules.Rule{
		{RuleID: "fail-1", EventType: "post", Priority: 100},
		{RuleID: "fail-2", EventType: "post", Priority: 50},
	}
	got := resolveVerdict([]types.RuleResult{}, matchedRules)
	if got != types.VerdictApprove {
		t.Errorf("resolveVerdict = %q, want approve (all rules failed)", got)
	}
}

// T36 augmentation: via pool — two rules both fail, verify approve and failedRules non-empty.
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

	if result.FinalVerdict != types.VerdictApprove {
		t.Errorf("FinalVerdict = %q, want approve", result.FinalVerdict)
	}
	if len(result.FailedRules) != 2 {
		t.Errorf("FailedRules len = %d, want 2", len(result.FailedRules))
	}
	if len(result.TriggeredRules) != 0 {
		t.Errorf("TriggeredRules len = %d, want 0", len(result.TriggeredRules))
	}
}

// --- T37-T40: interpretVerdict edge cases ---

// T37: Non-struct return -> error "must return a verdict()".
func TestInterpretVerdict_NonStructReturn(t *testing.T) {
	_, _, err := interpretVerdict(starlark.String("not a struct"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "must return a verdict()") {
		t.Errorf("error = %q, want substring 'must return a verdict()'", err.Error())
	}
}

// T37b: None return -> error "must return a verdict()".
func TestInterpretVerdict_NoneReturn(t *testing.T) {
	_, _, err := interpretVerdict(starlark.None)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "must return a verdict()") {
		t.Errorf("error = %q, want substring 'must return a verdict()'", err.Error())
	}
}

// T37c: Int return -> error "must return a verdict()".
func TestInterpretVerdict_IntReturn(t *testing.T) {
	_, _, err := interpretVerdict(starlark.MakeInt(42))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "must return a verdict()") {
		t.Errorf("error = %q, want substring 'must return a verdict()'", err.Error())
	}
}

// T38: Struct missing 'type' attribute.
func TestInterpretVerdict_MissingType(t *testing.T) {
	s := starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
		"reason": starlark.String("test"),
	})
	_, _, err := interpretVerdict(s)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "missing 'type'") {
		t.Errorf("error = %q, want substring \"missing 'type'\"", err.Error())
	}
}

// T39: 'type' attribute is not a string.
func TestInterpretVerdict_TypeNotString(t *testing.T) {
	s := starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
		"type":   starlark.MakeInt(42),
		"reason": starlark.String("test"),
	})
	_, _, err := interpretVerdict(s)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "must be string") {
		t.Errorf("error = %q, want substring 'must be string'", err.Error())
	}
}

// T40: Struct with valid 'type' but missing 'reason'.
func TestInterpretVerdict_MissingReason(t *testing.T) {
	s := starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
		"type": starlark.String("approve"),
	})
	_, _, err := interpretVerdict(s)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "missing 'reason'") {
		t.Errorf("error = %q, want substring \"missing 'reason'\"", err.Error())
	}
}

// Valid struct: all fields present and correct.
func TestInterpretVerdict_ValidStruct(t *testing.T) {
	s := starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
		"type":   starlark.String("block"),
		"reason": starlark.String("spam detected"),
	})
	verdict, reason, err := interpretVerdict(s)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if verdict != types.VerdictBlock {
		t.Errorf("verdict = %q, want block", verdict)
	}
	if reason != "spam detected" {
		t.Errorf("reason = %q, want 'spam detected'", reason)
	}
}

// Invalid verdict type in struct (e.g. "quarantine").
func TestInterpretVerdict_InvalidVerdictType(t *testing.T) {
	s := starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
		"type":   starlark.String("quarantine"),
		"reason": starlark.String("test"),
	})
	_, _, err := interpretVerdict(s)
	if err == nil {
		t.Fatal("expected error for invalid verdict type, got nil")
	}
	if !strings.Contains(err.Error(), "invalid verdict type") {
		t.Errorf("error = %q, want substring 'invalid verdict type'", err.Error())
	}
}
