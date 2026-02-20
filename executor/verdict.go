package executor

import (
	"fmt"

	"github.com/vinaysrao1/fruitfly/rules"
	"github.com/vinaysrao1/fruitfly/types"
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// resolveVerdict implements priority-then-weight verdict resolution.
func resolveVerdict(triggered []types.RuleResult, matchedRules []rules.Rule) types.Verdict {
	if len(triggered) == 0 {
		return types.VerdictApprove
	}

	priorityOf := make(map[string]int, len(matchedRules))
	for _, r := range matchedRules {
		priorityOf[r.RuleID] = r.Priority
	}

	maxPriority := priorityOf[triggered[0].RuleID]
	for _, rr := range triggered[1:] {
		if p := priorityOf[rr.RuleID]; p > maxPriority {
			maxPriority = p
		}
	}

	best := types.VerdictApprove
	bestWeight := 0
	for _, rr := range triggered {
		if priorityOf[rr.RuleID] != maxPriority {
			continue
		}
		w := types.VerdictWeight(rr.Verdict)
		if w > bestWeight {
			bestWeight = w
			best = rr.Verdict
		}
	}

	return best
}

// interpretVerdict extracts verdict and reason from a Starlark struct returned by evaluate().
func interpretVerdict(v starlark.Value) (types.Verdict, string, error) {
	s, ok := v.(*starlarkstruct.Struct)
	if !ok {
		return "", "", fmt.Errorf("evaluate() must return a verdict(), got %s", v.Type())
	}

	typeVal, err := s.Attr("type")
	if err != nil {
		return "", "", fmt.Errorf("verdict struct missing 'type': %w", err)
	}
	typeStr, ok := typeVal.(starlark.String)
	if !ok {
		return "", "", fmt.Errorf("verdict.type must be string, got %s", typeVal.Type())
	}

	reasonVal, err := s.Attr("reason")
	if err != nil {
		return "", "", fmt.Errorf("verdict struct missing 'reason': %w", err)
	}
	reasonStr, ok := reasonVal.(starlark.String)
	if !ok {
		return "", "", fmt.Errorf("verdict.reason must be string, got %s", reasonVal.Type())
	}

	verdict := types.Verdict(string(typeStr))
	switch verdict {
	case types.VerdictApprove, types.VerdictBlock, types.VerdictReview:
	default:
		return "", "", fmt.Errorf("invalid verdict type %q", string(typeStr))
	}

	return verdict, string(reasonStr), nil
}
