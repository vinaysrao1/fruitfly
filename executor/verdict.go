package executor

import (
	"fmt"

	"github.com/vinaysrao1/fruitfly/types"
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// resolveVerdict implements priority-then-weight verdict resolution: among
// the highest-priority triggered rules, the heaviest verdict wins. One pass,
// no allocation — results carry their rule's priority.
func resolveVerdict(triggered []types.RuleResult) types.Verdict {
	if len(triggered) == 0 {
		return types.VerdictApprove
	}
	best, bestPriority := triggered[0].Verdict, triggered[0].Priority
	for _, rr := range triggered[1:] {
		if rr.Priority > bestPriority ||
			(rr.Priority == bestPriority && types.VerdictWeight(rr.Verdict) > types.VerdictWeight(best)) {
			best, bestPriority = rr.Verdict, rr.Priority
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
