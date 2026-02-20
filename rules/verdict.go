package rules

import (
	"fmt"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// DefaultUDFs returns the predeclared UDF dictionary for compilation.
func DefaultUDFs() starlark.StringDict {
	return starlark.StringDict{
		"verdict":     starlark.NewBuiltin("verdict", verdictBuiltin),
		"memo":        starlark.NewBuiltin("memo", stubBuiltin),
		"counter":     starlark.NewBuiltin("counter", stubBuiltin),
		"now":         starlark.NewBuiltin("now", stubBuiltin),
		"log":         starlark.NewBuiltin("log", stubBuiltin),
		"hash":        starlark.NewBuiltin("hash", stubBuiltin),
		"regex_match": starlark.NewBuiltin("regex_match", stubBuiltin),
	}
}

// verdictBuiltin implements verdict(type, reason="").
func verdictBuiltin(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var verdictType string
	var reason string
	if err := starlark.UnpackArgs("verdict", args, kwargs,
		"type", &verdictType,
		"reason?", &reason,
	); err != nil {
		return nil, err
	}

	switch verdictType {
	case "approve", "block", "review":
		// valid
	default:
		return nil, fmt.Errorf("verdict: invalid type %q (must be approve, block, or review)", verdictType)
	}

	return starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
		"type":   starlark.String(verdictType),
		"reason": starlark.String(reason),
	}), nil
}

// stubBuiltin is a no-op placeholder for UDFs not yet implemented.
func stubBuiltin(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	return starlark.None, nil
}
