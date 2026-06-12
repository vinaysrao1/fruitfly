package rules

import (
	"crypto/sha256"
	"fmt"
	"log/slog"
	"time"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// EnvLocal is the starlark.Thread local under which the evaluation
// environment is stored. UDFs that need per-evaluation state resolve it at
// call time, which lets one compiled rule (and its predeclared bindings) be
// shared by every worker.
const EnvLocal = "fruitfly.env"

// EvalEnv is the per-evaluation environment behind the stateful UDFs.
// The executor's worker implements it; compile-time initialization runs
// with no environment, so calling a stateful UDF at module scope is an
// error rather than a silent stub.
type EvalEnv interface {
	// Counter increments and reads the sliding-window counter for
	// (entityID, eventType), validating the window and (in cluster mode)
	// key affinity.
	Counter(entityID, eventType string, windowSeconds int) (int64, error)
	// MemoGet/MemoSet access the per-event memo cache.
	MemoGet(key string) (starlark.Value, bool)
	MemoSet(key string, v starlark.Value)
	// RegexMatch reports whether pattern matches text, caching compiled
	// patterns.
	RegexMatch(pattern, text string) (bool, error)
}

// DefaultUDFs returns the predeclared UDF dictionary. The builtins are
// stateless and shared: stateful ones resolve their EvalEnv from the
// calling thread, so the same dict serves compilation and every worker.
func DefaultUDFs() starlark.StringDict {
	return starlark.StringDict{
		"verdict":     starlark.NewBuiltin("verdict", verdictBuiltin),
		"memo":        starlark.NewBuiltin("memo", memoBuiltin),
		"counter":     starlark.NewBuiltin("counter", counterBuiltin),
		"now":         starlark.NewBuiltin("now", nowBuiltin),
		"log":         starlark.NewBuiltin("log", logBuiltin),
		"hash":        starlark.NewBuiltin("hash", hashBuiltin),
		"regex_match": starlark.NewBuiltin("regex_match", regexMatchBuiltin),
	}
}

// env resolves the evaluation environment from the thread, or errors when
// called outside an evaluation (e.g. at module scope during compilation).
func env(thread *starlark.Thread, udf string) (EvalEnv, error) {
	if e, ok := thread.Local(EnvLocal).(EvalEnv); ok {
		return e, nil
	}
	return nil, fmt.Errorf("%s: no evaluation context (stateful UDFs cannot be called at module scope)", udf)
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
	default:
		return nil, fmt.Errorf("verdict: invalid type %q (must be approve, block, or review)", verdictType)
	}
	return starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
		"type":   starlark.String(verdictType),
		"reason": starlark.String(reason),
	}), nil
}

func memoBuiltin(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var key string
	var fn starlark.Callable
	if err := starlark.UnpackPositionalArgs("memo", args, kwargs, 2, &key, &fn); err != nil {
		return nil, err
	}
	e, err := env(thread, "memo")
	if err != nil {
		return nil, err
	}
	if cached, ok := e.MemoGet(key); ok {
		return cached, nil
	}
	result, err := starlark.Call(thread, fn, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("memo(%q): %w", key, err)
	}
	// Freeze before caching: the value is shared by every rule evaluated
	// for this event.
	result.Freeze()
	e.MemoSet(key, result)
	return result, nil
}

func counterBuiltin(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var entityID, eventType string
	var windowSeconds int
	if err := starlark.UnpackPositionalArgs("counter", args, kwargs, 3, &entityID, &eventType, &windowSeconds); err != nil {
		return nil, err
	}
	e, err := env(thread, "counter")
	if err != nil {
		return nil, err
	}
	total, err := e.Counter(entityID, eventType, windowSeconds)
	if err != nil {
		return nil, err
	}
	return starlark.MakeInt64(total), nil
}

func nowBuiltin(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	return starlark.MakeInt64(time.Now().Unix()), nil
}

func logBuiltin(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var message string
	if err := starlark.UnpackPositionalArgs("log", args, kwargs, 1, &message); err != nil {
		return nil, err
	}
	slog.Info("rule log", "message", message)
	return starlark.None, nil
}

func hashBuiltin(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var value string
	if err := starlark.UnpackPositionalArgs("hash", args, kwargs, 1, &value); err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(value))
	return starlark.String(fmt.Sprintf("%x", sum)), nil
}

func regexMatchBuiltin(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var pattern, text string
	if err := starlark.UnpackPositionalArgs("regex_match", args, kwargs, 2, &pattern, &text); err != nil {
		return nil, err
	}
	e, err := env(thread, "regex_match")
	if err != nil {
		return nil, err
	}
	matched, err := e.RegexMatch(pattern, text)
	if err != nil {
		return nil, err
	}
	return starlark.Bool(matched), nil
}
