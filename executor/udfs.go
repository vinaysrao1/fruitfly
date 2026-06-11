package executor

import (
	"crypto/sha256"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// buildUDFs constructs the predeclared dict for a single rule execution.
func buildUDFs(w *worker) starlark.StringDict {
	return starlark.StringDict{
		"verdict":     starlark.NewBuiltin("verdict", verdictUDF),
		"memo":        starlark.NewBuiltin("memo", memoUDF(w)),
		"counter":     starlark.NewBuiltin("counter", counterUDF(w)),
		"now":         starlark.NewBuiltin("now", nowUDF),
		"log":         starlark.NewBuiltin("log", logUDF),
		"hash":        starlark.NewBuiltin("hash", hashUDF),
		"regex_match": starlark.NewBuiltin("regex_match", regexMatchUDF(w)),
	}
}

func verdictUDF(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
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
		return nil, fmt.Errorf("verdict: invalid type %q", verdictType)
	}
	return starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
		"type":   starlark.String(verdictType),
		"reason": starlark.String(reason),
	}), nil
}

func memoUDF(w *worker) func(*starlark.Thread, *starlark.Builtin, starlark.Tuple, []starlark.Tuple) (starlark.Value, error) {
	return func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var key string
		var fn starlark.Callable
		if err := starlark.UnpackPositionalArgs("memo", args, kwargs, 2, &key, &fn); err != nil {
			return nil, err
		}
		if cached, ok := w.memo[key]; ok {
			return cached.(starlark.Value), nil
		}
		result, err := starlark.Call(thread, fn, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("memo(%q): %w", key, err)
		}
		// Freeze before caching: the value is shared by every rule
		// evaluated for this event.
		result.Freeze()
		w.memo[key] = result
		return result, nil
	}
}

func counterUDF(w *worker) func(*starlark.Thread, *starlark.Builtin, starlark.Tuple, []starlark.Tuple) (starlark.Value, error) {
	return func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var entityID, eventType string
		var windowSeconds int
		if err := starlark.UnpackPositionalArgs("counter", args, kwargs, 3, &entityID, &eventType, &windowSeconds); err != nil {
			return nil, err
		}
		if windowSeconds < 1 {
			return nil, fmt.Errorf("counter: window_seconds must be >= 1")
		}
		if int64(windowSeconds) > counterMaxWindowSeconds {
			// Counters retain counterMaxWindowSeconds of history; a larger
			// window would silently return a truncated count, which is the
			// one failure mode a rate-limiting primitive must not have.
			return nil, fmt.Errorf("counter: window_seconds must be <= %d", counterMaxWindowSeconds)
		}
		if w.pool.counterAffinity && entityID != w.curEntity {
			// Cluster mode: a key other than the routing entity would
			// scatter increments across pods' private stores and silently
			// undercount. Count by another perspective via producer-side
			// event fan-out (docs/SCALING_10M.md §6).
			return nil, fmt.Errorf("counter: key %q is not affine to routing entity %q (cluster mode requires affine counter keys)", entityID, w.curEntity)
		}
		now := time.Now().Unix()
		w.pool.counters.increment(entityID, eventType, now)
		total := w.pool.CounterSum(entityID, eventType, windowSeconds)
		return starlark.MakeInt64(total), nil
	}
}

func nowUDF(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	return starlark.MakeInt64(time.Now().Unix()), nil
}

func logUDF(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var message string
	if err := starlark.UnpackPositionalArgs("log", args, kwargs, 1, &message); err != nil {
		return nil, err
	}
	slog.Info("rule log", "message", message)
	return starlark.None, nil
}

func hashUDF(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var value string
	if err := starlark.UnpackPositionalArgs("hash", args, kwargs, 1, &value); err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(value))
	return starlark.String(fmt.Sprintf("%x", sum)), nil
}

func regexMatchUDF(w *worker) func(*starlark.Thread, *starlark.Builtin, starlark.Tuple, []starlark.Tuple) (starlark.Value, error) {
	return func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var pattern, text string
		if err := starlark.UnpackPositionalArgs("regex_match", args, kwargs, 2, &pattern, &text); err != nil {
			return nil, err
		}
		re, ok := w.regexCache[pattern]
		if !ok {
			var err error
			re, err = regexp.Compile(pattern)
			if err != nil {
				return starlark.None, fmt.Errorf("invalid regex: %w", err)
			}
			w.regexCache[pattern] = re
		}
		return starlark.Bool(re.MatchString(text)), nil
	}
}
