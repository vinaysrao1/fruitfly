package executor

import (
	"fmt"
	"regexp"
	"time"

	"go.starlark.net/starlark"
)

// ruleStepBudget bounds one rule evaluation in Starlark execution steps —
// deterministic, allocation-free, and independent of machine speed. Typical
// rules use a few thousand steps; the budget is generous headroom, with the
// per-event wall-clock timeout as the backstop for time spent inside UDFs.
const ruleStepBudget = 10_000_000

// The worker implements rules.EvalEnv: stateful UDFs resolve it from the
// evaluation thread's locals.

// Counter implements rules.EvalEnv.
func (w *worker) Counter(entityID, eventType string, windowSeconds int) (int64, error) {
	if windowSeconds < 1 {
		return 0, fmt.Errorf("counter: window_seconds must be >= 1")
	}
	if int64(windowSeconds) > counterMaxWindowSeconds {
		// Counters retain counterMaxWindowSeconds of history; a larger
		// window would silently return a truncated count, which is the one
		// failure mode a rate-limiting primitive must not have.
		return 0, fmt.Errorf("counter: window_seconds must be <= %d", counterMaxWindowSeconds)
	}
	if w.pool.counterAffinity && !affineKey(entityID, w.curEntity) {
		// Cluster mode: a key other than the routing entity would scatter
		// increments across pods' private stores and silently undercount.
		// Count by another perspective via producer-side event fan-out
		// (docs/SCALING_10M.md §6).
		return 0, fmt.Errorf("counter: key %q is not affine to routing entity %q (cluster mode requires the routing entity or a \"<entity>:<scope>\" derivation)", entityID, w.curEntity)
	}
	w.pool.counters.increment(entityID, eventType, time.Now().Unix())
	return w.pool.CounterSum(entityID, eventType, windowSeconds), nil
}

// affineKey reports whether a counter key is affine to the routing entity:
// the entity itself or a scoped derivation like "user-7:likes". Affine keys
// always live on the entity's home pod, so cross-pod counts stay exact.
func affineKey(key, entity string) bool {
	return key == entity ||
		(len(key) > len(entity)+1 && key[len(entity)] == ':' && key[:len(entity)] == entity)
}

// MemoGet implements rules.EvalEnv.
func (w *worker) MemoGet(key string) (starlark.Value, bool) {
	v, ok := w.memo[key]
	return v, ok
}

// MemoSet implements rules.EvalEnv.
func (w *worker) MemoSet(key string, v starlark.Value) {
	w.memo[key] = v
}

// RegexMatch implements rules.EvalEnv.
func (w *worker) RegexMatch(pattern, text string) (bool, error) {
	re, ok := w.regexCache[pattern]
	if !ok {
		var err error
		re, err = regexp.Compile(pattern)
		if err != nil {
			return false, fmt.Errorf("invalid regex: %w", err)
		}
		w.regexCache[pattern] = re
	}
	return re.MatchString(text), nil
}
