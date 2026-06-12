package rules

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

// compileStepBudget bounds module-scope execution during compilation, the
// same way rule evaluation is step-bounded at runtime.
const compileStepBudget = 10_000_000

// Rule is a compiled Starlark rule ready for execution.
type Rule struct {
	RuleID    string
	EventType string // "*" matches all
	Priority  int
	Program   *starlark.Program
	// Evaluate is the rule's evaluate() callable, initialized once at
	// compile time with frozen globals. Frozen values are safe for
	// concurrent calls, so every worker shares this one callable —
	// stateful UDFs resolve their per-worker environment via thread
	// locals (EnvLocal).
	Evaluate starlark.Callable
	// Match holds the lowered tier-1 prefilter clauses (nil = always run).
	Match []Predicate
	// Prefiltered counts events skipped by the prefilter; a pointer so all
	// copies of this Rule share one counter, surfaced via /admin/rules so
	// an over-aggressive match block is visible rather than silent.
	Prefiltered *atomic.Int64
	// Stats tracks evaluation timing (EWMA) for slow-rule detection;
	// shared by all copies of this Rule, like Prefiltered.
	Stats *RuleStats
}

// Snapshot is an immutable collection of compiled rules, sorted by priority desc.
type Snapshot struct {
	ID       string
	Rules    []Rule
	LoadedAt time.Time

	// byType maps each known event type to its matching rules (type-specific
	// plus wildcard, in priority order). wildcard holds rules with
	// event_type "*", returned for event types not present in byType.
	byType   map[string][]Rule
	wildcard []Rule
}

// RulesForEvent returns rules matching the given event type (including wildcard "*").
func (s *Snapshot) RulesForEvent(eventType string) []Rule {
	if s.byType != nil {
		if matched, ok := s.byType[eventType]; ok {
			return matched
		}
		return s.wildcard
	}
	// Fallback for snapshots constructed without buildIndex (e.g. in tests).
	var matched []Rule
	for _, r := range s.Rules {
		if r.EventType == "*" || r.EventType == eventType {
			matched = append(matched, r)
		}
	}
	return matched
}

// buildIndex precomputes per-event-type rule lists so RulesForEvent is a
// single map lookup with no per-event allocation. Rules must already be
// sorted by priority descending; the merge preserves that order. (Ties
// between a wildcard and a type-specific rule prefer the type-specific
// one — equal-priority order is unspecified anyway since the sort is not
// stable.) One pass over the rules plus one merge per type: O(N + T*W)
// instead of O(N*T).
func (s *Snapshot) buildIndex() {
	s.byType = make(map[string][]Rule)
	s.wildcard = nil
	for _, r := range s.Rules {
		if r.EventType == "*" {
			s.wildcard = append(s.wildcard, r)
		} else {
			s.byType[r.EventType] = append(s.byType[r.EventType], r)
		}
	}
	if len(s.wildcard) == 0 {
		return
	}
	for t, list := range s.byType {
		s.byType[t] = mergeByPriority(list, s.wildcard)
	}
}

// mergeByPriority merges two priority-descending rule slices into a new
// priority-descending slice.
func mergeByPriority(a, b []Rule) []Rule {
	out := make([]Rule, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i].Priority >= b[j].Priority {
			out = append(out, a[i])
			i++
		} else {
			out = append(out, b[j])
			j++
		}
	}
	out = append(out, a[i:]...)
	return append(out, b[j:]...)
}

// Compiler loads and compiles Starlark rules. It memoizes compiled rules by
// file content hash, so reloading a 10k-rule directory after a one-file
// edit costs one compile, not 10k. Cached rules share their Prefiltered
// counters across snapshots, so prefilter stats survive reloads for
// unchanged rules.
type Compiler struct {
	UDFs starlark.StringDict

	mu    sync.Mutex
	cache map[string]*Rule // content hash -> compiled rule
}

// cachedCompile returns the rule for src, compiling on cache miss.
func (c *Compiler) cachedCompile(filename string, src []byte) (*Rule, string, error) {
	sum := sha256.Sum256(src)
	key := string(sum[:])

	c.mu.Lock()
	rule, ok := c.cache[key]
	c.mu.Unlock()
	if ok {
		return rule, key, nil
	}

	rule, err := c.CompileSource(filename, string(src))
	if err != nil {
		return nil, "", err
	}
	c.mu.Lock()
	if c.cache == nil {
		c.cache = make(map[string]*Rule)
	}
	c.cache[key] = rule
	c.mu.Unlock()
	return rule, key, nil
}

// prune drops cache entries not used by the latest compile so renamed or
// deleted rules don't accumulate.
func (c *Compiler) prune(used map[string]bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key := range c.cache {
		if !used[key] {
			delete(c.cache, key)
		}
	}
}

// CompileDir reads all *.star files from dir, compiles them, and returns
// an immutable Snapshot. All-or-nothing: if any rule fails, returns error.
func (c *Compiler) CompileDir(dir string) (*Snapshot, error) {
	if _, err := os.Stat(dir); err != nil {
		return nil, fmt.Errorf("rules directory: %w", err)
	}
	pattern := filepath.Join(dir, "*.star")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("glob %s: %w", pattern, err)
	}

	// Compile files in parallel (compilation is pure); report the first
	// error in deterministic (sorted-glob) order. Unchanged files hit the
	// content cache instead of recompiling.
	compiled := make([]*Rule, len(files))
	hashes := make([]string, len(files))
	errs := make([]error, len(files))
	var wg sync.WaitGroup
	sem := make(chan struct{}, runtime.GOMAXPROCS(0))
	for i, path := range files {
		wg.Add(1)
		sem <- struct{}{} // bound in-flight goroutines, not just running ones
		go func(i int, path string) {
			defer wg.Done()
			defer func() { <-sem }()
			src, err := os.ReadFile(path)
			if err != nil {
				errs[i] = fmt.Errorf("read %s: %w", path, err)
				return
			}
			compiled[i], hashes[i], errs[i] = c.cachedCompile(filepath.Base(path), src)
		}(i, path)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	rules := make([]Rule, 0, len(files))
	seen := make(map[string]string) // rule_id -> filename
	used := make(map[string]bool, len(files))
	for i, rule := range compiled {
		if prev, dup := seen[rule.RuleID]; dup {
			return nil, fmt.Errorf("duplicate rule_id %q in %s and %s", rule.RuleID, prev, filepath.Base(files[i]))
		}
		seen[rule.RuleID] = filepath.Base(files[i])
		used[hashes[i]] = true
		rules = append(rules, *rule)
	}
	c.prune(used)

	// Sort by priority descending.
	sort.Slice(rules, func(i, j int) bool {
		return rules[i].Priority > rules[j].Priority
	})

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate snapshot ID: %w", err)
	}

	snap := &Snapshot{
		ID:       id.String(),
		Rules:    rules,
		LoadedAt: time.Now(),
	}
	snap.buildIndex()
	return snap, nil
}

// CompileSource compiles a single Starlark source string into a Rule.
func (c *Compiler) CompileSource(filename, source string) (*Rule, error) {
	// Step 1: Parse source into AST.
	file, err := syntax.Parse(filename, source, 0)
	if err != nil {
		return nil, err
	}

	// Step 2: Compile AST into a reusable Program.
	isPredeclared := func(name string) bool {
		_, ok := c.UDFs[name]
		return ok
	}
	prog, err := starlark.FileProgram(file, isPredeclared)
	if err != nil {
		return nil, err
	}

	// Step 3: Execute the program once to extract globals, then freeze them
	// so the resulting evaluate callable is safe to share across workers
	// (and module-level state mutation is an error, not nondeterminism).
	// Stateful UDFs (counter, memo, regex_match) resolve their environment
	// from the evaluating thread, so no per-worker re-initialization is
	// needed; calling them here, at module scope, fails by design.
	//
	// Module scope runs arbitrary code, so it gets the same step budget as
	// rule evaluation: an unbounded Init would wedge the reloader (and
	// startup) on a runaway module-scope loop until process restart.
	thread := &starlark.Thread{Name: "compile:" + filename}
	thread.SetMaxExecutionSteps(compileStepBudget)
	globals, err := prog.Init(thread, c.UDFs)
	if err != nil {
		return nil, err
	}
	globals.Freeze()

	// Step 4: Extract and validate metadata.
	ruleID, err := stringGlobal(globals, filename, "rule_id")
	if err != nil {
		return nil, err
	}
	eventType, err := stringGlobal(globals, filename, "event_type")
	if err != nil {
		return nil, err
	}
	priority, err := intGlobal(globals, filename, "priority")
	if err != nil {
		return nil, err
	}

	evalVal, ok := globals["evaluate"]
	if !ok {
		return nil, fmt.Errorf("%s: missing required global 'evaluate'", filename)
	}
	evalFn, ok := evalVal.(starlark.Callable)
	if !ok {
		return nil, fmt.Errorf("%s: 'evaluate' must be callable, got %s", filename, evalVal.Type())
	}

	// Optional tier-1 prefilter.
	var match []Predicate
	if matchVal, ok := globals["match"]; ok {
		match, err = lowerMatch(filename, matchVal)
		if err != nil {
			return nil, err
		}
	}

	return &Rule{
		RuleID:      ruleID,
		EventType:   eventType,
		Priority:    priority,
		Program:     prog,
		Evaluate:    evalFn,
		Match:       match,
		Prefiltered: &atomic.Int64{},
		Stats:       &RuleStats{},
	}, nil
}

func stringGlobal(globals starlark.StringDict, filename, name string) (string, error) {
	v, ok := globals[name]
	if !ok {
		return "", fmt.Errorf("%s: missing required global '%s'", filename, name)
	}
	s, ok := v.(starlark.String)
	if !ok {
		return "", fmt.Errorf("%s: '%s' must be string, got %s", filename, name, v.Type())
	}
	return string(s), nil
}

func intGlobal(globals starlark.StringDict, filename, name string) (int, error) {
	v, ok := globals[name]
	if !ok {
		return 0, fmt.Errorf("%s: missing required global '%s'", filename, name)
	}
	i, ok := v.(starlark.Int)
	if !ok {
		return 0, fmt.Errorf("%s: '%s' must be int, got %s", filename, name, v.Type())
	}
	val, ok := i.Int64()
	if !ok {
		return 0, fmt.Errorf("%s: '%s' value overflows int64", filename, name)
	}
	return int(val), nil
}
