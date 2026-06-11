package rules

import (
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

// Rule is a compiled Starlark rule ready for execution.
type Rule struct {
	RuleID    string
	EventType string // "*" matches all
	Priority  int
	Program   *starlark.Program
	// Match holds the lowered tier-1 prefilter clauses (nil = always run).
	Match []Predicate
	// Prefiltered counts events skipped by the prefilter; a pointer so all
	// copies of this Rule share one counter, surfaced via /admin/rules so
	// an over-aggressive match block is visible rather than silent.
	Prefiltered *atomic.Int64
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

// Compiler loads and compiles Starlark rules.
type Compiler struct {
	UDFs starlark.StringDict
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
	// error in deterministic (sorted-glob) order.
	compiled := make([]*Rule, len(files))
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
			compiled[i], errs[i] = c.CompileSource(filepath.Base(path), string(src))
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
	for i, rule := range compiled {
		if prev, dup := seen[rule.RuleID]; dup {
			return nil, fmt.Errorf("duplicate rule_id %q in %s and %s", rule.RuleID, prev, filepath.Base(files[i]))
		}
		seen[rule.RuleID] = filepath.Base(files[i])
		rules = append(rules, *rule)
	}

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

	// Step 3: Execute the program to extract globals.
	thread := &starlark.Thread{Name: "compile:" + filename}
	globals, err := prog.Init(thread, c.UDFs)
	if err != nil {
		return nil, err
	}

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
	if _, ok := evalVal.(starlark.Callable); !ok {
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
		Match:       match,
		Prefiltered: &atomic.Int64{},
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
