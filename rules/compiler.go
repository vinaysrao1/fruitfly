package rules

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
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
}

// Snapshot is an immutable collection of compiled rules, sorted by priority desc.
type Snapshot struct {
	ID       string
	Rules    []Rule
	LoadedAt time.Time
}

// RulesForEvent returns rules matching the given event type (including wildcard "*").
func (s *Snapshot) RulesForEvent(eventType string) []Rule {
	var matched []Rule
	for _, r := range s.Rules {
		if r.EventType == "*" || r.EventType == eventType {
			matched = append(matched, r)
		}
	}
	return matched
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

	rules := make([]Rule, 0, len(files))
	seen := make(map[string]string) // rule_id -> filename

	for _, path := range files {
		src, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}

		rule, err := c.CompileSource(filepath.Base(path), string(src))
		if err != nil {
			return nil, err
		}

		if prev, dup := seen[rule.RuleID]; dup {
			return nil, fmt.Errorf("duplicate rule_id %q in %s and %s", rule.RuleID, prev, filepath.Base(path))
		}
		seen[rule.RuleID] = filepath.Base(path)
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

	return &Snapshot{
		ID:       id.String(),
		Rules:    rules,
		LoadedAt: time.Now(),
	}, nil
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

	return &Rule{
		RuleID:    ruleID,
		EventType: eventType,
		Priority:  priority,
		Program:   prog,
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
