package rules

import (
	"fmt"
	"strings"

	"github.com/vinaysrao1/fruitfly/types"
	"go.starlark.net/starlark"
)

// Predicate is one tier-1 prefilter clause, lowered from a rule's optional
// `match` global. Clauses evaluate natively against the event — no Starlark
// — in nanoseconds. Semantics are fixed (docs/SCALING_10M.md §3): a missing
// or type-mismatched field makes the clause false (the rule is skipped,
// never errored); numbers compare as float64; `exists` is the explicit
// presence test.
type Predicate struct {
	Path  []string // field path, e.g. ["payload", "char_count"]
	Op    string   // ==, !=, <, <=, >, >=, in, exists, prefix
	Value any      // comparison constant: float64, string, bool, or []any for in
}

// matchOps is the closed set of supported tier-1 operators.
var matchOps = map[string]bool{
	"==": true, "!=": true, "<": true, "<=": true, ">": true, ">=": true,
	"in": true, "exists": true, "prefix": true,
}

// PrefilterMatch reports whether the event passes every tier-1 clause.
// Rules without a match block always pass.
func (r *Rule) PrefilterMatch(event *types.Event) bool {
	for _, p := range r.Match {
		if !p.eval(event) {
			return false
		}
	}
	return true
}

func (p *Predicate) eval(event *types.Event) bool {
	field, ok := resolvePath(event, p.Path)
	if p.Op == "exists" {
		return ok
	}
	if !ok {
		return false
	}
	switch p.Op {
	case "==":
		return scalarEqual(field, p.Value)
	case "!=":
		// Type mismatch is clause-false, so != requires comparable values.
		return scalarComparable(field, p.Value) && !scalarEqual(field, p.Value)
	case "<", "<=", ">", ">=":
		f, w, ok := comparable2(field, p.Value)
		if !ok {
			return false
		}
		switch p.Op {
		case "<":
			return f < w
		case "<=":
			return f <= w
		case ">":
			return f > w
		default:
			return f >= w
		}
	case "in":
		list, ok := p.Value.([]any)
		if !ok {
			return false
		}
		for _, item := range list {
			if scalarEqual(field, item) {
				return true
			}
		}
		return false
	case "prefix":
		fs, ok1 := field.(string)
		ws, ok2 := p.Value.(string)
		return ok1 && ok2 && strings.HasPrefix(fs, ws)
	}
	return false
}

// resolvePath walks the event by path. The first segment addresses the
// event envelope ("payload", "event_type", "event_id", "entity_id",
// "timestamp"); the rest descend payload maps.
func resolvePath(event *types.Event, path []string) (any, bool) {
	var cur any
	switch path[0] {
	case "payload":
		cur = event.Payload
	case "event_type":
		cur = event.EventType
	case "event_id":
		cur = event.EventID
	case "entity_id":
		cur = event.EntityID
	case "timestamp":
		cur = float64(event.Timestamp.Unix())
	default:
		return nil, false
	}
	for _, seg := range path[1:] {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[seg]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

// scalarEqual compares two scalars with float64 number coercion; mismatched
// types are unequal (clause false).
func scalarEqual(field, want any) bool {
	if f, w, ok := comparable2(field, want); ok {
		return f == w
	}
	switch fv := field.(type) {
	case string:
		wv, ok := want.(string)
		return ok && fv == wv
	case bool:
		wv, ok := want.(bool)
		return ok && fv == wv
	}
	return false
}

// scalarComparable reports whether the two values belong to the same
// comparable class (both numeric, both string, or both bool).
func scalarComparable(field, want any) bool {
	if _, _, ok := comparable2(field, want); ok {
		return true
	}
	switch field.(type) {
	case string:
		_, ok := want.(string)
		return ok
	case bool:
		_, ok := want.(bool)
		return ok
	}
	return false
}

// comparable2 coerces both values to float64 when both are numeric.
func comparable2(field, want any) (f, w float64, ok bool) {
	f, ok1 := toFloat(field)
	w, ok2 := toFloat(want)
	return f, w, ok1 && ok2
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}

// lowerMatch validates and lowers a rule's `match` global into predicates.
// The accepted shape is exactly {"all": [[path, op, value?], ...]}.
func lowerMatch(filename string, v starlark.Value) ([]Predicate, error) {
	dict, ok := v.(*starlark.Dict)
	if !ok {
		return nil, fmt.Errorf("%s: 'match' must be a dict, got %s", filename, v.Type())
	}
	if dict.Len() != 1 {
		return nil, fmt.Errorf("%s: 'match' must have exactly one key: \"all\"", filename)
	}
	allVal, found, _ := dict.Get(starlark.String("all"))
	if !found {
		return nil, fmt.Errorf("%s: 'match' must have exactly one key: \"all\"", filename)
	}
	clauses, ok := allVal.(*starlark.List)
	if !ok {
		return nil, fmt.Errorf("%s: match[\"all\"] must be a list of clauses", filename)
	}

	preds := make([]Predicate, 0, clauses.Len())
	for i := 0; i < clauses.Len(); i++ {
		clause, ok := clauses.Index(i).(*starlark.List)
		if !ok {
			return nil, fmt.Errorf("%s: match clause %d must be a list [path, op, value]", filename, i)
		}
		n := clause.Len()
		if n < 2 || n > 3 {
			return nil, fmt.Errorf("%s: match clause %d must be [path, op] or [path, op, value]", filename, i)
		}
		pathStr, ok := starlark.AsString(clause.Index(0))
		if !ok || pathStr == "" {
			return nil, fmt.Errorf("%s: match clause %d: path must be a non-empty string", filename, i)
		}
		op, ok := starlark.AsString(clause.Index(1))
		if !ok || !matchOps[op] {
			return nil, fmt.Errorf("%s: match clause %d: unsupported op %q", filename, i, clause.Index(1))
		}
		p := Predicate{Path: strings.Split(pathStr, "."), Op: op}

		switch {
		case op == "exists":
			if n != 2 {
				return nil, fmt.Errorf("%s: match clause %d: exists takes no value", filename, i)
			}
		case n != 3:
			return nil, fmt.Errorf("%s: match clause %d: op %q requires a value", filename, i, op)
		case op == "in":
			list, ok := clause.Index(2).(*starlark.List)
			if !ok || list.Len() == 0 {
				return nil, fmt.Errorf("%s: match clause %d: 'in' requires a non-empty list", filename, i)
			}
			items := make([]any, list.Len())
			var firstType string
			for j := 0; j < list.Len(); j++ {
				item, err := lowerScalar(list.Index(j))
				if err != nil {
					return nil, fmt.Errorf("%s: match clause %d: %v", filename, i, err)
				}
				t := scalarType(item)
				if j == 0 {
					firstType = t
				} else if t != firstType {
					return nil, fmt.Errorf("%s: match clause %d: 'in' list must be homogeneous (%s vs %s)", filename, i, firstType, t)
				}
				items[j] = item
			}
			p.Value = items
		default:
			val, err := lowerScalar(clause.Index(2))
			if err != nil {
				return nil, fmt.Errorf("%s: match clause %d: %v", filename, i, err)
			}
			if op == "prefix" {
				if _, ok := val.(string); !ok {
					return nil, fmt.Errorf("%s: match clause %d: prefix requires a string value", filename, i)
				}
			}
			if op == "<" || op == "<=" || op == ">" || op == ">=" {
				if _, ok := val.(float64); !ok {
					return nil, fmt.Errorf("%s: match clause %d: op %q requires a numeric value", filename, i, op)
				}
			}
			p.Value = val
		}
		preds = append(preds, p)
	}
	return preds, nil
}

// lowerScalar converts a Starlark scalar constant to its Go form (numbers
// become float64, matching JSON decoding).
func lowerScalar(v starlark.Value) (any, error) {
	switch val := v.(type) {
	case starlark.String:
		return string(val), nil
	case starlark.Int:
		i, ok := val.Int64()
		if !ok {
			return nil, fmt.Errorf("integer constant overflows int64")
		}
		return float64(i), nil
	case starlark.Float:
		return float64(val), nil
	case starlark.Bool:
		return bool(val), nil
	}
	return nil, fmt.Errorf("unsupported constant type %s (must be string, number, or bool)", v.Type())
}

func scalarType(v any) string {
	switch v.(type) {
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "bool"
	}
	return "unknown"
}
