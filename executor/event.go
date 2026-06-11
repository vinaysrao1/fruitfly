package executor

import (
	"fmt"
	"sort"

	"github.com/vinaysrao1/fruitfly/types"
	"go.starlark.net/starlark"
)

// eventToStarlark wraps a types.Event in a lazy, immutable Starlark view.
// Field values are converted on first access and memoized, so rules pay
// conversion cost only for the fields they actually touch — nothing is
// converted for an event whose rules read no payload fields.
func eventToStarlark(event types.Event) starlark.Value {
	return &lazyDict{src: map[string]any{
		"event_id":   event.EventID,
		"event_type": event.EventType,
		"timestamp":  event.Timestamp.Unix(),
		"payload":    event.Payload,
	}}
}

// lazyDict is an immutable dict-like Starlark view over a Go map. Values
// convert lazily on access; nested maps become nested lazyDicts. The view
// is shared by every rule evaluated for an event, so it is born frozen:
// assignment fails, and converted values are frozen before caching.
//
// One lazyDict is only ever accessed by the single worker goroutine
// evaluating its event, so the memo cache needs no synchronization.
type lazyDict struct {
	src   map[string]any
	cache map[string]starlark.Value
}

var (
	_ starlark.IterableMapping = (*lazyDict)(nil)
	_ starlark.Sequence        = (*lazyDict)(nil)
	_ starlark.HasSetKey       = (*lazyDict)(nil)
	_ starlark.HasAttrs        = (*lazyDict)(nil)
)

func (d *lazyDict) Type() string          { return "dict" }
func (d *lazyDict) Freeze()               {} // born frozen
func (d *lazyDict) Truth() starlark.Bool  { return len(d.src) > 0 }
func (d *lazyDict) Len() int              { return len(d.src) }
func (d *lazyDict) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable type: dict") }

func (d *lazyDict) String() string {
	return fmt.Sprintf("<event dict, %d keys>", len(d.src))
}

func (d *lazyDict) value(key string) starlark.Value {
	if v, ok := d.cache[key]; ok {
		return v
	}
	v := lazyConvert(d.src[key])
	if d.cache == nil {
		d.cache = make(map[string]starlark.Value, len(d.src))
	}
	d.cache[key] = v
	return v
}

// Get implements starlark.Mapping (event["key"], "key" in event).
func (d *lazyDict) Get(k starlark.Value) (starlark.Value, bool, error) {
	key, ok := starlark.AsString(k)
	if !ok {
		return nil, false, nil
	}
	if _, present := d.src[key]; !present {
		return nil, false, nil
	}
	return d.value(key), true, nil
}

// SetKey rejects assignment: the event is shared across all rules for an
// event, so mutation must be a rule error, never cross-rule state leakage.
func (d *lazyDict) SetKey(k, v starlark.Value) error {
	return fmt.Errorf("cannot assign to frozen event dict")
}

// sortedKeys returns the keys in deterministic order (Go map iteration is
// randomized; rules deserve stable iteration).
func (d *lazyDict) sortedKeys() []string {
	keys := make([]string, 0, len(d.src))
	for k := range d.src {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Iterate implements starlark.Iterable (for k in event).
func (d *lazyDict) Iterate() starlark.Iterator {
	return &lazyDictIterator{keys: d.sortedKeys()}
}

// Items implements starlark.IterableMapping.
func (d *lazyDict) Items() []starlark.Tuple {
	items := make([]starlark.Tuple, 0, len(d.src))
	for _, k := range d.sortedKeys() {
		items = append(items, starlark.Tuple{starlark.String(k), d.value(k)})
	}
	return items
}

type lazyDictIterator struct {
	keys []string
	i    int
}

func (it *lazyDictIterator) Next(p *starlark.Value) bool {
	if it.i >= len(it.keys) {
		return false
	}
	*p = starlark.String(it.keys[it.i])
	it.i++
	return true
}

func (it *lazyDictIterator) Done() {}

// Attr provides the dict methods rules rely on: get, keys, items, values.
func (d *lazyDict) Attr(name string) (starlark.Value, error) {
	switch name {
	case "get":
		return starlark.NewBuiltin("get", func(_ *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			var key starlark.Value
			var dflt starlark.Value = starlark.None
			if err := starlark.UnpackPositionalArgs("get", args, kwargs, 1, &key, &dflt); err != nil {
				return nil, err
			}
			if v, found, err := d.Get(key); err != nil {
				return nil, err
			} else if found {
				return v, nil
			}
			return dflt, nil
		}), nil
	case "keys":
		return starlark.NewBuiltin("keys", func(_ *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			if err := starlark.UnpackPositionalArgs("keys", args, kwargs, 0); err != nil {
				return nil, err
			}
			keys := d.sortedKeys()
			elems := make([]starlark.Value, len(keys))
			for i, k := range keys {
				elems[i] = starlark.String(k)
			}
			l := starlark.NewList(elems)
			l.Freeze()
			return l, nil
		}), nil
	case "items":
		return starlark.NewBuiltin("items", func(_ *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			if err := starlark.UnpackPositionalArgs("items", args, kwargs, 0); err != nil {
				return nil, err
			}
			items := d.Items()
			elems := make([]starlark.Value, len(items))
			for i, item := range items {
				elems[i] = item
			}
			l := starlark.NewList(elems)
			l.Freeze()
			return l, nil
		}), nil
	case "values":
		return starlark.NewBuiltin("values", func(_ *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			if err := starlark.UnpackPositionalArgs("values", args, kwargs, 0); err != nil {
				return nil, err
			}
			keys := d.sortedKeys()
			elems := make([]starlark.Value, len(keys))
			for i, k := range keys {
				elems[i] = d.value(k)
			}
			l := starlark.NewList(elems)
			l.Freeze()
			return l, nil
		}), nil
	}
	return nil, nil
}

func (d *lazyDict) AttrNames() []string {
	return []string{"get", "items", "keys", "values"}
}

// lazyConvert converts one Go value to a frozen Starlark value. Maps become
// lazyDicts (deferring their own conversion); other composites convert
// eagerly and are frozen so rules cannot mutate shared state.
func lazyConvert(v any) starlark.Value {
	if v == nil {
		return starlark.None
	}
	switch val := v.(type) {
	case bool:
		return starlark.Bool(val)
	case float64:
		return starlark.Float(val)
	case int: // defensive: unreachable from JSON unmarshalling
		return starlark.MakeInt(val)
	case int64: // defensive: unreachable from JSON unmarshalling
		return starlark.MakeInt64(val)
	case string:
		return starlark.String(val)
	case []any:
		elems := make([]starlark.Value, len(val))
		for i, elem := range val {
			elems[i] = lazyConvert(elem)
		}
		l := starlark.NewList(elems)
		l.Freeze()
		return l
	case map[string]any:
		return &lazyDict{src: val}
	default:
		return starlark.String(fmt.Sprintf("%v", val))
	}
}
