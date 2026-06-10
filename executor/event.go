package executor

import (
	"fmt"

	"github.com/vinaysrao1/fruitfly/types"
	"go.starlark.net/starlark"
)

// eventToStarlark converts a types.Event to a Starlark dict.
// The dict is frozen: it is shared by every rule evaluated for the event,
// and an unfrozen dict would let one rule mutate what later rules see.
func eventToStarlark(event types.Event) *starlark.Dict {
	d := starlark.NewDict(4)
	d.SetKey(starlark.String("event_id"), starlark.String(event.EventID))
	d.SetKey(starlark.String("event_type"), starlark.String(event.EventType))
	d.SetKey(starlark.String("timestamp"), starlark.MakeInt64(event.Timestamp.Unix()))
	d.SetKey(starlark.String("payload"), anyToStarlark(event.Payload))
	d.Freeze()
	return d
}

func anyToStarlark(v any) starlark.Value {
	if v == nil {
		return starlark.None
	}
	switch val := v.(type) {
	case bool:
		return starlark.Bool(val)
	case float64:
		return starlark.Float(val)
	case int: // defensive: unreachable from JSON unmarshalling (JSON numbers become float64); kept for non-JSON callers
		return starlark.MakeInt(val)
	case int64: // defensive: unreachable from JSON unmarshalling (JSON numbers become float64); kept for non-JSON callers
		return starlark.MakeInt64(val)
	case string:
		return starlark.String(val)
	case []any:
		elems := make([]starlark.Value, len(val))
		for i, elem := range val {
			elems[i] = anyToStarlark(elem)
		}
		return starlark.NewList(elems)
	case map[string]any:
		d := starlark.NewDict(len(val))
		for k, v2 := range val {
			d.SetKey(starlark.String(k), anyToStarlark(v2))
		}
		return d
	default:
		return starlark.String(fmt.Sprintf("%v", val))
	}
}
