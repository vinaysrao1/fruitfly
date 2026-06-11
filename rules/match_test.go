package rules

import (
	"strings"
	"testing"
	"time"

	"github.com/vinaysrao1/fruitfly/types"
)

func matchEvent(payload map[string]any) *types.Event {
	return &types.Event{
		EventID:   "evt-1",
		EventType: "post",
		EntityID:  "user-1",
		Timestamp: time.Unix(1700000000, 0),
		Payload:   payload,
	}
}

func compileMatch(t *testing.T, matchBlock string) *Rule {
	t.Helper()
	c := newTestCompiler()
	rule, err := c.CompileSource("m.star", `
rule_id = "m"
event_type = "post"
priority = 100
match = `+matchBlock+`
def evaluate(event):
    return verdict("approve")
`)
	if err != nil {
		t.Fatalf("CompileSource: %v", err)
	}
	return rule
}

func TestPrefilter_Ops(t *testing.T) {
	tests := []struct {
		name    string
		match   string
		payload map[string]any
		want    bool
	}{
		{"eq number true", `{"all": [["payload.n", "==", 5]]}`, map[string]any{"n": float64(5)}, true},
		{"eq number false", `{"all": [["payload.n", "==", 5]]}`, map[string]any{"n": float64(6)}, false},
		{"eq string", `{"all": [["payload.lang", "==", "en"]]}`, map[string]any{"lang": "en"}, true},
		{"eq type mismatch false", `{"all": [["payload.n", "==", 5]]}`, map[string]any{"n": "5"}, false},
		{"ne comparable", `{"all": [["payload.n", "!=", 5]]}`, map[string]any{"n": float64(6)}, true},
		{"ne type mismatch false", `{"all": [["payload.n", "!=", 5]]}`, map[string]any{"n": "x"}, false},
		{"lt", `{"all": [["payload.n", "<", 20]]}`, map[string]any{"n": float64(19)}, true},
		{"lt boundary", `{"all": [["payload.n", "<", 20]]}`, map[string]any{"n": float64(20)}, false},
		{"ge", `{"all": [["payload.n", ">=", 20]]}`, map[string]any{"n": float64(20)}, true},
		{"missing field false", `{"all": [["payload.n", "<", 20]]}`, map[string]any{}, false},
		{"in hit", `{"all": [["payload.lang", "in", ["en", "es"]]]}`, map[string]any{"lang": "es"}, true},
		{"in miss", `{"all": [["payload.lang", "in", ["en", "es"]]]}`, map[string]any{"lang": "fr"}, false},
		{"exists true", `{"all": [["payload.flag", "exists"]]}`, map[string]any{"flag": nil}, true},
		{"exists false", `{"all": [["payload.flag", "exists"]]}`, map[string]any{}, false},
		{"prefix", `{"all": [["payload.id", "prefix", "user:"]]}`, map[string]any{"id": "user:9"}, true},
		{"prefix non-string false", `{"all": [["payload.id", "prefix", "user:"]]}`, map[string]any{"id": float64(9)}, false},
		{"nested path", `{"all": [["payload.meta.depth", ">", 1]]}`, map[string]any{"meta": map[string]any{"depth": float64(2)}}, true},
		{"envelope event_type", `{"all": [["event_type", "==", "post"]]}`, map[string]any{}, true},
		{"all conjunction", `{"all": [["payload.n", "<", 20], ["payload.lang", "==", "en"]]}`,
			map[string]any{"n": float64(5), "lang": "fr"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := compileMatch(t, tt.match)
			if got := rule.PrefilterMatch(matchEvent(tt.payload)); got != tt.want {
				t.Errorf("PrefilterMatch = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPrefilter_NoMatchBlockAlwaysPasses(t *testing.T) {
	c := newTestCompiler()
	rule, err := c.CompileSource("plain.star", `
rule_id = "plain"
event_type = "post"
priority = 100
def evaluate(event):
    return verdict("approve")
`)
	if err != nil {
		t.Fatal(err)
	}
	if !rule.PrefilterMatch(matchEvent(nil)) {
		t.Error("rule without match block must always pass the prefilter")
	}
}

func TestPrefilter_LoweringErrors(t *testing.T) {
	tests := []struct {
		name    string
		match   string
		wantErr string
	}{
		{"not a dict", `[["payload.n", "<", 20]]`, "must be a dict"},
		{"wrong key", `{"any": [["payload.n", "<", 20]]}`, `exactly one key`},
		{"bad op", `{"all": [["payload.n", "~=", 20]]}`, "unsupported op"},
		{"exists with value", `{"all": [["payload.n", "exists", 1]]}`, "takes no value"},
		{"missing value", `{"all": [["payload.n", "<"]]}`, "requires a value"},
		{"heterogeneous in", `{"all": [["payload.n", "in", [1, "two"]]]}`, "homogeneous"},
		{"empty in", `{"all": [["payload.n", "in", []]]}`, "non-empty list"},
		{"prefix non-string", `{"all": [["payload.n", "prefix", 5]]}`, "requires a string"},
		{"lt non-number", `{"all": [["payload.n", "<", "x"]]}`, "requires a numeric"},
		{"empty path", `{"all": [["", "<", 20]]}`, "non-empty string"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestCompiler()
			_, err := c.CompileSource("bad.star", `
rule_id = "bad"
event_type = "post"
priority = 100
match = `+tt.match+`
def evaluate(event):
    return verdict("approve")
`)
			if err == nil {
				t.Fatal("expected lowering error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}
