package output

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vinaysrao1/fruitfly/types"
)

// benchPayload builds a ~1KB event payload typical of a JSON event.
func benchPayload() (map[string]any, []byte) {
	m := map[string]any{
		"event_type": "post",
		"timestamp":  "2026-06-10T12:00:00Z",
		"entity_id":  "user-12345",
		"text":       strings.Repeat("lorem ipsum dolor sit amet ", 30),
		"counts":     map[string]any{"likes": float64(10), "replies": float64(2)},
		"tags":       []any{"a", "b", "c"},
	}
	raw, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return m, raw
}

func benchResults(n int, withRaw bool) []types.Result {
	m, raw := benchPayload()
	out := make([]types.Result, n)
	for i := range out {
		out[i] = types.Result{
			EventID:        fmt.Sprintf("bench-%d-%d", time.Now().UnixNano(), i),
			EventType:      "post",
			FinalVerdict:   types.VerdictApprove,
			TriggeredRules: []types.RuleResult{{RuleID: "r1", Verdict: types.VerdictApprove}},
			Payload:        m,
			LatencyUS:      123,
			ProcessedAt:    time.Now(),
		}
		if withRaw {
			out[i].RawPayload = raw
		}
	}
	return out
}

// BenchmarkInsertBatch_RawPayload measures a 100-row DuckDB batch insert when
// the raw ingested JSON is reused for the payload column (new path).
func BenchmarkInsertBatch_RawPayload(b *testing.B) {
	w, err := NewWriter(b.TempDir()+"/bench.duckdb", "")
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		batch := benchResults(100, true)
		b.StartTimer()
		if err := w.insertBatch(batch); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkInsertBatch_MarshalFallback measures the same insert when the
// payload map must be re-marshalled per row (equivalent to the old path).
func BenchmarkInsertBatch_MarshalFallback(b *testing.B) {
	w, err := NewWriter(b.TempDir()+"/bench.duckdb", "")
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		batch := benchResults(100, false)
		b.StartTimer()
		if err := w.insertBatch(batch); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPayloadMarshal isolates the pure per-event marshal cost that the
// RawPayload optimization removes.
func BenchmarkPayloadMarshal(b *testing.B) {
	m, _ := benchPayload()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(m); err != nil {
			b.Fatal(err)
		}
	}
}
