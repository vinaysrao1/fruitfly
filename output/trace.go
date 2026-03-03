package output

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/vinaysrao1/fruitfly/types"
)

// TraceEntry is a single JSONL record written by the TraceRecorder.
type TraceEntry struct {
	Event      types.Event  `json:"event"`
	Result     types.Result `json:"result"`
	SnapshotID string       `json:"snapshot_id"`
	Timestamp  time.Time    `json:"timestamp"`
}

// TraceRecorder taps the result pipeline and writes JSONL trace files.
// It is safe for concurrent use. When disabled, all operations are no-ops.
type TraceRecorder struct {
	mu      sync.Mutex
	file    *os.File
	encoder *json.Encoder
	enabled bool
}

// NewTraceRecorder creates a TraceRecorder. When enabled is false, the recorder is a no-op
// and path is ignored. When enabled, the file at path is opened (or created) for appending.
func NewTraceRecorder(enabled bool, path string) (*TraceRecorder, error) {
	if !enabled {
		return &TraceRecorder{enabled: false}, nil
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open trace file %s: %w", path, err)
	}

	return &TraceRecorder{
		file:    f,
		encoder: json.NewEncoder(f),
		enabled: true,
	}, nil
}

// Record writes a trace entry for the given event and result.
// snapshotID is the ID of the snapshot that evaluated the event.
// No-op when the recorder is disabled.
func (tr *TraceRecorder) Record(event types.Event, result types.Result, snapshotID string) {
	if !tr.enabled {
		return
	}

	entry := TraceEntry{
		Event:      event,
		Result:     result,
		SnapshotID: snapshotID,
		Timestamp:  time.Now(),
	}

	tr.mu.Lock()
	defer tr.mu.Unlock()
	_ = tr.encoder.Encode(entry) // best-effort; log errors are swallowed to avoid disrupting the pipeline
}

// Close flushes and closes the underlying trace file.
// No-op when the recorder is disabled.
func (tr *TraceRecorder) Close() error {
	if !tr.enabled || tr.file == nil {
		return nil
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.file.Close()
}
