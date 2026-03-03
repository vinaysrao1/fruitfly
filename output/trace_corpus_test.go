package output

// T3: Trace corpus from jetstream test data.
//
// This file generates a JSONL trace corpus representing realistic traffic patterns:
// - Posts (normal and short)
// - Likes (normal and flood)
// - Spam bursts (many events from one entity)
// - Mixed traffic (multiple entities, multiple event types)
//
// The corpus is written to jetstream_test/traces/corpus.jsonl and can be used
// with the replay CLI tool (cmd/replay) to detect regressions across rule changes.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vinaysrao1/fruitfly/executor"
	"github.com/vinaysrao1/fruitfly/rules"
	"github.com/vinaysrao1/fruitfly/types"
)

// generateCorpusEvent builds a realistic event for the corpus.
func generateCorpusEvent(id int, eventType, entityID string, payload map[string]any) types.Event {
	return types.Event{
		EventID:    fmt.Sprintf("corpus-%s-%04d", eventType, id),
		EventType:  eventType,
		Timestamp:  time.Now(),
		Payload:    payload,
		ReceivedAt: time.Now(),
	}
}

// TestGenerateTraceCorpus (T3) generates a trace corpus from jetstream-style events
// using the production rules. The corpus is written to jetstream_test/traces/corpus.jsonl.
//
// Run with: go test ./output/ -run TestGenerateTraceCorpus -v
// The generated file can then be used with: fruitfly-replay --traces jetstream_test/traces/corpus.jsonl --rules jetstream_test/rules
func TestGenerateTraceCorpus(t *testing.T) {
	// Compile production rules.
	compiler := &rules.Compiler{UDFs: rules.DefaultUDFs()}
	snap, err := compiler.CompileDir("../jetstream_test/rules")
	if err != nil {
		t.Fatalf("CompileDir: %v", err)
	}

	// Build corpus events representing realistic traffic patterns.
	var events []types.Event
	eventCounter := 0

	// Pattern 1: Normal posts (long text, no spam)
	for i := 0; i < 10; i++ {
		eventCounter++
		events = append(events, generateCorpusEvent(eventCounter, "post", fmt.Sprintf("user-%02d", i), map[string]any{
			"entity_id":  fmt.Sprintf("user-%02d", i),
			"text":       fmt.Sprintf("This is a normal post from user %d with enough content.", i),
			"char_count": 55,
		}))
	}

	// Pattern 2: Short posts (under flood threshold)
	for i := 0; i < 5; i++ {
		eventCounter++
		events = append(events, generateCorpusEvent(eventCounter, "post", "user-short-poster", map[string]any{
			"entity_id":  "user-short-poster",
			"text":       "hi",
			"char_count": 2,
		}))
	}

	// Pattern 3: Short post spam flood (> 5 short posts from same entity)
	for i := 0; i < 6; i++ {
		eventCounter++
		events = append(events, generateCorpusEvent(eventCounter, "post", "spam-short-poster", map[string]any{
			"entity_id":  "spam-short-poster",
			"text":       "hi",
			"char_count": 2,
		}))
	}

	// Pattern 4: Normal likes
	for i := 0; i < 5; i++ {
		eventCounter++
		events = append(events, generateCorpusEvent(eventCounter, "like", fmt.Sprintf("user-%02d", i), map[string]any{
			"entity_id": fmt.Sprintf("user-%02d", i),
		}))
	}

	// Pattern 5: Like flood (> 5 likes from same entity in window)
	for i := 0; i < 6; i++ {
		eventCounter++
		events = append(events, generateCorpusEvent(eventCounter, "like", "like-spammer", map[string]any{
			"entity_id": "like-spammer",
		}))
	}

	// Pattern 6: Numeric content (should trigger review)
	eventCounter++
	events = append(events, generateCorpusEvent(eventCounter, "post", "numeric-user", map[string]any{
		"entity_id":  "numeric-user",
		"text":       "Call 12345678901234567890 now!",
		"char_count": 30,
	}))

	// Pattern 7: Mixed traffic from many entities
	for i := 0; i < 10; i++ {
		eventCounter++
		entityID := fmt.Sprintf("mixed-user-%02d", i)
		eventType := "post"
		if i%2 == 0 {
			eventType = "like"
		}
		payload := map[string]any{
			"entity_id": entityID,
		}
		if eventType == "post" {
			payload["text"] = "Hello world from a mixed traffic user."
			payload["char_count"] = 38
		}
		events = append(events, generateCorpusEvent(eventCounter, eventType, entityID, payload))
	}

	// Run all corpus events through the production rules.
	var snapshotPtr atomic.Pointer[rules.Snapshot]
	snapshotPtr.Store(snap)
	pool := executor.NewPool(2, &snapshotPtr, 5*time.Second, 1*time.Second)

	in := make(chan types.Event, len(events))
	out := make(chan types.Result, len(events))

	for _, e := range events {
		in <- e
	}
	close(in)
	pool.Run(context.Background(), in, out)

	// Collect results indexed by event_id.
	resultMap := make(map[string]types.Result, len(events))
	for r := range out {
		resultMap[r.EventID] = r
	}

	// Write trace corpus to jetstream_test/traces/corpus.jsonl.
	corpusPath := filepath.Join("..", "jetstream_test", "traces", "corpus.jsonl")
	if err := os.MkdirAll(filepath.Dir(corpusPath), 0o755); err != nil {
		t.Fatalf("mkdir traces: %v", err)
	}

	f, err := os.Create(corpusPath)
	if err != nil {
		t.Fatalf("create corpus file: %v", err)
	}
	defer f.Close()

	writer := bufio.NewWriter(f)
	encoder := json.NewEncoder(writer)

	for _, event := range events {
		result, ok := resultMap[event.EventID]
		if !ok {
			t.Errorf("no result for event %s", event.EventID)
			continue
		}
		entry := TraceEntry{
			Event:      event,
			Result:     result,
			SnapshotID: snap.ID,
			Timestamp:  time.Now(),
		}
		if err := encoder.Encode(entry); err != nil {
			t.Fatalf("encode trace entry: %v", err)
		}
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush corpus: %v", err)
	}

	// Verify corpus was written correctly.
	corpusData, err := os.Open(corpusPath)
	if err != nil {
		t.Fatalf("open corpus for verification: %v", err)
	}
	defer corpusData.Close()

	scanner := bufio.NewScanner(corpusData)
	lineCount := 0
	for scanner.Scan() {
		if scanner.Text() != "" {
			lineCount++
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan corpus: %v", err)
	}

	if lineCount != len(events) {
		t.Errorf("expected %d corpus entries, got %d", len(events), lineCount)
	}

	t.Logf("T3 Trace corpus generated: %s (%d entries)", corpusPath, lineCount)

	// Log a summary of verdicts in the corpus for visibility.
	var approveCount, blockCount, reviewCount int
	for _, r := range resultMap {
		switch r.FinalVerdict {
		case types.VerdictApprove:
			approveCount++
		case types.VerdictBlock:
			blockCount++
		case types.VerdictReview:
			reviewCount++
		}
	}
	t.Logf("Corpus verdict distribution: approve=%d, block=%d, review=%d", approveCount, blockCount, reviewCount)
}
