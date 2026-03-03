package executor

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vinaysrao1/fruitfly/rules"
	"github.com/vinaysrao1/fruitfly/types"
)

// loadProductionRules compiles the production rule set from jetstream_test/rules.
func loadProductionRules(t *testing.T) *rules.Snapshot {
	t.Helper()
	c := &rules.Compiler{UDFs: rules.DefaultUDFs()}
	snap, err := c.CompileDir("../jetstream_test/rules")
	if err != nil {
		t.Fatalf("CompileDir: %v", err)
	}
	return snap
}

// likeEvent builds a like event with the given entity_id.
func likeEvent(id int, entityID string) types.Event {
	return types.Event{
		EventID:    fmt.Sprintf("like-%d", id),
		EventType:  "like",
		Timestamp:  time.Now(),
		Payload:    map[string]any{"entity_id": entityID},
		ReceivedAt: time.Now(),
	}
}

// postEvent builds a post event with the given payload fields.
func postEvent(id int, entityID, text string, charCount int) types.Event {
	return types.Event{
		EventID:   fmt.Sprintf("post-%d", id),
		EventType: "post",
		Timestamp: time.Now(),
		Payload: map[string]any{
			"entity_id":  entityID,
			"text":       text,
			"char_count": charCount,
		},
		ReceivedAt: time.Now(),
	}
}

// runEvents sends events through a pool with a fresh 1-worker pool using the given snapshot.
func runEvents(t *testing.T, snap *rules.Snapshot, events []types.Event) []types.Result {
	t.Helper()
	pool, _ := makePool(snap, 1)

	in := make(chan types.Event, len(events))
	out := make(chan types.Result, len(events))
	for _, e := range events {
		in <- e
	}
	close(in)
	pool.Run(context.Background(), in, out)

	results := make([]types.Result, 0, len(events))
	for r := range out {
		results = append(results, r)
	}
	return results
}

// TestSpamLikeFlood_ThresholdBehavior verifies the spam_like_flood rule:
//   - events 1-5 from the same entity: approve
//   - event 6 from the same entity: block with reason containing "spam: like flood"
func TestSpamLikeFlood_ThresholdBehavior(t *testing.T) {
	snap := loadProductionRules(t)

	// Build 6 like events from the same entity.
	const numEvents = 6
	events := make([]types.Event, numEvents)
	for i := 0; i < numEvents; i++ {
		events[i] = likeEvent(i, "user-flood-1")
	}

	results := runEvents(t, snap, events)

	if len(results) != numEvents {
		t.Fatalf("expected %d results, got %d", numEvents, len(results))
	}

	for i, r := range results {
		if i < 5 {
			// Events 1-5 (index 0-4): counter returns 1..5, not > 5 -> approve
			if r.FinalVerdict != types.VerdictApprove {
				t.Errorf("event %d: expected approve (count=%d <= 5), got %s", i+1, i+1, r.FinalVerdict)
			}
		} else {
			// Event 6 (index 5): counter returns 6 > 5 -> block
			if r.FinalVerdict != types.VerdictBlock {
				t.Errorf("event %d: expected block (count=6 > 5), got %s", i+1, r.FinalVerdict)
			}
			found := false
			for _, rr := range r.TriggeredRules {
				if strings.Contains(rr.Reason, "spam: like flood") {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("event %d: expected reason containing 'spam: like flood', triggered_rules: %+v", i+1, r.TriggeredRules)
			}
		}
	}
}

// TestSpamLikeFlood_DifferentEntities verifies that entities don't share counters.
func TestSpamLikeFlood_DifferentEntities(t *testing.T) {
	snap := loadProductionRules(t)

	// 6 events alternating between two different users. Each user only sends 3 events.
	events := []types.Event{
		likeEvent(1, "entity-A"),
		likeEvent(2, "entity-B"),
		likeEvent(3, "entity-A"),
		likeEvent(4, "entity-B"),
		likeEvent(5, "entity-A"),
		likeEvent(6, "entity-B"),
	}

	results := runEvents(t, snap, events)

	if len(results) != 6 {
		t.Fatalf("expected 6 results, got %d", len(results))
	}

	// All should be approve since no entity exceeds 5 likes.
	for i, r := range results {
		if r.FinalVerdict != types.VerdictApprove {
			t.Errorf("event %d (entity alternating): expected approve (only 3 per entity), got %s", i+1, r.FinalVerdict)
		}
	}
}

// TestSpamLikeFlood_ApproveBelow5 verifies the approve path: exactly 5 likes -> still approve.
func TestSpamLikeFlood_ApproveBelow5(t *testing.T) {
	snap := loadProductionRules(t)

	events := make([]types.Event, 5)
	for i := 0; i < 5; i++ {
		events[i] = likeEvent(i, "user-exactly-5")
	}

	results := runEvents(t, snap, events)

	for i, r := range results {
		if r.FinalVerdict != types.VerdictApprove {
			t.Errorf("event %d: expected approve (count=%d <= 5), got %s", i+1, i+1, r.FinalVerdict)
		}
	}
}

// TestSpamShortPost_CharCountThreshold verifies the spam_short_post rule:
//   - char_count >= 10: always approve regardless of post count
//   - char_count < 10, entity <= 5 posts: approve
//   - char_count < 10, entity > 5 posts: block with "spam: short post flood"
func TestSpamShortPost_CharCountThreshold(t *testing.T) {
	snap := loadProductionRules(t)

	// Long posts (char_count >= 10) should always approve.
	events := []types.Event{
		postEvent(1, "user-long-posts", "hello world!", 12),
		postEvent(2, "user-long-posts", "this is a long post", 20),
	}

	results := runEvents(t, snap, events)

	for i, r := range results {
		if r.FinalVerdict != types.VerdictApprove {
			t.Errorf("long post event %d: expected approve (char_count >= 10), got %s", i+1, r.FinalVerdict)
		}
	}
}

// TestSpamShortPost_ShortPostBelowFloodThreshold verifies short posts under limit approve.
func TestSpamShortPost_ShortPostBelowFloodThreshold(t *testing.T) {
	snap := loadProductionRules(t)

	// 5 short posts from the same entity: all should approve (count <= 5).
	events := make([]types.Event, 5)
	for i := 0; i < 5; i++ {
		events[i] = postEvent(i, "user-short-ok", "hi", 2)
	}

	results := runEvents(t, snap, events)

	for i, r := range results {
		if r.FinalVerdict != types.VerdictApprove {
			t.Errorf("short post event %d: expected approve (count=%d <= 5), got %s", i+1, i+1, r.FinalVerdict)
		}
	}
}

// TestSpamShortPost_ShortPostFloodBlocked verifies that > 5 short posts from same entity blocks.
func TestSpamShortPost_ShortPostFloodBlocked(t *testing.T) {
	snap := loadProductionRules(t)

	// 6 short posts from the same entity: 6th should block.
	const numPosts = 6
	events := make([]types.Event, numPosts)
	for i := 0; i < numPosts; i++ {
		events[i] = postEvent(i, "user-short-spammer", "hi", 2)
	}

	results := runEvents(t, snap, events)

	if len(results) != numPosts {
		t.Fatalf("expected %d results, got %d", numPosts, len(results))
	}

	for i, r := range results {
		if i < 5 {
			if r.FinalVerdict != types.VerdictApprove {
				t.Errorf("short post event %d: expected approve, got %s", i+1, r.FinalVerdict)
			}
		} else {
			if r.FinalVerdict != types.VerdictBlock {
				t.Errorf("short post event %d: expected block (count=6 > 5), got %s", i+1, r.FinalVerdict)
			}
			found := false
			for _, rr := range r.TriggeredRules {
				if strings.Contains(rr.Reason, "spam: short post flood") {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("event %d: expected reason containing 'spam: short post flood', triggered: %+v", i+1, r.TriggeredRules)
			}
		}
	}
}

// loadNumericContentRule compiles only the numeric_content rule for isolated testing.
// This avoids priority interference from other rules (e.g., spam_short_post at priority 100).
func loadNumericContentRule(t *testing.T) *rules.Snapshot {
	t.Helper()
	c := &rules.Compiler{UDFs: rules.DefaultUDFs()}
	rule, err := c.CompileSource("numeric_content.star", `
rule_id = "numeric-content"
event_type = "post"
priority = 90

def evaluate(event):
    text = event["payload"].get("text", "")

    numeric_count = 0
    for ch in text.elems():
        if ch >= "0" and ch <= "9":
            numeric_count += 1

    if numeric_count > 10:
        return verdict("review", reason="high numeric content: " + str(numeric_count) + " digits")

    return verdict("approve")
`)
	if err != nil {
		t.Fatalf("CompileSource(numeric_content): %v", err)
	}
	return &rules.Snapshot{
		ID:       "numeric-content-test",
		Rules:    []rules.Rule{*rule},
		LoadedAt: time.Now(),
	}
}

// TestNumericContent_BelowThreshold verifies the numeric_content rule:
// text with <= 10 digits -> approve.
// Tests run with the numeric_content rule in isolation to avoid priority interference.
func TestNumericContent_BelowThreshold(t *testing.T) {
	snap := loadNumericContentRule(t)

	tests := []struct {
		name      string
		text      string
		charCount int
	}{
		{"no digits", "hello world", 11},
		{"exactly 10 digits", "abc 1234567890 xyz", 18},
		{"0 digits", "just text here no numbers", 25},
		{"5 digits", "abc12345xyz", 11},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			event := postEvent(1, "user-numeric", tc.text, tc.charCount)
			results := runEvents(t, snap, []types.Event{event})

			if len(results) != 1 {
				t.Fatalf("expected 1 result, got %d", len(results))
			}
			r := results[0]
			if r.FinalVerdict != types.VerdictApprove {
				t.Errorf("text %q: expected approve (<= 10 digits), got %s", tc.text, r.FinalVerdict)
			}
		})
	}
}

// TestNumericContent_AboveThreshold verifies that > 10 digits triggers a review verdict.
// Tests run with the numeric_content rule in isolation to avoid priority interference.
func TestNumericContent_AboveThreshold(t *testing.T) {
	snap := loadNumericContentRule(t)

	tests := []struct {
		name      string
		text      string
		charCount int
	}{
		{"11 digits", "abc 12345678901 xyz", 19},
		{"all digits", "12345678901234567890", 20},
		{"mixed heavy numeric", "call 1234567890123 for info", 27},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			event := postEvent(1, "user-numeric-heavy", tc.text, tc.charCount)
			results := runEvents(t, snap, []types.Event{event})

			if len(results) != 1 {
				t.Fatalf("expected 1 result, got %d", len(results))
			}
			r := results[0]
			if r.FinalVerdict != types.VerdictReview {
				t.Errorf("text %q: expected review (> 10 digits), got %s", tc.text, r.FinalVerdict)
			}
			found := false
			for _, rr := range r.TriggeredRules {
				if strings.Contains(rr.Reason, "high numeric content") {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("text %q: expected reason containing 'high numeric content', triggered: %+v", tc.text, r.TriggeredRules)
			}
		})
	}
}

// TestNumericContent_WithProductionRules verifies interaction between numeric_content (priority 90)
// and spam_short_post (priority 100) in the full production rule set.
// When char_count >= 10, spam_short_post (priority 100) approves, overriding numeric_content's review.
func TestNumericContent_WithProductionRules_PriorityInteraction(t *testing.T) {
	snap := loadProductionRules(t)

	// A post with > 10 digits but char_count >= 10:
	// - spam_short_post (priority 100): char_count=20 >= 10 -> approve
	// - numeric_content (priority 90): > 10 digits -> review
	// Final verdict: approve (spam_short_post has higher priority)
	event := postEvent(1, "user-priority-test", "12345678901234567890", 20)
	results := runEvents(t, snap, []types.Event{event})

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// With full production rules: spam_short_post (p=100) wins over numeric_content (p=90).
	// The test documents the priority interaction rather than requiring review.
	r := results[0]
	t.Logf("Priority interaction: spam_short_post(100) approve overrides numeric_content(90) review: verdict=%s", r.FinalVerdict)
	// spam_short_post approves (char_count >= 10), winning over numeric_content's review.
	if r.FinalVerdict != types.VerdictApprove {
		t.Errorf("expected approve (spam_short_post priority 100 wins), got %s", r.FinalVerdict)
	}
}

// TestCatchall_AlwaysApprove verifies that the catchall rule approves any event type.
// Since catchall has priority 1, it is overridden by higher-priority rules.
// Test it in isolation with a snapshot containing only the catchall rule.
func TestCatchall_AlwaysApprove(t *testing.T) {
	c := &rules.Compiler{UDFs: rules.DefaultUDFs()}
	rule, err := c.CompileSource("catchall.star", `
rule_id = "catchall-approve"
event_type = "*"
priority = 1

def evaluate(event):
    return verdict("approve")
`)
	if err != nil {
		t.Fatalf("CompileSource: %v", err)
	}
	snap := &rules.Snapshot{
		ID:       "catchall-test",
		Rules:    []rules.Rule{*rule},
		LoadedAt: time.Now(),
	}

	tests := []struct {
		name      string
		eventType string
	}{
		{"post", "post"},
		{"like", "like"},
		{"comment", "comment"},
		{"unknown_type", "unknown_type"},
		{"empty", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool, _ := makePool(snap, 1)
			event := types.Event{
				EventID:    fmt.Sprintf("catchall-%s", tc.name),
				EventType:  tc.eventType,
				Timestamp:  time.Now(),
				Payload:    map[string]any{},
				ReceivedAt: time.Now(),
			}
			result := runSingleEvent(t, pool, event)
			if result.FinalVerdict != types.VerdictApprove {
				t.Errorf("event_type=%q: expected approve from catchall, got %s", tc.eventType, result.FinalVerdict)
			}
		})
	}
}

// TestProductionRules_TableDriven is a comprehensive table-driven test for all production rules.
func TestProductionRules_TableDriven(t *testing.T) {
	snap := loadProductionRules(t)

	tests := []struct {
		name            string
		events          []types.Event
		expectedVerdicts []types.Verdict
		expectedReasons  []string // substring to find in triggered rule reasons (empty = no check)
	}{
		{
			name: "like: single like always approve",
			events: []types.Event{
				likeEvent(1, "user-single"),
			},
			expectedVerdicts: []types.Verdict{types.VerdictApprove},
		},
		{
			name: "post: long text always approve",
			events: []types.Event{
				postEvent(1, "user-long", "This is a long post with many characters", 40),
			},
			expectedVerdicts: []types.Verdict{types.VerdictApprove},
		},
		{
			name: "post: digits exactly 10 approve",
			events: []types.Event{
				postEvent(1, "user-digits", "1234567890", 10),
			},
			expectedVerdicts: []types.Verdict{types.VerdictApprove},
		},
		// Note: In the full production rule set, numeric_content (priority 90) is
		// overridden by spam_short_post (priority 100) for post events with char_count >= 10.
		// Testing numeric_content's review behavior requires an isolated rule set.
		// See TestNumericContent_AboveThreshold for the isolated test.
		{
			name: "post: digits exactly 10 approve (all rules)",
			events: []types.Event{
				postEvent(1, "user-digits-full", "1234567890", 10),
			},
			expectedVerdicts: []types.Verdict{types.VerdictApprove},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Use a fresh pool for each sub-test to avoid counter cross-contamination.
			pool, _ := makePool(snap, 1)
			in := make(chan types.Event, len(tc.events))
			out := make(chan types.Result, len(tc.events))
			for _, e := range tc.events {
				in <- e
			}
			close(in)
			pool.Run(context.Background(), in, out)

			var results []types.Result
			for r := range out {
				results = append(results, r)
			}

			if len(results) != len(tc.expectedVerdicts) {
				t.Fatalf("expected %d results, got %d", len(tc.expectedVerdicts), len(results))
			}

			for i, r := range results {
				if r.FinalVerdict != tc.expectedVerdicts[i] {
					t.Errorf("result[%d]: expected %s, got %s", i, tc.expectedVerdicts[i], r.FinalVerdict)
				}
				if i < len(tc.expectedReasons) && tc.expectedReasons[i] != "" {
					found := false
					for _, rr := range r.TriggeredRules {
						if strings.Contains(rr.Reason, tc.expectedReasons[i]) {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("result[%d]: expected reason containing %q, triggered: %+v",
							i, tc.expectedReasons[i], r.TriggeredRules)
					}
				}
			}
		})
	}
}
