package integration_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/vinaysrao1/fruitfly/types"
)

// busyBlockRule blocks every event after a short busy loop. The loop makes
// evaluation non-trivial so events queued at shutdown are genuinely drained
// rather than already processed, and so a cancelled context (the old bug)
// would reliably abort evaluation mid-rule.
const busyBlockRule = `
rule_id = "busy-block"
event_type = "*"
priority = 100
def evaluate(event):
    x = 0
    for i in range(20000):
        x += 1
    return verdict("block", reason="always")
`

// TestShutdown_DrainedEventsEvaluateNormally is the regression test for the
// shutdown-ordering bug: the old sequence cancelled the shared context
// before draining eventChan, so every queued event's rules aborted under a
// dead context, landed in FailedRules, and the verdict fell open to approve
// — falsified verdicts persisted on every restart. With the fixed ordering
// (reloader cancel + channel close, pipeline context kept live) every
// drained event must produce its true verdict.
func TestShutdown_DrainedEventsEvaluateNormally(t *testing.T) {
	tp := newTestPipeline(t, busyBlockRule, defaultOpts(), "")
	cancel, poolDone, writerDone := tp.start(t)

	// Queue events directly so many are still in the channel when shutdown
	// begins (the busy loop keeps workers behind the producer).
	const numEvents = 50
	for i := 0; i < numEvents; i++ {
		tp.eventChan <- types.Event{
			EventID:    fmt.Sprintf("drain-%03d", i),
			EventType:  "post",
			Timestamp:  time.Now(),
			Payload:    map[string]any{"n": float64(i)},
			ReceivedAt: time.Now(),
		}
	}

	// Shut down immediately; queued events must drain with correct verdicts.
	tp.shutdown(t, cancel, poolDone, writerDone)

	rows := tp.queryDB(t)
	if len(rows) != numEvents {
		t.Fatalf("expected %d rows after drain, got %d", numEvents, len(rows))
	}
	for _, row := range rows {
		if row.Verdict != "block" {
			t.Errorf("event %s: verdict = %q, want block (drained events must not fail open)",
				row.EventID, row.Verdict)
		}
		if row.TriggeredRulesJSON == "[]" || row.TriggeredRulesJSON == "" {
			t.Errorf("event %s: no triggered rules recorded (rule did not run to completion)",
				row.EventID)
		}
	}
}
