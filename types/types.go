package types

import "time"

// Event represents an incoming event to be evaluated by the rules engine.
type Event struct {
	EventID   string
	EventType string
	// EntityID is the routing key: the configured routing field from the
	// payload when present, otherwise the event ID. It decides which pod
	// owns the event in cluster mode and which counter keys are affine.
	EntityID   string
	Timestamp  time.Time
	Payload    map[string]any
	RawJSON    []byte
	ReceivedAt time.Time
}

// Verdict is the outcome of a rule or the final decision for an event.
type Verdict string

const (
	VerdictApprove Verdict = "approve"
	VerdictBlock   Verdict = "block"
	VerdictReview  Verdict = "review"
)

// VerdictWeight returns a numeric weight for verdict comparison.
// Higher weight means more severe: block(3) > review(2) > approve(1) > unknown(0).
func VerdictWeight(v Verdict) int {
	switch v {
	case VerdictBlock:
		return 3
	case VerdictReview:
		return 2
	case VerdictApprove:
		return 1
	default:
		return 0
	}
}

// RuleResult captures the outcome of a single rule evaluation.
type RuleResult struct {
	RuleID   string
	Priority int
	Verdict  Verdict
	Reason   string
	// Err is the in-process error; bare errors marshal as "{}", so ErrMsg
	// carries the message for DuckDB rows and webhook bodies.
	Err     error `json:"-"`
	ErrMsg  string
	Elapsed time.Duration
}

// Result captures the final outcome after all rules have been evaluated for an event.
type Result struct {
	EventID        string
	EventType      string
	FinalVerdict   Verdict
	TriggeredRules []RuleResult
	FailedRules    []RuleResult
	Payload        map[string]any
	// RawPayload is the original event JSON as received. When set, sinks use
	// it directly instead of re-marshalling Payload. Excluded from the
	// webhook body, which already carries Payload.
	RawPayload  []byte `json:"-"`
	LatencyUS   int64
	ProcessedAt time.Time
}
