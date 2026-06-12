package rules

import (
	"log/slog"
	"math"
	"slices"
	"sync/atomic"
	"time"
)

// ewmaAlpha is the EWMA smoothing factor — dimensionless, not a latency.
// At 0.05, ~the last 40 evaluations dominate: a single random spike barely
// moves the average; sustained slowness moves it within dozens of events.
const ewmaAlpha = 0.05

// minEvalsForSlow is how many samples a rule needs before slow-detection
// applies, so a cold rule's first (often slow) evaluations don't flag it.
const minEvalsForSlow = 10

// slowFactor flags a rule whose average is this many times the median of
// its peers — scale-free: meaningful on any hardware at any baseline.
const slowFactor = 16

// warnInterval rate-limits per-rule slow warnings.
const warnInterval = 60 // seconds

// RuleStats tracks a rule's evaluation timing. Shared by every copy of a
// Rule (and across snapshots for unchanged rules, like Prefiltered), and
// safe for concurrent update from any worker.
type RuleStats struct {
	evals    atomic.Int64
	ewmaBits atomic.Uint64 // math.Float64bits of the EWMA in nanoseconds
	lastWarn atomic.Int64  // unix seconds of the last slow warning
}

// Observe folds one evaluation duration into the rule's moving average.
func (s *RuleStats) Observe(elapsed time.Duration) {
	s.evals.Add(1)
	sample := float64(elapsed.Nanoseconds())
	for {
		old := s.ewmaBits.Load()
		prev := math.Float64frombits(old)
		next := prev + ewmaAlpha*(sample-prev)
		if old == 0 {
			next = sample // first sample seeds the average
		}
		if s.ewmaBits.CompareAndSwap(old, math.Float64bits(next)) {
			return
		}
	}
}

// EWMA returns the rule's average evaluation time.
func (s *RuleStats) EWMA() time.Duration {
	return time.Duration(math.Float64frombits(s.ewmaBits.Load()))
}

// Evals returns how many evaluations have been observed.
func (s *RuleStats) Evals() int64 { return s.evals.Load() }

// WarnIfSlow logs (rate-limited) when the rule's average exceeds its fair
// share of the event budget — budget ÷ matched rules, derived entirely from
// existing configuration rather than a hardwired latency. Returns whether a
// warning was emitted.
func (s *RuleStats) WarnIfSlow(ruleID string, eventBudget time.Duration, matched int) bool {
	if s.evals.Load() < minEvalsForSlow {
		return false
	}
	if matched < 1 {
		matched = 1
	}
	fairShare := eventBudget / time.Duration(matched)
	ewma := s.EWMA()
	if ewma <= fairShare {
		return false
	}
	now := time.Now().Unix()
	last := s.lastWarn.Load()
	if now-last < warnInterval || !s.lastWarn.CompareAndSwap(last, now) {
		return false
	}
	slog.Warn("slow rule: average evaluation time exceeds its share of the event budget",
		"rule_id", ruleID, "avg", ewma, "budget_share", fairShare)
	return true
}

// SlowRuleThreshold returns the EWMA above which a rule counts as slow
// relative to its peers: slowFactor × the median EWMA of rules with enough
// samples. Returns 0 when fewer than two rules qualify (no meaningful
// peer group).
func SlowRuleThreshold(rs []Rule) time.Duration {
	var ewmas []time.Duration
	for i := range rs {
		if rs[i].Stats != nil && rs[i].Stats.Evals() >= minEvalsForSlow {
			ewmas = append(ewmas, rs[i].Stats.EWMA())
		}
	}
	if len(ewmas) < 2 {
		return 0
	}
	slices.Sort(ewmas)
	return ewmas[len(ewmas)/2] * slowFactor
}
