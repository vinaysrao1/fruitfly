package rules

import (
	"sync"
	"testing"
	"time"
)

func TestRuleStats_ObserveSeedsAndConverges(t *testing.T) {
	var s RuleStats
	if s.EWMA() != 0 || s.Evals() != 0 {
		t.Fatal("zero-value stats must read as zero")
	}
	s.Observe(100 * time.Microsecond)
	if got := s.EWMA(); got != 100*time.Microsecond {
		t.Errorf("first sample must seed the average: got %v", got)
	}
	// Constant input converges to (stays at) the input.
	for i := 0; i < 100; i++ {
		s.Observe(100 * time.Microsecond)
	}
	if got := s.EWMA(); got < 99*time.Microsecond || got > 101*time.Microsecond {
		t.Errorf("EWMA of constant input = %v, want ~100µs", got)
	}
	// One outlier barely moves it (the random-spike property).
	s.Observe(100 * time.Millisecond)
	if got := s.EWMA(); got > 6*time.Millisecond {
		t.Errorf("single spike moved EWMA to %v — should be heavily damped", got)
	}
	if s.Evals() != 102 {
		t.Errorf("Evals = %d, want 102", s.Evals())
	}
}

func TestRuleStats_ConcurrentObserveBounded(t *testing.T) {
	var s RuleStats
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				s.Observe(time.Millisecond)
			}
		}()
	}
	wg.Wait()
	if s.Evals() != 8000 {
		t.Errorf("Evals = %d, want 8000 (no lost observations)", s.Evals())
	}
	if got := s.EWMA(); got < 990*time.Microsecond || got > 1010*time.Microsecond {
		t.Errorf("EWMA = %v, want ~1ms (constant concurrent input)", got)
	}
}

func TestRuleStats_WarnIfSlow(t *testing.T) {
	var s RuleStats
	// Below the sample floor: never warns even when slow.
	s.Observe(time.Second)
	if s.WarnIfSlow("r", time.Second, 10) {
		t.Error("warned below minEvalsForSlow")
	}
	for i := 0; i < minEvalsForSlow; i++ {
		s.Observe(time.Second)
	}
	// Fair share = 1s/10 = 100ms; EWMA ~1s -> warn once, then rate-limited.
	if !s.WarnIfSlow("r", time.Second, 10) {
		t.Error("expected warning for rule exceeding its budget share")
	}
	if s.WarnIfSlow("r", time.Second, 10) {
		t.Error("second warning within the rate-limit window")
	}
	// Fast rule never warns.
	var fast RuleStats
	for i := 0; i < minEvalsForSlow+1; i++ {
		fast.Observe(time.Microsecond)
	}
	if fast.WarnIfSlow("f", time.Second, 10) {
		t.Error("fast rule must not warn")
	}
}

func TestSlowRuleThreshold(t *testing.T) {
	mk := func(d time.Duration, evals int) Rule {
		s := &RuleStats{}
		for i := 0; i < evals; i++ {
			s.Observe(d)
		}
		return Rule{Stats: s}
	}
	// Fewer than two qualified rules: no peer group.
	if got := SlowRuleThreshold([]Rule{mk(time.Millisecond, 20)}); got != 0 {
		t.Errorf("threshold with one rule = %v, want 0", got)
	}
	// Cold rules (below sample floor) are excluded.
	rs := []Rule{mk(time.Millisecond, 20), mk(time.Millisecond, 20), mk(time.Hour, 2), {Stats: &RuleStats{}}}
	got := SlowRuleThreshold(rs)
	want := slowFactor * time.Millisecond
	if got < want-100*time.Microsecond || got > want+100*time.Microsecond {
		t.Errorf("threshold = %v, want ~%v (slowFactor × median of warm rules)", got, want)
	}
	// A genuinely slow warm rule exceeds the threshold; peers don't.
	slow := mk(time.Second, 20)
	if slow.Stats.EWMA() <= got {
		t.Error("slow rule should exceed the peer threshold")
	}
	if rs[0].Stats.EWMA() > got {
		t.Error("normal rule should not exceed the peer threshold")
	}
}
