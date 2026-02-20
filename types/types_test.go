package types

import "testing"

func TestVerdictWeight_Ordering(t *testing.T) {
	block := VerdictWeight(VerdictBlock)
	review := VerdictWeight(VerdictReview)
	approve := VerdictWeight(VerdictApprove)
	unknown := VerdictWeight(Verdict("something_else"))

	if block != 3 {
		t.Errorf("expected block=3, got %d", block)
	}
	if review != 2 {
		t.Errorf("expected review=2, got %d", review)
	}
	if approve != 1 {
		t.Errorf("expected approve=1, got %d", approve)
	}
	if unknown != 0 {
		t.Errorf("expected unknown=0, got %d", unknown)
	}

	if !(block > review && review > approve && approve > unknown) {
		t.Errorf("ordering violated: block(%d) > review(%d) > approve(%d) > unknown(%d)",
			block, review, approve, unknown)
	}
}
