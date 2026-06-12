package rules

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newTestCompiler() *Compiler {
	return &Compiler{UDFs: DefaultUDFs()}
}

func TestCompileSource_ValidRule(t *testing.T) {
	c := newTestCompiler()
	src, err := os.ReadFile("testdata/valid_rule.star")
	if err != nil {
		t.Fatal(err)
	}

	rule, err := c.CompileSource("valid_rule.star", string(src))
	if err != nil {
		t.Fatalf("CompileSource failed: %v", err)
	}

	if rule.RuleID != "test-valid-rule" {
		t.Errorf("RuleID = %q, want %q", rule.RuleID, "test-valid-rule")
	}
	if rule.EventType != "post" {
		t.Errorf("EventType = %q, want %q", rule.EventType, "post")
	}
	if rule.Priority != 100 {
		t.Errorf("Priority = %d, want %d", rule.Priority, 100)
	}
	if rule.Program == nil {
		t.Error("Program is nil")
	}
}

func TestCompileDir_SortedByPriority(t *testing.T) {
	c := newTestCompiler()
	dir := t.TempDir()
	copyTestFile(t, "testdata/high_priority.star", dir)
	copyTestFile(t, "testdata/low_priority.star", dir)
	copyTestFile(t, "testdata/mid_priority.star", dir)

	snap, err := c.CompileDir(dir)
	if err != nil {
		t.Fatalf("CompileDir failed: %v", err)
	}

	if len(snap.Rules) != 3 {
		t.Fatalf("expected 3 rules, got %d", len(snap.Rules))
	}

	for i := 1; i < len(snap.Rules); i++ {
		if snap.Rules[i-1].Priority < snap.Rules[i].Priority {
			t.Errorf("rules not sorted: rules[%d].Priority=%d < rules[%d].Priority=%d",
				i-1, snap.Rules[i-1].Priority, i, snap.Rules[i].Priority)
		}
	}

	if snap.Rules[0].Priority != 200 || snap.Rules[1].Priority != 100 || snap.Rules[2].Priority != 50 {
		t.Errorf("unexpected priorities: %d, %d, %d",
			snap.Rules[0].Priority, snap.Rules[1].Priority, snap.Rules[2].Priority)
	}

	if snap.ID == "" {
		t.Error("snapshot ID is empty")
	}
	if snap.LoadedAt.IsZero() {
		t.Error("snapshot LoadedAt is zero")
	}
}

func TestCompileDir_DuplicateRuleID_Error(t *testing.T) {
	c := newTestCompiler()
	dir := t.TempDir()

	writeFile(t, filepath.Join(dir, "rule_a.star"), `
rule_id = "duplicate-id"
event_type = "post"
priority = 100
def evaluate(event):
    return verdict("approve")
`)
	writeFile(t, filepath.Join(dir, "rule_b.star"), `
rule_id = "duplicate-id"
event_type = "comment"
priority = 50
def evaluate(event):
    return verdict("approve")
`)

	_, err := c.CompileDir(dir)
	if err == nil {
		t.Fatal("expected error for duplicate rule_id, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("error should mention 'duplicate', got: %v", err)
	}
	if !strings.Contains(err.Error(), "duplicate-id") {
		t.Errorf("error should mention the rule_id, got: %v", err)
	}
}

func TestCompileSource_SyntaxError(t *testing.T) {
	c := newTestCompiler()
	src, err := os.ReadFile("testdata/syntax_error.star")
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.CompileSource("syntax_error.star", string(src))
	if err == nil {
		t.Fatal("expected syntax error, got nil")
	}
	if !strings.Contains(err.Error(), "syntax_error.star") {
		t.Errorf("error should contain filename, got: %v", err)
	}
}

func TestCompileSource_MissingEvaluate(t *testing.T) {
	c := newTestCompiler()
	src, err := os.ReadFile("testdata/missing_evaluate.star")
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.CompileSource("missing_evaluate.star", string(src))
	if err == nil {
		t.Fatal("expected error for missing evaluate, got nil")
	}
	if !strings.Contains(err.Error(), "evaluate") {
		t.Errorf("error should mention 'evaluate', got: %v", err)
	}
}

func TestCompileDir_EmptyDir(t *testing.T) {
	c := newTestCompiler()
	dir := t.TempDir()

	snap, err := c.CompileDir(dir)
	if err != nil {
		t.Fatalf("expected no error for empty dir, got: %v", err)
	}
	if len(snap.Rules) != 0 {
		t.Errorf("expected 0 rules, got %d", len(snap.Rules))
	}
	if snap.ID == "" {
		t.Error("snapshot ID is empty even for empty dir")
	}
}

func TestRulesForEvent_FilterAndWildcard(t *testing.T) {
	c := newTestCompiler()
	dir := t.TempDir()

	copyTestFile(t, "testdata/high_priority.star", dir) // event_type = "post"
	copyTestFile(t, "testdata/mid_priority.star", dir)  // event_type = "comment"
	copyTestFile(t, "testdata/wildcard_rule.star", dir) // event_type = "*"

	snap, err := c.CompileDir(dir)
	if err != nil {
		t.Fatalf("CompileDir failed: %v", err)
	}

	// Query for "post" events: should get "post" + "*" rules.
	matched := snap.RulesForEvent("post")
	if len(matched) != 2 {
		t.Fatalf("expected 2 rules for 'post', got %d", len(matched))
	}
	ruleIDs := map[string]bool{}
	for _, r := range matched {
		ruleIDs[r.RuleID] = true
	}
	if !ruleIDs["high-priority"] {
		t.Error("missing 'high-priority' rule for 'post' event")
	}
	if !ruleIDs["wildcard-rule"] {
		t.Error("missing 'wildcard-rule' for 'post' event")
	}
	if ruleIDs["mid-priority"] {
		t.Error("'mid-priority' (comment) should not match 'post' event")
	}

	// Query for "comment": should get "comment" + "*" rules.
	matched = snap.RulesForEvent("comment")
	if len(matched) != 2 {
		t.Fatalf("expected 2 rules for 'comment', got %d", len(matched))
	}

	// Query for "unknown": should get only wildcard.
	matched = snap.RulesForEvent("unknown")
	if len(matched) != 1 {
		t.Fatalf("expected 1 rule for 'unknown', got %d", len(matched))
	}
	if matched[0].RuleID != "wildcard-rule" {
		t.Errorf("expected wildcard-rule, got %s", matched[0].RuleID)
	}
}

func TestReloader_FileChange_UpdatesSnapshot(t *testing.T) {
	c := newTestCompiler()
	dir := t.TempDir()

	writeFile(t, filepath.Join(dir, "rule.star"), `
rule_id = "mutable-rule"
event_type = "post"
priority = 100
def evaluate(event):
    return verdict("approve")
`)

	var snap atomic.Pointer[Snapshot]
	reloader, err := NewReloader(c, dir, &snap)
	if err != nil {
		t.Fatalf("NewReloader failed: %v", err)
	}

	initialSnap := snap.Load()
	if initialSnap == nil {
		t.Fatal("initial snapshot is nil")
	}
	initialID := initialSnap.ID

	if !reloader.Ready.Load() {
		t.Error("reloader should be ready after initial load")
	}

	// Modify the rule file.
	writeFile(t, filepath.Join(dir, "rule.star"), `
rule_id = "mutable-rule"
event_type = "post"
priority = 200
def evaluate(event):
    return verdict("block", reason="updated")
`)

	// Trigger manual reload.
	reloader.Reload()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		_ = reloader.Run(ctx)
		close(done)
	}()

	// Wait for snapshot to change.
	deadline := time.After(3 * time.Second)
	for {
		newSnap := snap.Load()
		if newSnap != nil && newSnap.ID != initialID {
			if len(newSnap.Rules) != 1 {
				t.Fatalf("expected 1 rule, got %d", len(newSnap.Rules))
			}
			if newSnap.Rules[0].Priority != 200 {
				t.Errorf("expected priority 200, got %d", newSnap.Rules[0].Priority)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for snapshot update")
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	cancel()
	<-done
}

func TestReloader_BadReload_PreservesOldSnapshot(t *testing.T) {
	c := newTestCompiler()
	dir := t.TempDir()

	writeFile(t, filepath.Join(dir, "rule.star"), `
rule_id = "stable-rule"
event_type = "post"
priority = 100
def evaluate(event):
    return verdict("approve")
`)

	var snap atomic.Pointer[Snapshot]
	reloader, err := NewReloader(c, dir, &snap)
	if err != nil {
		t.Fatalf("NewReloader failed: %v", err)
	}

	initialSnap := snap.Load()
	if initialSnap == nil {
		t.Fatal("initial snapshot is nil")
	}

	// Replace with broken syntax.
	writeFile(t, filepath.Join(dir, "rule.star"), `
rule_id = "broken"
def evaluate(event)
    return verdict("approve")
`)

	// Trigger manual reload.
	reloader.Reload()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		_ = reloader.Run(ctx)
		close(done)
	}()

	// Give the reloader time to process the reload signal.
	time.Sleep(500 * time.Millisecond)

	// Snapshot should still be the original.
	currentSnap := snap.Load()
	if currentSnap.ID != initialSnap.ID {
		t.Errorf("snapshot should be preserved on bad reload: got ID %s, want %s",
			currentSnap.ID, initialSnap.ID)
	}
	if len(currentSnap.Rules) != 1 {
		t.Errorf("expected 1 rule preserved, got %d", len(currentSnap.Rules))
	}
	if currentSnap.Rules[0].RuleID != "stable-rule" {
		t.Errorf("expected rule 'stable-rule', got %q", currentSnap.Rules[0].RuleID)
	}

	cancel()
	<-done
}

// TestReloader_UnchangedContent_KeepsSnapshot: a reload with unchanged rule
// content must not republish — republishing would mint a new snapshot ID and
// needlessly invalidate every worker's eval cache.
func TestReloader_UnchangedContent_KeepsSnapshot(t *testing.T) {
	c := newTestCompiler()
	dir := t.TempDir()

	writeFile(t, filepath.Join(dir, "rule.star"), `
rule_id = "steady-rule"
event_type = "post"
priority = 100
def evaluate(event):
    return verdict("approve")
`)

	var snap atomic.Pointer[Snapshot]
	reloader, err := NewReloader(c, dir, &snap)
	if err != nil {
		t.Fatalf("NewReloader: %v", err)
	}
	initialID := snap.Load().ID

	// Trigger a manual reload with no content change.
	reloader.Reload()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = reloader.Run(ctx)
		close(done)
	}()

	time.Sleep(500 * time.Millisecond)
	if got := snap.Load().ID; got != initialID {
		t.Errorf("snapshot ID changed on unchanged content: %s -> %s", initialID, got)
	}

	// A real content change must still publish a new snapshot.
	writeFile(t, filepath.Join(dir, "rule.star"), `
rule_id = "steady-rule"
event_type = "post"
priority = 200
def evaluate(event):
    return verdict("approve")
`)
	reloader.Reload()

	deadline := time.After(3 * time.Second)
	for snap.Load().ID == initialID {
		select {
		case <-deadline:
			t.Fatal("snapshot did not update after content change")
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}

	cancel()
	<-done
}

// --- Helpers ---

func copyTestFile(t *testing.T, src, dstDir string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(dstDir, filepath.Base(src))
	if err := os.WriteFile(dst, data, 0644); err != nil {
		t.Fatal(err)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

// TestCompileDir_ContentCacheReusesUnchangedRules: recompiling after a
// one-file edit reuses the compiled rule (same shared Prefiltered counter)
// for unchanged files and recompiles only the changed one.
func TestCompileDir_ContentCacheReusesUnchangedRules(t *testing.T) {
	c := newTestCompiler()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a.star"), `
rule_id = "rule-a"
event_type = "post"
priority = 100
def evaluate(event):
    return verdict("approve")
`)
	writeFile(t, filepath.Join(dir, "b.star"), `
rule_id = "rule-b"
event_type = "post"
priority = 50
def evaluate(event):
    return verdict("approve")
`)

	snap1, err := c.CompileDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	counters1 := map[string]*atomic.Int64{}
	for i := range snap1.Rules {
		counters1[snap1.Rules[i].RuleID] = snap1.Rules[i].Prefiltered
	}

	// Edit only b.star.
	writeFile(t, filepath.Join(dir, "b.star"), `
rule_id = "rule-b"
event_type = "post"
priority = 60
def evaluate(event):
    return verdict("approve")
`)
	snap2, err := c.CompileDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := range snap2.Rules {
		r := &snap2.Rules[i]
		switch r.RuleID {
		case "rule-a":
			if r.Prefiltered != counters1["rule-a"] {
				t.Error("unchanged rule-a was recompiled (Prefiltered counter not shared)")
			}
		case "rule-b":
			if r.Prefiltered == counters1["rule-b"] {
				t.Error("changed rule-b was not recompiled")
			}
			if r.Priority != 60 {
				t.Errorf("rule-b priority = %d, want 60", r.Priority)
			}
		}
	}
}

// BenchmarkCompileDir500 measures cold compilation of 500 rules.
func BenchmarkCompileDir500(b *testing.B) {
	dir := b.TempDir()
	for i := 0; i < 500; i++ {
		src := fmt.Sprintf("rule_id = \"r-%d\"\nevent_type = \"t-%d\"\npriority = %d\ndef evaluate(event):\n    return verdict(\"approve\")\n", i, i%10, i)
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("r%03d.star", i)), []byte(src), 0o644); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := newTestCompiler() // fresh compiler: cold cache
		if _, err := c.CompileDir(dir); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRecompileOneChanged500 measures a warm reload after one file
// changes out of 500 — the content cache should make this delta-cost.
func BenchmarkRecompileOneChanged500(b *testing.B) {
	dir := b.TempDir()
	for i := 0; i < 500; i++ {
		src := fmt.Sprintf("rule_id = \"r-%d\"\nevent_type = \"t-%d\"\npriority = %d\ndef evaluate(event):\n    return verdict(\"approve\")\n", i, i%10, i)
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("r%03d.star", i)), []byte(src), 0o644); err != nil {
			b.Fatal(err)
		}
	}
	c := newTestCompiler()
	if _, err := c.CompileDir(dir); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src := fmt.Sprintf("rule_id = \"r-0\"\nevent_type = \"t-0\"\npriority = %d\ndef evaluate(event):\n    return verdict(\"approve\")\n", 1000+i)
		if err := os.WriteFile(filepath.Join(dir, "r000.star"), []byte(src), 0o644); err != nil {
			b.Fatal(err)
		}
		if _, err := c.CompileDir(dir); err != nil {
			b.Fatal(err)
		}
	}
}
