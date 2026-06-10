// Package main implements the fruitfly replay and shadow mode CLI tools.
//
// Usage:
//
//	# Replay traces against new rules and diff verdicts
//	fruitfly-replay --traces <file> --rules <dir>
//
//	# Shadow mode: compare two rule sets against the same trace corpus
//	fruitfly-replay --traces <file> --old-rules <v1-dir> --new-rules <v2-dir> --shadow
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/vinaysrao1/fruitfly/executor"
	"github.com/vinaysrao1/fruitfly/output"
	"github.com/vinaysrao1/fruitfly/rules"
	"github.com/vinaysrao1/fruitfly/types"
)

// VerdictDiff captures a change in verdict for a single event between two runs.
type VerdictDiff struct {
	EventID     string        `json:"event_id"`
	EventType   string        `json:"event_type"`
	OldVerdict  types.Verdict `json:"old_verdict"`
	NewVerdict  types.Verdict `json:"new_verdict"`
	OldRules    []string      `json:"old_triggered_rules"`
	NewRules    []string      `json:"new_triggered_rules"`
}

// ReplayReport summarizes the result of a replay or shadow comparison.
type ReplayReport struct {
	TotalEvents    int           `json:"total_events"`
	Changed        int           `json:"changed"`
	Unchanged      int           `json:"unchanged"`
	Diffs          []VerdictDiff `json:"diffs"`
	ChangesByRule  map[string]int `json:"changes_by_rule"`
	Duration       time.Duration  `json:"duration_ms"`
}

func main() {
	tracesFile := flag.String("traces", "", "path to JSONL trace file (required)")
	rulesDir := flag.String("rules", "", "path to rules directory for replay")
	oldRulesDir := flag.String("old-rules", "", "path to old rules directory (shadow mode)")
	newRulesDir := flag.String("new-rules", "", "path to new rules directory (shadow mode)")
	shadowMode := flag.Bool("shadow", false, "enable shadow mode (diff two rule sets)")
	workers := flag.Int("workers", 4, "number of worker goroutines")
	flag.Parse()

	if *tracesFile == "" {
		fmt.Fprintln(os.Stderr, "error: --traces is required")
		flag.Usage()
		os.Exit(1)
	}

	if *shadowMode {
		if *oldRulesDir == "" || *newRulesDir == "" {
			fmt.Fprintln(os.Stderr, "error: --old-rules and --new-rules are required in shadow mode")
			flag.Usage()
			os.Exit(1)
		}
		runShadow(*tracesFile, *oldRulesDir, *newRulesDir, *workers)
	} else {
		if *rulesDir == "" {
			fmt.Fprintln(os.Stderr, "error: --rules is required for replay mode")
			flag.Usage()
			os.Exit(1)
		}
		runReplay(*tracesFile, *rulesDir, *workers)
	}
}

// runReplay replays trace events against a single rule set, printing a diff
// against the original verdicts from the trace.
func runReplay(tracesFile, rulesDir string, workers int) {
	entries, err := loadTraces(tracesFile)
	if err != nil {
		slog.Error("load traces", "error", err)
		os.Exit(1)
	}

	snap, err := compileRules(rulesDir)
	if err != nil {
		slog.Error("compile rules", "error", err)
		os.Exit(1)
	}

	start := time.Now()
	results := replayEvents(entries, snap, workers)

	// Diff new results against original verdicts from the trace.
	report := diffAgainstTrace(entries, results)
	report.Duration = time.Since(start)

	printReport(report, "Replay")
}

// runShadow replays the same trace against two rule sets and diffs the verdicts.
func runShadow(tracesFile, oldRulesDir, newRulesDir string, workers int) {
	entries, err := loadTraces(tracesFile)
	if err != nil {
		slog.Error("load traces", "error", err)
		os.Exit(1)
	}

	oldSnap, err := compileRules(oldRulesDir)
	if err != nil {
		slog.Error("compile old rules", "error", err)
		os.Exit(1)
	}

	newSnap, err := compileRules(newRulesDir)
	if err != nil {
		slog.Error("compile new rules", "error", err)
		os.Exit(1)
	}

	start := time.Now()
	oldResults := replayEvents(entries, oldSnap, workers)
	newResults := replayEvents(entries, newSnap, workers)

	report := diffTwoRuns(entries, oldResults, newResults)
	report.Duration = time.Since(start)

	printReport(report, "Shadow Mode")
}

// loadTraces reads JSONL entries from the trace file.
func loadTraces(path string) ([]output.TraceEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open trace file: %w", err)
	}
	defer f.Close()

	var entries []output.TraceEntry
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 10*1024*1024), 10*1024*1024) // 10MB buffer for large events
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		var entry output.TraceEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			return nil, fmt.Errorf("parse trace entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan trace file: %w", err)
	}
	return entries, nil
}

// compileRules compiles all *.star files in dir into a snapshot.
func compileRules(dir string) (*rules.Snapshot, error) {
	compiler := &rules.Compiler{UDFs: rules.DefaultUDFs()}
	return compiler.CompileDir(dir)
}

// replayEvents runs all events from the trace through the given snapshot using a pool.
// Returns a map from event_id to result.
func replayEvents(entries []output.TraceEntry, snap *rules.Snapshot, numWorkers int) map[string]types.Result {
	if len(entries) == 0 {
		return nil
	}

	var snapshotPtr atomic.Pointer[rules.Snapshot]
	snapshotPtr.Store(snap)
	pool := executor.NewPool(numWorkers, &snapshotPtr, 5*time.Second, 1*time.Second)

	in := make(chan types.Event, len(entries))
	out := make(chan types.Result, len(entries))

	for _, entry := range entries {
		in <- entry.Event
	}
	close(in)

	pool.Run(context.Background(), in, out)

	results := make(map[string]types.Result, len(entries))
	for r := range out {
		results[r.EventID] = r
	}
	return results
}

// diffAgainstTrace compares new results against the original verdicts in the trace.
func diffAgainstTrace(entries []output.TraceEntry, newResults map[string]types.Result) ReplayReport {
	report := ReplayReport{
		TotalEvents:   len(entries),
		ChangesByRule: make(map[string]int),
	}

	for _, entry := range entries {
		newResult, ok := newResults[entry.Event.EventID]
		if !ok {
			continue
		}

		originalVerdict := entry.Result.FinalVerdict
		newVerdict := newResult.FinalVerdict

		if originalVerdict == newVerdict {
			report.Unchanged++
			continue
		}

		report.Changed++

		oldRuleIDs := ruleIDs(entry.Result.TriggeredRules)
		newRuleIDs := ruleIDs(newResult.TriggeredRules)

		diff := VerdictDiff{
			EventID:    entry.Event.EventID,
			EventType:  entry.Event.EventType,
			OldVerdict: originalVerdict,
			NewVerdict: newVerdict,
			OldRules:   oldRuleIDs,
			NewRules:   newRuleIDs,
		}
		report.Diffs = append(report.Diffs, diff)

		for _, ruleID := range newRuleIDs {
			report.ChangesByRule[ruleID]++
		}
	}

	return report
}

// diffTwoRuns compares results from two rule sets applied to the same trace.
func diffTwoRuns(entries []output.TraceEntry, oldResults, newResults map[string]types.Result) ReplayReport {
	report := ReplayReport{
		TotalEvents:   len(entries),
		ChangesByRule: make(map[string]int),
	}

	for _, entry := range entries {
		oldResult, hasOld := oldResults[entry.Event.EventID]
		newResult, hasNew := newResults[entry.Event.EventID]

		if !hasOld || !hasNew {
			continue
		}

		if oldResult.FinalVerdict == newResult.FinalVerdict {
			report.Unchanged++
			continue
		}

		report.Changed++

		diff := VerdictDiff{
			EventID:    entry.Event.EventID,
			EventType:  entry.Event.EventType,
			OldVerdict: oldResult.FinalVerdict,
			NewVerdict: newResult.FinalVerdict,
			OldRules:   ruleIDs(oldResult.TriggeredRules),
			NewRules:   ruleIDs(newResult.TriggeredRules),
		}
		report.Diffs = append(report.Diffs, diff)

		for _, ruleID := range diff.NewRules {
			report.ChangesByRule[ruleID]++
		}
	}

	return report
}

// ruleIDs extracts rule IDs from a slice of RuleResult.
func ruleIDs(rrs []types.RuleResult) []string {
	ids := make([]string, 0, len(rrs))
	for _, rr := range rrs {
		ids = append(ids, rr.RuleID)
	}
	return ids
}

// printReport outputs the replay or shadow report in human-readable format.
func printReport(report ReplayReport, mode string) {
	fmt.Printf("=== %s Report ===\n", mode)
	fmt.Printf("Total events:  %d\n", report.TotalEvents)
	fmt.Printf("Changed:       %d\n", report.Changed)
	fmt.Printf("Unchanged:     %d\n", report.Unchanged)
	fmt.Printf("Duration:      %v\n", report.Duration)

	if len(report.ChangesByRule) > 0 {
		fmt.Printf("\nChanges by rule:\n")
		for ruleID, count := range report.ChangesByRule {
			fmt.Printf("  %-40s %d\n", ruleID, count)
		}
	}

	if len(report.Diffs) > 0 {
		fmt.Printf("\nVerdict changes (first 20):\n")
		shown := 0
		for _, d := range report.Diffs {
			if shown >= 20 {
				fmt.Printf("  ... and %d more\n", len(report.Diffs)-20)
				break
			}
			fmt.Printf("  %-40s %s -> %s\n", d.EventID, d.OldVerdict, d.NewVerdict)
			shown++
		}
	} else {
		fmt.Println("\nNo verdict changes detected.")
	}

	// Exit with code 1 if there are differences (useful for CI).
	if report.Changed > 0 {
		os.Exit(1)
	}
}
