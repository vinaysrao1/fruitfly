package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"

	_ "github.com/marcboeker/go-duckdb"
)

// receiverStats mirrors the JSON returned by GET /stats on the receiver.
type receiverStats struct {
	Total       int64            `json:"total"`
	ByVerdict   map[string]int64 `json:"by_verdict"`
	ByComposite map[string]int64 `json:"by_composite"`
}

// duckDBResults holds all data queried from the DuckDB results table.
type duckDBResults struct {
	Total       int64
	ByVerdict   map[string]int64
	ByComposite map[string]int64
}

func fetchReceiverStats(receiverURL string) (*receiverStats, error) {
	resp, err := http.Get(receiverURL + "/stats")
	if err != nil {
		return nil, fmt.Errorf("GET %s/stats: %w", receiverURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("receiver /stats returned HTTP %d", resp.StatusCode)
	}

	var stats receiverStats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return nil, fmt.Errorf("decode receiver stats: %w", err)
	}
	return &stats, nil
}

func queryDuckDB(duckdbPath string) (*duckDBResults, error) {
	db, err := sql.Open("duckdb", duckdbPath+"?access_mode=read_only")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	results := &duckDBResults{
		ByVerdict:   make(map[string]int64),
		ByComposite: make(map[string]int64),
	}

	// Total count.
	row := db.QueryRow("SELECT COUNT(*) FROM results")
	if err := row.Scan(&results.Total); err != nil {
		return nil, fmt.Errorf("query total count: %w", err)
	}

	// Verdict breakdown.
	rows, err := db.Query("SELECT verdict, COUNT(*) as cnt FROM results GROUP BY verdict ORDER BY verdict")
	if err != nil {
		return nil, fmt.Errorf("query verdict breakdown: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var verdict string
		var cnt int64
		if err := rows.Scan(&verdict, &cnt); err != nil {
			return nil, fmt.Errorf("scan verdict row: %w", err)
		}
		results.ByVerdict[verdict] = cnt
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate verdict rows: %w", err)
	}

	// Event type x verdict breakdown.
	rows2, err := db.Query("SELECT event_type, verdict, COUNT(*) as cnt FROM results GROUP BY event_type, verdict ORDER BY event_type, verdict")
	if err != nil {
		return nil, fmt.Errorf("query event_type x verdict breakdown: %w", err)
	}
	defer rows2.Close()
	for rows2.Next() {
		var eventType, verdict string
		var cnt int64
		if err := rows2.Scan(&eventType, &verdict, &cnt); err != nil {
			return nil, fmt.Errorf("scan composite row: %w", err)
		}
		key := eventType + ":" + verdict
		results.ByComposite[key] = cnt
	}
	if err := rows2.Err(); err != nil {
		return nil, fmt.Errorf("iterate composite rows: %w", err)
	}

	return results, nil
}

func printReport(shimSent int64, db *duckDBResults, recv *receiverStats) bool {
	pass := true

	fmt.Println("=== Jetstream Test Results ===")
	fmt.Println()

	// Pipeline counts.
	dropped := shimSent - db.Total
	missed := db.Total - recv.Total
	var missedPct float64
	if db.Total > 0 {
		missedPct = float64(missed) / float64(db.Total) * 100
	}

	fmt.Println("Pipeline Counts:")
	fmt.Printf("  Shim sent:         %d\n", shimSent)
	fmt.Printf("  DuckDB results:    %d  (%d dropped)\n", db.Total, dropped)
	fmt.Printf("  Webhooks received: %d  (%d missed, %.1f%%)\n", recv.Total, missed, missedPct)
	fmt.Println()

	// Verdict breakdown from DuckDB.
	fmt.Println("Verdict Breakdown (DuckDB):")
	verdicts := []string{"approve", "block", "review"}
	for _, v := range verdicts {
		cnt := db.ByVerdict[v]
		var pct float64
		if db.Total > 0 {
			pct = float64(cnt) / float64(db.Total) * 100
		}
		fmt.Printf("  %-8s %d  (%4.1f%%)\n", v+":", cnt, pct)
	}
	fmt.Println()

	// Event type x verdict breakdown from DuckDB.
	fmt.Println("Event Type x Verdict (DuckDB):")
	compositeKeys := make([]string, 0, len(db.ByComposite))
	for k := range db.ByComposite {
		compositeKeys = append(compositeKeys, k)
	}
	sort.Strings(compositeKeys)
	for _, k := range compositeKeys {
		fmt.Printf("  %-20s %d\n", k+":", db.ByComposite[k])
	}
	fmt.Println()

	// Webhook verdict breakdown.
	fmt.Println("Webhook Verdict Breakdown:")
	for _, v := range verdicts {
		cnt := recv.ByVerdict[v]
		fmt.Printf("  %-8s %d\n", v+":", cnt)
	}
	fmt.Println()

	// Checks.
	var failReasons []string

	// Check 1: shim sent vs duckdb total (exact match expected).
	if dropped != 0 {
		failReasons = append(failReasons, fmt.Sprintf("%d events lost in pipeline (shim sent %d, DuckDB has %d)", dropped, shimSent, db.Total))
		pass = false
	}

	// Check 2: duckdb total vs receiver total (>1% is a failure).
	if db.Total > 0 && missedPct > 1.0 {
		failReasons = append(failReasons, fmt.Sprintf("webhook loss %.1f%% exceeds 1%% threshold (%d missed of %d)", missedPct, missed, db.Total))
		pass = false
	}

	// Check 3: verdict breakdown sum must equal total.
	var verdictSum int64
	for _, cnt := range db.ByVerdict {
		verdictSum += cnt
	}
	if verdictSum != db.Total {
		failReasons = append(failReasons, fmt.Sprintf("verdict breakdown sum %d != DuckDB total %d", verdictSum, db.Total))
		pass = false
	}

	if pass {
		fmt.Println("Status: PASS (no pipeline drops, webhook loss < 1%)")
	} else {
		fmt.Println("Status: FAIL")
		for _, reason := range failReasons {
			fmt.Printf("  - %s\n", reason)
		}
	}

	return pass
}

func main() {
	receiverURL := flag.String("receiver-url", "http://localhost:9090", "Receiver base URL")
	duckdbPath := flag.String("duckdb-path", "./jetstream_test.duckdb", "Path to Fruitfly's DuckDB file")
	shimSent := flag.Int64("shim-sent", 0, "Number of events the shim sent (required)")
	flag.Parse()

	if *shimSent == 0 {
		fmt.Fprintln(os.Stderr, "error: -shim-sent is required and must be > 0")
		flag.Usage()
		os.Exit(1)
	}

	recv, err := fetchReceiverStats(*receiverURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error fetching receiver stats: %v\n", err)
		os.Exit(1)
	}

	dbResults, err := queryDuckDB(*duckdbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error querying DuckDB: %v\n", err)
		os.Exit(1)
	}

	pass := printReport(*shimSent, dbResults, recv)
	if !pass {
		os.Exit(1)
	}
}
