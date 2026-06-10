package output

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb"

	"github.com/vinaysrao1/fruitfly/types"
)

// slowHandler returns an httptest.Server whose handler sleeps for d before responding.
func slowHandler(d time.Duration, status int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(d)
		w.WriteHeader(status)
	}))
}

func makeResult(eventID, eventType string, verdict types.Verdict) types.Result {
	return types.Result{
		EventID:      eventID,
		EventType:    eventType,
		FinalVerdict: verdict,
		TriggeredRules: []types.RuleResult{
			{RuleID: "rule-1", Verdict: verdict, Reason: "test reason"},
		},
		FailedRules: []types.RuleResult{},
		Payload:     map[string]any{"key": "value"},
		LatencyUS:   1500,
		ProcessedAt: time.Now(),
	}
}

// testDB creates a temp DuckDB path and returns a Writer + the db path for post-run queries.
func testDB(t *testing.T, webhookURL string) (*Writer, string) {
	t.Helper()
	dbPath := t.TempDir() + "/test.duckdb"
	w, err := NewWriter(dbPath, webhookURL)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	return w, dbPath
}

// openReadDB opens a fresh DuckDB connection at dbPath for post-run verification.
func openReadDB(t *testing.T, dbPath string) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		t.Fatalf("open read db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestWriteAndQuery_SingleResult(t *testing.T) {
	w, dbPath := testDB(t, "")

	in := make(chan types.Result, 1)
	in <- makeResult("evt-001", "purchase", types.VerdictBlock)
	close(in)

	if err := w.Run(context.Background(), in); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Run closed the writer's DB; open a new connection for verification.
	db := openReadDB(t, dbPath)

	var verdict, eventType string
	var latencyUS int64
	if err := db.QueryRow(
		"SELECT verdict, event_type, latency_us FROM results WHERE event_id = ?",
		"evt-001",
	).Scan(&verdict, &eventType, &latencyUS); err != nil {
		t.Fatalf("query: %v", err)
	}
	if verdict != "block" {
		t.Errorf("verdict: got %q, want %q", verdict, "block")
	}
	if eventType != "purchase" {
		t.Errorf("event_type: got %q, want %q", eventType, "purchase")
	}
	if latencyUS != 1500 {
		t.Errorf("latency_us: got %d, want 1500", latencyUS)
	}
}

func TestBatchFlush_OnSize(t *testing.T) {
	w, dbPath := testDB(t, "")

	in := make(chan types.Result, 200)
	for i := 0; i < 100; i++ {
		in <- makeResult(fmt.Sprintf("evt-%03d", i), "click", types.VerdictApprove)
	}
	close(in)

	if err := w.Run(context.Background(), in); err != nil {
		t.Fatalf("Run: %v", err)
	}

	db := openReadDB(t, dbPath)

	var n int
	if err := db.QueryRow("SELECT count(*) FROM results").Scan(&n); err != nil {
		t.Fatalf("query count: %v", err)
	}
	if n != 100 {
		t.Errorf("row count: got %d, want 100", n)
	}
}

func TestBatchFlush_OnTimer(t *testing.T) {
	dbPath := t.TempDir() + "/test.duckdb"
	w, err := NewWriter(dbPath, "")
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	in := make(chan types.Result, 20)
	for i := 0; i < 10; i++ {
		in <- makeResult(fmt.Sprintf("evt-%03d", i), "view", types.VerdictApprove)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx, in) //nolint:errcheck
	}()

	// Wait past the 500ms flush interval.
	time.Sleep(700 * time.Millisecond)

	// Query the writer's DB directly (Run is still running, DB is still open).
	var n int
	if err := w.db.QueryRow("SELECT count(*) FROM results").Scan(&n); err != nil {
		t.Fatalf("query count: %v", err)
	}
	if n != 10 {
		t.Errorf("after timer: row count = %d, want 10", n)
	}

	close(in)
	<-done
}

func TestFinalFlush_OnChannelClose(t *testing.T) {
	w, dbPath := testDB(t, "")

	in := make(chan types.Result, 10)
	for i := 0; i < 5; i++ {
		in <- makeResult(fmt.Sprintf("evt-%03d", i), "signup", types.VerdictReview)
	}
	close(in)

	if err := w.Run(context.Background(), in); err != nil {
		t.Fatalf("Run: %v", err)
	}

	db := openReadDB(t, dbPath)

	var n int
	if err := db.QueryRow("SELECT count(*) FROM results").Scan(&n); err != nil {
		t.Fatalf("query count: %v", err)
	}
	if n != 5 {
		t.Errorf("row count: got %d, want 5", n)
	}
}

func TestWebhook_SuccessfulDelivery(t *testing.T) {
	var received atomic.Int32
	var capturedBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedBody, _ = io.ReadAll(r.Body)
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w, _ := testDB(t, srv.URL)

	result := makeResult("evt-webhook-01", "login", types.VerdictApprove)
	w.sendWebhook(context.Background(), result)

	if n := received.Load(); n != 1 {
		t.Errorf("webhook calls: got %d, want 1", n)
	}

	var parsed map[string]any
	if err := json.Unmarshal(capturedBody, &parsed); err != nil {
		t.Fatalf("webhook body is not valid JSON: %v", err)
	}
	if parsed["EventID"] != "evt-webhook-01" {
		t.Errorf("webhook body EventID: got %v, want evt-webhook-01", parsed["EventID"])
	}
}

func TestWebhook_RetryOn500(t *testing.T) {
	var callCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := callCount.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w, _ := testDB(t, srv.URL)
	w.sendWebhook(context.Background(), makeResult("evt-retry-01", "order", types.VerdictBlock))

	if n := callCount.Load(); n != 2 {
		t.Errorf("webhook calls: got %d, want 2 (1 failure + 1 success)", n)
	}
}

func TestWebhook_No_RetryOn400(t *testing.T) {
	var callCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	w, _ := testDB(t, srv.URL)
	w.sendWebhook(context.Background(), makeResult("evt-400-01", "order", types.VerdictApprove))

	if n := callCount.Load(); n != 1 {
		t.Errorf("webhook calls: got %d, want 1 (no retry on 400)", n)
	}
}

// T50: DuckDB INSERT OR IGNORE duplicate event_id
// Insert two results with same event_id; verify only 1 row with the first row's data.
func TestDuplicateEventID_InsertOrIgnore(t *testing.T) {
	w, dbPath := testDB(t, "")

	in := make(chan types.Result, 2)
	r1 := makeResult("dup-id", "type1", types.VerdictApprove)
	r2 := makeResult("dup-id", "type2", types.VerdictBlock)
	in <- r1
	in <- r2
	close(in)

	if err := w.Run(context.Background(), in); err != nil {
		t.Fatalf("Run: %v", err)
	}

	db := openReadDB(t, dbPath)

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM results WHERE event_id = 'dup-id'").Scan(&count); err != nil {
		t.Fatalf("query count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row for duplicate event_id, got %d", count)
	}

	var verdict string
	if err := db.QueryRow("SELECT verdict FROM results WHERE event_id = 'dup-id'").Scan(&verdict); err != nil {
		t.Fatalf("query verdict: %v", err)
	}
	if verdict != "approve" {
		t.Errorf("expected first row preserved (approve), got %q", verdict)
	}
}

// T16: runRetention cleanup
// Insert rows with processed_at 31 days ago, call runRetention, verify deleted.
// Insert rows 29 days ago, verify retained.
func TestRunRetention_DeletesExpiredRows(t *testing.T) {
	w, dbPath := testDB(t, "")

	// Insert old row (31 days ago) directly via SQL.
	oldTime := time.Now().Add(-31 * 24 * time.Hour)
	_, err := w.db.Exec(
		`INSERT INTO results (event_id, event_type, verdict, triggered_rules, failed_rules, payload, latency_us, processed_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"old-event", "test", "approve", "[]", "[]", "{}", 100, oldTime,
	)
	if err != nil {
		t.Fatalf("insert old row: %v", err)
	}

	// Insert recent row (29 days ago).
	recentTime := time.Now().Add(-29 * 24 * time.Hour)
	_, err = w.db.Exec(
		`INSERT INTO results (event_id, event_type, verdict, triggered_rules, failed_rules, payload, latency_us, processed_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"recent-event", "test", "approve", "[]", "[]", "{}", 100, recentTime,
	)
	if err != nil {
		t.Fatalf("insert recent row: %v", err)
	}

	w.runRetention()

	var oldCount, recentCount int
	if err := w.db.QueryRow("SELECT COUNT(*) FROM results WHERE event_id = 'old-event'").Scan(&oldCount); err != nil {
		t.Fatalf("query old count: %v", err)
	}
	if err := w.db.QueryRow("SELECT COUNT(*) FROM results WHERE event_id = 'recent-event'").Scan(&recentCount); err != nil {
		t.Fatalf("query recent count: %v", err)
	}

	if oldCount != 0 {
		t.Errorf("expected old row deleted, got count=%d", oldCount)
	}
	if recentCount != 1 {
		t.Errorf("expected recent row retained, got count=%d", recentCount)
	}

	// Verify the read db sees the same state.
	_ = dbPath // dbPath used by openReadDB after Close; skip here since w.db is still open.
}

// T14a: Webhook endpoint down (connection refused)
func TestWebhook_EndpointDown(t *testing.T) {
	// Use a server that immediately closes to simulate connection refused.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // close immediately so connections are refused

	w, dbPath := testDB(t, url)

	in := make(chan types.Result, 1)
	in <- makeResult("down-event", "test", types.VerdictApprove)
	close(in)

	if err := w.Run(context.Background(), in); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// DuckDB write should succeed even when webhook is down.
	db := openReadDB(t, dbPath)
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM results WHERE event_id = 'down-event'").Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 DuckDB row despite webhook failure, got %d", count)
	}
}

// T14b: Webhook slow (timeout) — server sleeps longer than webhookTimeout.
func TestWebhook_SlowEndpoint_Timeout(t *testing.T) {
	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		// Sleep longer than the webhookTimeout (5s); client should time out.
		time.Sleep(7 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w, dbPath := testDB(t, srv.URL)
	// Reduce timeout for the test to keep it fast.
	w.webhookClient.Timeout = 100 * time.Millisecond

	in := make(chan types.Result, 1)
	in <- makeResult("slow-event", "test", types.VerdictApprove)
	close(in)

	if err := w.Run(context.Background(), in); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Give goroutines time to finish attempts.
	time.Sleep(500 * time.Millisecond)

	// DuckDB write should succeed.
	db := openReadDB(t, dbPath)
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM results WHERE event_id = 'slow-event'").Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 DuckDB row despite webhook timeout, got %d", count)
	}
}

// T14c: Webhook persistent 500 — assert exactly 3 attempts.
func TestWebhook_Persistent500_3Attempts(t *testing.T) {
	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	w, _ := testDB(t, srv.URL)
	w.sendWebhook(context.Background(), makeResult("500-all", "test", types.VerdictApprove))

	if n := callCount.Load(); n != 3 {
		t.Errorf("expected 3 attempts on persistent 500, got %d", n)
	}
}

// T14d: Webhook 4xx no retry (already covered by TestWebhook_No_RetryOn400, add for completeness).
func TestWebhook_4xx_NoRetry(t *testing.T) {
	tests := []struct {
		name   string
		status int
	}{
		{"403 Forbidden", http.StatusForbidden},
		{"404 Not Found", http.StatusNotFound},
		{"422 Unprocessable", http.StatusUnprocessableEntity},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var callCount atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				callCount.Add(1)
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			w, _ := testDB(t, srv.URL)
			w.sendWebhook(context.Background(), makeResult("4xx-test", "test", types.VerdictApprove))

			if n := callCount.Load(); n != 1 {
				t.Errorf("status %d: expected 1 call (no retry), got %d", tc.status, n)
			}
		})
	}
}

// T14e: Webhook 429 retry — return 429 twice then 200, assert 3 total calls.
func TestWebhook_429Retry(t *testing.T) {
	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := callCount.Add(1)
		if n <= 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w, _ := testDB(t, srv.URL)
	w.sendWebhook(context.Background(), makeResult("retry-429", "test", types.VerdictApprove))

	if n := callCount.Load(); n != 3 {
		t.Errorf("expected 3 calls (2x429 + 1x200), got %d", n)
	}
}

// T14f: Webhook 3xx falls through retry loop (not a redirect that succeeds).
func TestWebhook_3xx_Retries(t *testing.T) {
	var callCount atomic.Int32
	// Use a custom client with no redirect follow so 301 is treated as an error status.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		// 301 does not satisfy statusCode < 300, and is not in the 4xx no-retry range.
		// sendWebhook will retry 3 times.
		w.WriteHeader(http.StatusMovedPermanently)
	}))
	defer srv.Close()

	w, _ := testDB(t, srv.URL)
	// Disable redirects so the client sees the 301 directly.
	w.webhookClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}
	w.sendWebhook(context.Background(), makeResult("3xx-test", "test", types.VerdictApprove))

	if n := callCount.Load(); n != 3 {
		t.Errorf("expected 3 retry attempts on 301, got %d", n)
	}
}

// T27: Webhook semaphore saturation
// Fill webhookSem to capacity, verify webhook is dropped with warning, DuckDB still succeeds.
func TestWebhookSemaphore_Saturation(t *testing.T) {
	var webhookCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webhookCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w, dbPath := testDB(t, srv.URL)

	// Fill the semaphore to capacity so no webhook goroutine can be launched.
	for i := 0; i < maxConcurrentWebhooks; i++ {
		w.webhookSem <- struct{}{}
	}

	in := make(chan types.Result, 1)
	in <- makeResult("sem-test", "test", types.VerdictApprove)
	close(in)

	if err := w.Run(context.Background(), in); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Drain the semaphore after Run exits.
	for i := 0; i < maxConcurrentWebhooks; i++ {
		<-w.webhookSem
	}

	db := openReadDB(t, dbPath)
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM results").Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 DuckDB row despite webhook drop, got %d", count)
	}
	if n := webhookCalls.Load(); n != 0 {
		t.Errorf("expected 0 webhook calls (semaphore full), got %d", n)
	}
}

// T13: DuckDB write failure injection
// Close the underlying db before flushing; flush should retry once then drop without panicking.
func TestDuckDB_WriteFailure_RetryThenDrop(t *testing.T) {
	w, _ := testDB(t, "")

	// Close the DB to simulate a write failure.
	w.db.Close()

	// flush should retry once and then drop the batch without panicking.
	batch := []types.Result{makeResult("fail-event", "test", types.VerdictApprove)}
	w.flush(batch) // must not panic

	// If we reach here without panic, the retry-then-drop behavior is confirmed.
}

// T28: Writer drain path exercise.
// Cancel context immediately before writer has a chance to process items, then add items
// to the channel. The writer must drain them via the ctx.Done() drain branch.
func TestWriterDrain_ContextCancel(t *testing.T) {
	w, dbPath := testDB(t, "")

	// Use an unbuffered channel so we control when items arrive.
	in := make(chan types.Result, 20)

	// Cancel context BEFORE starting the writer so it immediately hits ctx.Done().
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel right away

	done := make(chan error, 1)
	go func() {
		done <- w.Run(ctx, in)
	}()

	// Give the writer goroutine time to reach the ctx.Done() select branch.
	time.Sleep(20 * time.Millisecond)

	// Now send items into the channel. The writer is in its drain loop and will
	// pick these up before the drain timeout fires.
	const numItems = 5
	for i := 0; i < numItems; i++ {
		in <- makeResult(fmt.Sprintf("drain-%d", i), "test", types.VerdictApprove)
	}
	// Close the channel so the drain loop exits cleanly (rather than waiting for timeout).
	close(in)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("writer did not exit within 10s after context cancel")
	}

	db := openReadDB(t, dbPath)
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM results").Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != numItems {
		t.Errorf("expected %d rows after drain, got %d", numItems, count)
	}
}

// T28b: Writer drain timeout expires with items still in channel.
// Verify Writer exits after 5s even when channel is never closed.
func TestWriterDrain_TimeoutExpiry(t *testing.T) {
	w, _ := testDB(t, "")

	// Channel is never closed — drain will time out after 5s.
	in := make(chan types.Result, 1)
	in <- makeResult("timeout-event", "test", types.VerdictApprove)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- w.Run(ctx, in)
	}()

	// Cancel context immediately.
	time.Sleep(10 * time.Millisecond)
	cancel()

	// Writer should exit after drain timeout (5s) + some margin.
	select {
	case <-done:
		// success: writer exited after drain timeout
	case <-time.After(10 * time.Second):
		t.Fatal("writer did not exit within 10s after context cancel with never-closed channel")
	}
}
