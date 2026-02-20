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
