package integration_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vinaysrao1/fruitfly/output"
	"github.com/vinaysrao1/fruitfly/rules"
)

// buildAdminMux recreates the admin handler registrations from main.go.
// This allows testing the admin endpoints without starting the full HTTP server.
//
// DUPLICATION WARNING: This function duplicates the handler logic from main.go
// (lines ~94-156). If the admin handlers in main.go are changed (e.g. new routes
// added, response format modified, status codes altered), this function MUST be
// updated in lockstep to keep tests valid. A future refactor should extract the
// handler registration into a shared exported function (e.g. admin.RegisterHandlers)
// so that both main.go and this test use the same implementation.
func buildAdminMux(snapshotPtr *atomic.Pointer[rules.Snapshot], reloader *rules.Reloader, writer *output.Writer) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /admin/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("GET /admin/ready", func(w http.ResponseWriter, r *http.Request) {
		if reloader.Ready.Load() && writer.Ready.Load() {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
		} else {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
		}
	})

	mux.HandleFunc("GET /admin/rules", func(w http.ResponseWriter, r *http.Request) {
		snap := snapshotPtr.Load()
		if snap == nil {
			http.Error(w, "no snapshot loaded", http.StatusServiceUnavailable)
			return
		}
		type ruleInfo struct {
			RuleID      string `json:"rule_id"`
			EventType   string `json:"event_type"`
			Priority    int    `json:"priority"`
			Prefiltered int64  `json:"prefiltered"`
			Evals       int64  `json:"evals"`
			AvgUS       int64  `json:"avg_us"`
			Slow        bool   `json:"slow"`
		}
		type response struct {
			ID        string     `json:"id"`
			RuleCount int        `json:"rule_count"`
			LoadedAt  time.Time  `json:"loaded_at"`
			Rules     []ruleInfo `json:"rules"`
		}
		slowThreshold := rules.SlowRuleThreshold(snap.Rules)
		ruleInfos := make([]ruleInfo, len(snap.Rules))
		for i, rule := range snap.Rules {
			ewma := rule.Stats.EWMA()
			ruleInfos[i] = ruleInfo{
				RuleID:      rule.RuleID,
				EventType:   rule.EventType,
				Priority:    rule.Priority,
				Prefiltered: rule.Prefiltered.Load(),
				Evals:       rule.Stats.Evals(),
				AvgUS:       ewma.Microseconds(),
				Slow:        slowThreshold > 0 && ewma > slowThreshold,
			}
		}
		resp := response{
			ID:        snap.ID,
			RuleCount: len(snap.Rules),
			LoadedAt:  snap.LoadedAt,
			Rules:     ruleInfos,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("POST /admin/rules/reload", func(w http.ResponseWriter, r *http.Request) {
		reloader.Reload()
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("reload triggered"))
	})

	mux.HandleFunc("GET /admin/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("# metrics placeholder\n"))
	})

	return mux
}

// TestAdminHealth_Always200 (T51):
// GET /admin/health always returns 200 with body "ok" regardless of system state.
func TestAdminHealth_Always200(t *testing.T) {
	tp := newTestPipeline(t, approveAllRule, defaultOpts(), "")
	mux := buildAdminMux(tp.snapshotPtr, tp.reloader, tp.writer)

	req := httptest.NewRequest(http.MethodGet, "/admin/health", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("want 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if rr.Body.String() != "ok" {
		t.Errorf("want body 'ok', got %q", rr.Body.String())
	}
}

// TestAdminReady_ReflectsState (T52):
// GET /admin/ready returns 200 when both reloader and writer are ready; 503 otherwise.
func TestAdminReady_ReflectsState(t *testing.T) {
	tp := newTestPipeline(t, approveAllRule, defaultOpts(), "")
	mux := buildAdminMux(tp.snapshotPtr, tp.reloader, tp.writer)

	// Both ready (default state after NewReloader + NewWriter).
	t.Run("both_ready_200", func(t *testing.T) {
		tp.reloader.Ready.Store(true)
		tp.writer.Ready.Store(true)

		req := httptest.NewRequest(http.MethodGet, "/admin/ready", nil)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("want 200, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	// Writer not ready.
	t.Run("writer_not_ready_503", func(t *testing.T) {
		tp.reloader.Ready.Store(true)
		tp.writer.Ready.Store(false)

		req := httptest.NewRequest(http.MethodGet, "/admin/ready", nil)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusServiceUnavailable {
			t.Errorf("want 503, got %d: %s", rr.Code, rr.Body.String())
		}

		// Restore state.
		tp.writer.Ready.Store(true)
	})

	// Reloader not ready.
	t.Run("reloader_not_ready_503", func(t *testing.T) {
		tp.reloader.Ready.Store(false)
		tp.writer.Ready.Store(true)

		req := httptest.NewRequest(http.MethodGet, "/admin/ready", nil)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusServiceUnavailable {
			t.Errorf("want 503, got %d: %s", rr.Code, rr.Body.String())
		}

		// Restore state.
		tp.reloader.Ready.Store(true)
	})
}

// TestAdminRules_ReturnsSnapshot (T53):
// GET /admin/rules returns JSON with id, rule_count, loaded_at, rules array.
// Returns 503 when snapshot is nil.
func TestAdminRules_ReturnsSnapshot(t *testing.T) {
	tp := newTestPipeline(t, approveAllRule, defaultOpts(), "")
	mux := buildAdminMux(tp.snapshotPtr, tp.reloader, tp.writer)

	// Snapshot is loaded (newTestPipeline calls NewReloader which loads it).
	t.Run("snapshot_present_200", func(t *testing.T) {
		snap := tp.snapshotPtr.Load()
		if snap == nil {
			t.Fatal("expected snapshot to be loaded, but it's nil")
		}

		req := httptest.NewRequest(http.MethodGet, "/admin/rules", nil)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("want 200, got %d: %s", rr.Code, rr.Body.String())
		}

		ct := rr.Header().Get("Content-Type")
		if !strings.Contains(ct, "application/json") {
			t.Errorf("want Content-Type application/json, got %q", ct)
		}

		var resp struct {
			ID        string    `json:"id"`
			RuleCount int       `json:"rule_count"`
			LoadedAt  time.Time `json:"loaded_at"`
			Rules     []struct {
				RuleID    string `json:"rule_id"`
				EventType string `json:"event_type"`
				Priority  int    `json:"priority"`
			} `json:"rules"`
		}
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal response: %v", err)
		}

		if resp.ID == "" {
			t.Error("expected non-empty snapshot id")
		}
		if resp.RuleCount != len(snap.Rules) {
			t.Errorf("want rule_count=%d, got %d", len(snap.Rules), resp.RuleCount)
		}
		if resp.LoadedAt.IsZero() {
			t.Error("expected non-zero loaded_at")
		}
		if len(resp.Rules) != len(snap.Rules) {
			t.Errorf("want %d rules in array, got %d", len(snap.Rules), len(resp.Rules))
		}
	})

	// Nil snapshot: 503.
	t.Run("nil_snapshot_503", func(t *testing.T) {
		// Store nil in the snapshot pointer.
		tp.snapshotPtr.Store(nil)

		req := httptest.NewRequest(http.MethodGet, "/admin/rules", nil)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusServiceUnavailable {
			t.Errorf("want 503, got %d: %s", rr.Code, rr.Body.String())
		}

		// Restore snapshot so other test cases work.
		cancel, poolDone, writerDone := tp.start(t)
		tp.reloader.Reload()
		time.Sleep(200 * time.Millisecond)
		cancel()
		close(tp.eventChan)
		<-poolDone
		<-writerDone
	})
}

// TestAdminRulesReload_Triggers (T54):
// POST /admin/rules/reload returns 202 and actually triggers a reload (snapshot ID changes).
func TestAdminRulesReload_Triggers(t *testing.T) {
	tp := newTestPipeline(t, approveAllRule, defaultOpts(), "")
	cancel, poolDone, writerDone := tp.start(t)

	mux := buildAdminMux(tp.snapshotPtr, tp.reloader, tp.writer)

	// Capture initial snapshot ID.
	initialSnap := tp.snapshotPtr.Load()
	if initialSnap == nil {
		t.Fatal("initial snapshot is nil")
	}

	// Overwrite the rule file with blockAllRule so reload produces a different snapshot ID.
	writeRule(t, tp.rulesDir, "rule.star", blockAllRule)

	// POST to reload endpoint.
	req := httptest.NewRequest(http.MethodPost, "/admin/rules/reload", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Errorf("want 202, got %d: %s", rr.Code, rr.Body.String())
	}

	// Wait for the snapshot to update (reload is async via the reloadCh).
	deadline := time.Now().Add(2 * time.Second)
	swapped := false
	for time.Now().Before(deadline) {
		newSnap := tp.snapshotPtr.Load()
		if newSnap != nil && newSnap.ID != initialSnap.ID {
			swapped = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !swapped {
		t.Error("expected snapshot to reload (ID to change) after POST /admin/rules/reload, but it did not")
	}

	tp.shutdown(t, cancel, poolDone, writerDone)
}

// TestAdminMetrics_Placeholder (T55):
// GET /admin/metrics returns 200 with Content-Type text/plain.
func TestAdminMetrics_Placeholder(t *testing.T) {
	tp := newTestPipeline(t, approveAllRule, defaultOpts(), "")
	mux := buildAdminMux(tp.snapshotPtr, tp.reloader, tp.writer)

	req := httptest.NewRequest(http.MethodGet, "/admin/metrics", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("want 200, got %d: %s", rr.Code, rr.Body.String())
	}

	ct := rr.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/plain") {
		t.Errorf("want Content-Type text/plain, got %q", ct)
	}

	if rr.Body.Len() == 0 {
		t.Error("expected non-empty response body")
	}
}
