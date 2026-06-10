package ingest

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vinaysrao1/fruitfly/types"
)

func makeServer(bufSize, maxBytes int) (*Server, chan types.Event) {
	ch := make(chan types.Event, bufSize)
	srv := NewServer(":unused", maxBytes, ch)
	return srv, ch
}

func post(srv *Server, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	return rr
}

func validBody(eventID string) string {
	id := ""
	if eventID != "" {
		id = `"event_id": "` + eventID + `",`
	}
	return `{` + id + `"event_type": "user.signup", "timestamp": "2024-01-15T10:30:00Z"}`
}

func TestValidEvent_ProducesCorrectEvent(t *testing.T) {
	srv, ch := makeServer(1, 1024)

	before := time.Now()
	rr := post(srv, `{
		"event_id":   "test-event-123",
		"event_type": "user.signup",
		"timestamp":  "2024-01-15T10:30:00Z",
		"user_id":    "u-456"
	}`)
	after := time.Now()

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rr.Code, rr.Body.String())
	}

	select {
	case event := <-ch:
		if event.EventID == "" {
			t.Error("EventID must not be empty")
		}
		if event.EventType == "" {
			t.Error("EventType must not be empty")
		}
		if event.Timestamp.IsZero() {
			t.Error("Timestamp must not be zero")
		}
		if len(event.RawJSON) == 0 {
			t.Error("RawJSON must not be empty")
		}
		if event.EventID != "test-event-123" {
			t.Errorf("EventID: want %q, got %q", "test-event-123", event.EventID)
		}
		if event.EventType != "user.signup" {
			t.Errorf("EventType: want %q, got %q", "user.signup", event.EventType)
		}
		wantTS, _ := time.Parse(time.RFC3339, "2024-01-15T10:30:00Z")
		if !event.Timestamp.Equal(wantTS) {
			t.Errorf("Timestamp: want %v, got %v", wantTS, event.Timestamp)
		}
		if event.ReceivedAt.Before(before) || event.ReceivedAt.After(after) {
			t.Errorf("ReceivedAt %v not in expected range [%v, %v]", event.ReceivedAt, before, after)
		}
		if event.Payload == nil {
			t.Error("Payload must not be nil")
		}
		if uid, _ := event.Payload["user_id"].(string); uid != "u-456" {
			t.Errorf("Payload[user_id]: want %q, got %q", "u-456", uid)
		}
	default:
		t.Fatal("expected event on channel, got none")
	}
}

func TestMissingEventID_AutoGenerates(t *testing.T) {
	srv, ch := makeServer(1, 1024)

	rr := post(srv, `{"event_type": "order.placed", "timestamp": "2024-03-01T00:00:00Z"}`)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rr.Code, rr.Body.String())
	}

	select {
	case event := <-ch:
		if event.EventID == "" {
			t.Fatal("EventID must be auto-generated when absent from payload")
		}
		if len(event.EventID) != 36 {
			t.Errorf("auto-generated EventID has unexpected length %d: %q", len(event.EventID), event.EventID)
		}
		if event.EventID[14] != '7' {
			t.Errorf("auto-generated EventID is not UUIDv7 (position 14 should be '7'): %q", event.EventID)
		}
	default:
		t.Fatal("expected event on channel, got none")
	}
}

func TestInvalidJSON_Returns400(t *testing.T) {
	srv, ch := makeServer(1, 1024)

	cases := []string{
		"",
		"not json",
		`{"event_type": }`,
		`[1, 2, 3]`,
	}

	for _, body := range cases {
		rr := post(srv, body)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("body=%q: want 400, got %d", body, rr.Code)
		}
	}

	if len(ch) != 0 {
		t.Errorf("expected empty channel, got %d events", len(ch))
	}
}

func TestMissingRequiredFields_Returns400(t *testing.T) {
	srv, ch := makeServer(10, 1024)

	cases := []struct {
		name string
		body string
	}{
		{"missing event_type", `{"timestamp": "2024-01-15T10:30:00Z"}`},
		{"missing timestamp", `{"event_type": "user.login"}`},
		{"event_type is integer", `{"event_type": 42, "timestamp": "2024-01-15T10:30:00Z"}`},
		{"event_type is empty string", `{"event_type": "", "timestamp": "2024-01-15T10:30:00Z"}`},
		{"event_type is boolean", `{"event_type": true, "timestamp": "2024-01-15T10:30:00Z"}`},
		{"timestamp is invalid RFC3339", `{"event_type": "login", "timestamp": "not-a-date"}`},
		{"timestamp is integer", `{"event_type": "login", "timestamp": 1700000000}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rr := post(srv, tc.body)
			if rr.Code != http.StatusBadRequest {
				t.Errorf("want 400, got %d: %s", rr.Code, rr.Body.String())
			}
		})
	}

	if len(ch) != 0 {
		t.Errorf("expected empty channel, got %d events", len(ch))
	}
}

func TestOversizedPayload_Returns413(t *testing.T) {
	const maxBytes = 64
	srv, ch := makeServer(1, maxBytes)

	bigJSON := `{"event_type":"t","timestamp":"2024-01-15T10:30:00Z","pad":"` +
		strings.Repeat("x", maxBytes) + `"}`

	rr := post(srv, bigJSON)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("want 413, got %d: %s", rr.Code, rr.Body.String())
	}
	if len(ch) != 0 {
		t.Error("expected empty channel after oversized payload")
	}
}

func TestChannelFull_Returns429(t *testing.T) {
	srv, ch := makeServer(1, 1024)

	body := validBody("fill-id")

	rr1 := post(srv, body)
	if rr1.Code != http.StatusAccepted {
		t.Fatalf("first request: want 202, got %d", rr1.Code)
	}

	if len(ch) != 1 {
		t.Fatalf("expected 1 item in channel, got %d", len(ch))
	}

	rr2 := post(srv, validBody("second-id"))
	if rr2.Code != http.StatusTooManyRequests {
		t.Errorf("second request: want 429, got %d: %s", rr2.Code, rr2.Body.String())
	}

	<-ch
}

func TestContentType_NonJSON_Returns415(t *testing.T) {
	srv, ch := makeServer(1, 1024)

	body := validBody("ct-test-id")
	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(body))
	req.Header.Set("Content-Type", "text/plain")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusUnsupportedMediaType {
		t.Errorf("want 415, got %d: %s", rr.Code, rr.Body.String())
	}
	if len(ch) != 0 {
		t.Error("expected empty channel after content-type rejection")
	}
}

func TestContentType_Missing_Accepted(t *testing.T) {
	srv, ch := makeServer(1, 1024)

	body := validBody("ct-missing-id")
	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(body))
	// No Content-Type header set.
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Errorf("want 202, got %d: %s", rr.Code, rr.Body.String())
	}
	if len(ch) != 1 {
		t.Errorf("expected 1 event on channel, got %d", len(ch))
	}
	<-ch
}

func TestEventID_TooLong_Returns400(t *testing.T) {
	srv, ch := makeServer(1, 4096)

	longID := strings.Repeat("a", 300)
	body := `{"event_id": "` + longID + `", "event_type": "user.signup", "timestamp": "2024-01-15T10:30:00Z"}`
	rr := post(srv, body)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d: %s", rr.Code, rr.Body.String())
	}
	if len(ch) != 0 {
		t.Error("expected empty channel after event_id too long rejection")
	}
}

// TestEventID_256CharBoundary tests the exact boundary for event_id length.
// T8: 256-char event_id is accepted (202), 257-char event_id is rejected (400).
func TestEventID_256CharBoundary(t *testing.T) {
	srv, ch := makeServer(2, 4096)

	// 256 chars: exactly at limit, must be accepted.
	id256 := strings.Repeat("a", 256)
	body256 := `{"event_id": "` + id256 + `", "event_type": "test", "timestamp": "2024-01-15T10:30:00Z"}`
	rr := post(srv, body256)
	if rr.Code != http.StatusAccepted {
		t.Errorf("256-char event_id: want 202, got %d: %s", rr.Code, rr.Body.String())
	}
	<-ch

	// 257 chars: one over the limit, must be rejected.
	id257 := strings.Repeat("a", 257)
	body257 := `{"event_id": "` + id257 + `", "event_type": "test", "timestamp": "2024-01-15T10:30:00Z"}`
	rr2 := post(srv, body257)
	if rr2.Code != http.StatusBadRequest {
		t.Errorf("257-char event_id: want 400, got %d: %s", rr2.Code, rr2.Body.String())
	}
	if len(ch) != 0 {
		t.Error("expected empty channel after 257-char event_id rejection")
	}
}
