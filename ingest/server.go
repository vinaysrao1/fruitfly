package ingest

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vinaysrao1/fruitfly/types"
)

// Server is the HTTP event ingestion server.
type Server struct {
	addr     string
	maxBytes int
	eventOut chan<- types.Event
}

// NewServer creates the ingestion server.
func NewServer(addr string, maxBytes int, eventOut chan<- types.Event) *Server {
	return &Server{
		addr:     addr,
		maxBytes: maxBytes,
		eventOut: eventOut,
	}
}

// Handler returns the configured HTTP mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", s.handleEvent)
	return mux
}

// handleEvent handles POST /events.
func (s *Server) handleEvent(w http.ResponseWriter, r *http.Request) {
	ct := r.Header.Get("Content-Type")
	if ct != "" && !strings.HasPrefix(ct, "application/json") {
		slog.Warn("invalid event: unsupported content type", "content_type", ct)
		http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, int64(s.maxBytes))

	raw, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			slog.Warn("invalid event: payload too large")
			http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		slog.Warn("invalid event: malformed JSON", "error", err.Error())
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	eventTypeRaw, ok := payload["event_type"]
	if !ok {
		slog.Warn("invalid event: missing or invalid event_type")
		http.Error(w, "missing required field: event_type", http.StatusBadRequest)
		return
	}
	eventType, ok := eventTypeRaw.(string)
	if !ok || eventType == "" {
		slog.Warn("invalid event: missing or invalid event_type")
		http.Error(w, "event_type must be a non-empty string", http.StatusBadRequest)
		return
	}

	timestampRaw, ok := payload["timestamp"]
	if !ok {
		slog.Warn("invalid event: missing or invalid timestamp")
		http.Error(w, "missing required field: timestamp", http.StatusBadRequest)
		return
	}
	timestampStr, ok := timestampRaw.(string)
	if !ok {
		slog.Warn("invalid event: missing or invalid timestamp")
		http.Error(w, "timestamp must be a string", http.StatusBadRequest)
		return
	}
	ts, err := time.Parse(time.RFC3339, timestampStr)
	if err != nil {
		slog.Warn("invalid event: invalid timestamp format", "timestamp", timestampStr)
		http.Error(w, "timestamp must be RFC3339", http.StatusBadRequest)
		return
	}

	eventID := ""
	if idRaw, ok := payload["event_id"]; ok {
		if idStr, ok := idRaw.(string); ok && idStr != "" {
			eventID = idStr
		}
	}
	if len(eventID) > 256 {
		slog.Warn("invalid event: event_id too long", "length", len(eventID))
		http.Error(w, "event_id must be 256 characters or fewer", http.StatusBadRequest)
		return
	}
	if eventID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			http.Error(w, "failed to generate event_id", http.StatusInternalServerError)
			return
		}
		eventID = id.String()
	}

	event := types.Event{
		EventID:    eventID,
		EventType:  eventType,
		Timestamp:  ts,
		Payload:    payload,
		RawJSON:    raw,
		ReceivedAt: time.Now(),
	}

	select {
	case s.eventOut <- event:
		w.WriteHeader(http.StatusAccepted)
	default:
		slog.Warn("backpressure: input channel full, rejecting event")
		http.Error(w, "server busy", http.StatusTooManyRequests)
	}
}
