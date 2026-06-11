package ingest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vinaysrao1/fruitfly/types"
)

// maxBatchBytes bounds a POST /events/batch request body.
const maxBatchBytes = 8 << 20 // 8MB

// forwardedHeader marks an event already routed by a peer. Forwarded events
// are processed wherever they land — never re-forwarded — which bounds
// routing to one hop and makes loops impossible during ring divergence.
const forwardedHeader = "X-Fruitfly-Forwarded"

// Router decides event ownership in cluster mode and forwards raw events to
// their owning peer. A nil Router means single-node operation.
type Router interface {
	// Owner returns the peer that owns key, and whether that peer is this
	// process.
	Owner(key string) (peer string, self bool)
	// Forward sends body to peer; batch selects the NDJSON batch endpoint.
	Forward(ctx context.Context, peer string, body []byte, batch bool) error
}

// Server is the HTTP event ingestion server.
type Server struct {
	addr         string
	maxBytes     int
	routingField string
	router       Router
	eventOut     chan<- types.Event
}

// NewServer creates the ingestion server. routingField names the payload
// field used as the routing key (events without it route by event ID).
// router enables cluster forwarding; nil means single-node.
func NewServer(addr string, maxBytes int, eventOut chan<- types.Event) *Server {
	return &Server{
		addr:         addr,
		maxBytes:     maxBytes,
		routingField: "entity_id",
		eventOut:     eventOut,
	}
}

// SetRoutingField overrides the payload field used as the routing key.
// Must be called before the server starts handling requests.
func (s *Server) SetRoutingField(field string) {
	if field != "" {
		s.routingField = field
	}
}

// SetRouter enables cluster-mode forwarding. Must be called before the
// server starts handling requests.
func (s *Server) SetRouter(r Router) { s.router = r }

// Handler returns the configured HTTP mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", s.handleEvent)
	mux.HandleFunc("POST /events/batch", s.handleBatch)
	return mux
}

// parseEvent validates one raw JSON event and builds a types.Event.
func (s *Server) parseEvent(raw []byte) (types.Event, error) {
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return types.Event{}, fmt.Errorf("invalid JSON: %w", err)
	}

	eventType, ok := payload["event_type"].(string)
	if !ok || eventType == "" {
		return types.Event{}, errors.New("event_type must be a non-empty string")
	}

	timestampStr, ok := payload["timestamp"].(string)
	if !ok {
		return types.Event{}, errors.New("missing required field: timestamp")
	}
	ts, err := time.Parse(time.RFC3339, timestampStr)
	if err != nil {
		return types.Event{}, errors.New("timestamp must be RFC3339")
	}

	eventID := ""
	if idStr, ok := payload["event_id"].(string); ok {
		eventID = idStr
	}
	if len(eventID) > 256 {
		return types.Event{}, errors.New("event_id must be 256 characters or fewer")
	}
	if eventID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return types.Event{}, fmt.Errorf("generate event_id: %w", err)
		}
		eventID = id.String()
	}

	// Routing key: the configured payload field when present, else the
	// event ID (server-generated IDs spread entity-less events uniformly).
	entityID := eventID
	if v, ok := payload[s.routingField].(string); ok && v != "" {
		entityID = v
	}

	return types.Event{
		EventID:    eventID,
		EventType:  eventType,
		EntityID:   entityID,
		Timestamp:  ts,
		Payload:    payload,
		RawJSON:    raw,
		ReceivedAt: time.Now(),
	}, nil
}

// enqueue offers the event to the pipeline without blocking.
func (s *Server) enqueue(event types.Event) bool {
	select {
	case s.eventOut <- event:
		return true
	default:
		return false
	}
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

	event, err := s.parseEvent(raw)
	if err != nil {
		slog.Warn("invalid event", "error", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Cluster mode: route to the owning peer unless this event was already
	// forwarded once (one-hop bound).
	if s.router != nil && r.Header.Get(forwardedHeader) == "" {
		if peer, self := s.router.Owner(event.EntityID); !self {
			if err := s.router.Forward(r.Context(), peer, raw, false); err == nil {
				w.WriteHeader(http.StatusAccepted)
				return
			}
			// Peer unreachable: process locally rather than drop. Counters
			// for this key are briefly approximate — the same class of
			// degradation as a ring change.
			slog.Warn("forward failed, processing locally", "peer", peer, "error", err)
		}
	}

	if !s.enqueue(event) {
		slog.Warn("backpressure: input channel full, rejecting event")
		http.Error(w, "server busy", http.StatusTooManyRequests)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// batchResponse reports per-batch outcomes for POST /events/batch.
type batchResponse struct {
	Accepted  int `json:"accepted"`
	Forwarded int `json:"forwarded"`
	Rejected  int `json:"rejected"`
}

// handleBatch handles POST /events/batch: NDJSON, one event per line.
// Invalid lines are rejected individually; valid lines are enqueued (or, in
// cluster mode, forwarded to their owning peer grouped per peer). On
// backpressure the remainder of the batch is rejected and 429 is returned
// with the counts so far.
func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBatchBytes)

	var resp batchResponse
	overloaded := false

	// Lines owned by remote peers, grouped per peer for one request each.
	remote := make(map[string][][]byte)
	forwarded := r.Header.Get(forwardedHeader) != ""

	scanner := bufio.NewScanner(r.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), s.maxBytes)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		if overloaded {
			resp.Rejected++
			continue
		}
		// Scanner reuses its buffer; events keep their raw bytes.
		raw := bytes.Clone(line)

		event, err := s.parseEvent(raw)
		if err != nil {
			resp.Rejected++
			continue
		}

		if s.router != nil && !forwarded {
			if peer, self := s.router.Owner(event.EntityID); !self {
				remote[peer] = append(remote[peer], raw)
				continue
			}
		}

		if s.enqueue(event) {
			resp.Accepted++
		} else {
			resp.Rejected++
			overloaded = true
		}
	}
	if err := scanner.Err(); err != nil {
		http.Error(w, fmt.Sprintf("read body: %v", err), http.StatusBadRequest)
		return
	}

	// Forward remote groups, one NDJSON request per peer. On failure, fall
	// back to local processing (degraded counters beat dropped events).
	for peer, lines := range remote {
		body := bytes.Join(lines, []byte("\n"))
		if err := s.router.Forward(r.Context(), peer, body, true); err == nil {
			resp.Forwarded += len(lines)
			continue
		}
		slog.Warn("batch forward failed, processing locally", "peer", peer, "lines", len(lines))
		for _, raw := range lines {
			event, err := s.parseEvent(raw)
			if err != nil {
				resp.Rejected++
				continue
			}
			if s.enqueue(event) {
				resp.Accepted++
			} else {
				resp.Rejected++
				overloaded = true
			}
		}
	}

	status := http.StatusAccepted
	if overloaded {
		status = http.StatusTooManyRequests
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(resp)
}
