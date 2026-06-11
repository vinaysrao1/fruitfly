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

// peerStatus extracts the HTTP status from a Forward error when the peer
// actually responded (as opposed to a transport failure).
func peerStatus(err error) (int, bool) {
	var se interface{ StatusCode() int }
	if errors.As(err, &se) {
		return se.StatusCode(), true
	}
	return 0, false
}

// handleEvent handles POST /events.
func (s *Server) handleEvent(w http.ResponseWriter, r *http.Request) {
	if !contentTypeOK(w, r) {
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
			err := s.router.Forward(r.Context(), peer, raw, false)
			if err == nil {
				w.WriteHeader(http.StatusAccepted)
				return
			}
			if code, ok := peerStatus(err); ok {
				// The peer received the event and refused it; its answer is
				// authoritative. Propagate backpressure instead of breaking
				// affinity by processing locally.
				status := http.StatusBadGateway
				if code == http.StatusTooManyRequests {
					status = http.StatusTooManyRequests
				}
				slog.Warn("owning peer rejected event", "peer", peer, "status", code)
				http.Error(w, fmt.Sprintf("owning peer rejected event (%d)", code), status)
				return
			}
			// Transport failure: the peer never received the bytes, so local
			// processing cannot duplicate. Counters for this key are briefly
			// approximate — the same class of degradation as a ring change.
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

func contentTypeOK(w http.ResponseWriter, r *http.Request) bool {
	ct := r.Header.Get("Content-Type")
	if ct != "" && !strings.HasPrefix(ct, "application/json") {
		slog.Warn("invalid event: unsupported content type", "content_type", ct)
		http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
		return false
	}
	return true
}

// batchResponse reports per-batch outcomes for POST /events/batch.
type batchResponse struct {
	Accepted  int `json:"accepted"`
	Forwarded int `json:"forwarded"`
	Rejected  int `json:"rejected"`
}

// remoteGroup collects a peer's share of a batch: raw lines for forwarding
// plus their already-parsed events for transport-failure fallback.
type remoteGroup struct {
	raws   [][]byte
	events []types.Event
}

// handleBatch handles POST /events/batch: NDJSON, one event per line.
// Per-line contract: invalid or oversized lines are rejected individually
// and never abort the batch. Valid lines are enqueued or, in cluster mode,
// forwarded to their owning peer grouped per peer. On backpressure the
// remainder is rejected and 429 is returned with the counts. Retrying a
// partially-accepted batch is safe only when events carry client event IDs
// (downstream persistence dedupes on event_id).
func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	if !contentTypeOK(w, r) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBatchBytes)

	var resp batchResponse
	overloaded := false
	readStatus := 0

	remote := make(map[string]*remoteGroup)
	forwarded := r.Header.Get(forwardedHeader) != ""

	reader := bufio.NewReaderSize(r.Body, 64*1024)
	for {
		line, err := reader.ReadBytes('\n')
		if err == nil || err == io.EOF {
			s.batchLine(bytes.TrimSpace(line), forwarded, &resp, &overloaded, remote)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			// A partial line was discarded; everything before it is already
			// accounted for. Report what happened with the counts so far.
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				readStatus = http.StatusRequestEntityTooLarge
			} else {
				readStatus = http.StatusBadRequest
			}
			break
		}
	}

	// Forward remote groups, one NDJSON request per peer.
	for peer, group := range remote {
		body := bytes.Join(group.raws, []byte("\n"))
		err := s.router.Forward(r.Context(), peer, body, true)
		if err == nil {
			resp.Forwarded += len(group.raws)
			continue
		}
		if code, ok := peerStatus(err); ok {
			// The peer received the sub-batch and may have processed part
			// of it before answering (e.g. its accepted prefix under
			// backpressure). Replaying locally would duplicate events, so
			// count the sub-batch rejected and surface backpressure.
			slog.Warn("peer rejected forwarded batch", "peer", peer, "status", code, "lines", len(group.raws))
			resp.Rejected += len(group.raws)
			if code == http.StatusTooManyRequests {
				overloaded = true
			}
			continue
		}
		// Transport failure: the peer never received the bytes; process
		// locally rather than drop (degraded counters beat lost events).
		slog.Warn("batch forward failed, processing locally", "peer", peer, "lines", len(group.raws))
		for _, event := range group.events {
			if s.enqueue(event) {
				resp.Accepted++
			} else {
				resp.Rejected++
				overloaded = true
			}
		}
	}

	status := http.StatusAccepted
	switch {
	case readStatus != 0:
		status = readStatus
	case overloaded:
		status = http.StatusTooManyRequests
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(resp)
}

// batchLine handles one NDJSON line: validate, then enqueue locally or
// stage for forwarding to the owning peer.
func (s *Server) batchLine(line []byte, forwarded bool, resp *batchResponse, overloaded *bool, remote map[string]*remoteGroup) {
	if len(line) == 0 {
		return
	}
	if len(line) > s.maxBytes {
		resp.Rejected++ // oversized line: reject it alone, not the batch
		return
	}
	if *overloaded {
		resp.Rejected++
		return
	}
	// ReadBytes allocates per line, but defensively clone: events keep
	// their raw bytes for the lifetime of the pipeline.
	raw := bytes.Clone(line)

	event, err := s.parseEvent(raw)
	if err != nil {
		resp.Rejected++
		return
	}

	if s.router != nil && !forwarded {
		if peer, self := s.router.Owner(event.EntityID); !self {
			g := remote[peer]
			if g == nil {
				g = &remoteGroup{}
				remote[peer] = g
			}
			g.raws = append(g.raws, raw)
			g.events = append(g.events, event)
			return
		}
	}

	if s.enqueue(event) {
		resp.Accepted++
	} else {
		resp.Rejected++
		*overloaded = true
	}
}
