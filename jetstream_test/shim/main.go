package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
)

// jetstreamEvent is the top-level Jetstream message.
type jetstreamEvent struct {
	DID    string           `json:"did"`
	TimeUS int64            `json:"time_us"`
	Kind   string           `json:"kind"`
	Commit *jetstreamCommit `json:"commit"`
}

type jetstreamCommit struct {
	Rev        string         `json:"rev"`
	Operation  string         `json:"operation"`
	Collection string         `json:"collection"`
	RKey       string         `json:"rkey"`
	Record     map[string]any `json:"record"`
	CID        string         `json:"cid"`
}

func transformPost(evt jetstreamEvent) map[string]any {
	commit := evt.Commit
	record := commit.Record

	text, _ := record["text"].(string)

	var timestamp string
	if createdAt, ok := record["createdAt"].(string); ok && createdAt != "" {
		timestamp = createdAt
	} else {
		timestamp = time.Now().UTC().Format(time.RFC3339)
	}

	var langs []string
	if rawLangs, ok := record["langs"]; ok {
		if langSlice, ok := rawLangs.([]any); ok {
			for _, l := range langSlice {
				if s, ok := l.(string); ok {
					langs = append(langs, s)
				}
			}
		}
	}

	isReply := record["reply"] != nil

	return map[string]any{
		"event_id":   fmt.Sprintf("%s/%s/%s", evt.DID, commit.Collection, commit.RKey),
		"event_type": "post",
		"timestamp":  timestamp,
		"entity_id":  evt.DID,
		"text":       text,
		"char_count": utf8.RuneCountInString(text),
		"langs":      langs,
		"is_reply":   isReply,
		"rkey":       commit.RKey,
		"cid":        commit.CID,
	}
}

func transformLike(evt jetstreamEvent) map[string]any {
	commit := evt.Commit
	record := commit.Record

	var timestamp string
	if createdAt, ok := record["createdAt"].(string); ok && createdAt != "" {
		timestamp = createdAt
	} else {
		timestamp = time.Now().UTC().Format(time.RFC3339)
	}

	var subjectURI, subjectCID string
	if subjectRaw, ok := record["subject"]; ok {
		if subjectMap, ok := subjectRaw.(map[string]any); ok {
			subjectURI, _ = subjectMap["uri"].(string)
			subjectCID, _ = subjectMap["cid"].(string)
		}
	}

	return map[string]any{
		"event_id":    fmt.Sprintf("%s/%s/%s", evt.DID, commit.Collection, commit.RKey),
		"event_type":  "like",
		"timestamp":   timestamp,
		"entity_id":   evt.DID,
		"subject_uri": subjectURI,
		"subject_cid": subjectCID,
		"rkey":        commit.RKey,
		"cid":         commit.CID,
	}
}

func postToFruitfly(ctx context.Context, client *http.Client, url string, payload map[string]any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	backoff := 100 * time.Millisecond
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/events", bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("new request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("http post: %w", err)
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusAccepted {
			return nil
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			continue
		}
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}
	return fmt.Errorf("dropped after 3 retries (429 backpressure)")
}

func printStats(sent, skipped, dropped, errors *atomic.Int64) {
	log.Printf("[shim] sent=%d skipped=%d dropped=%d errors=%d",
		sent.Load(), skipped.Load(), dropped.Load(), errors.Load())
}

func writeStatsFile(sent, skipped, dropped, errors *atomic.Int64) {
	content := fmt.Sprintf("sent=%d\nskipped=%d\ndropped=%d\nerrors=%d\n",
		sent.Load(), skipped.Load(), dropped.Load(), errors.Load())
	if err := os.WriteFile("/tmp/jetstream_shim_stats.txt", []byte(content), 0644); err != nil {
		log.Printf("[shim] failed to write stats file: %v", err)
	}
}

func main() {
	jetstreamURL := flag.String("jetstream-url", "wss://jetstream2.us-east.bsky.network/subscribe", "Jetstream WebSocket URL")
	fruitflyURL := flag.String("fruitfly-url", "http://localhost:8080", "Fruitfly base URL")
	maxEvents := flag.Int64("max-events", 1000, "Stop after sending this many events (0 = unlimited)")
	concurrency := flag.Int("concurrency", 10, "Max concurrent HTTP POST goroutines")
	flag.Parse()

	var sent, skipped, dropped, errors atomic.Int64

	// Build WebSocket URL with query params.
	wsURL := *jetstreamURL + "?wantedCollections=app.bsky.feed.post&wantedCollections=app.bsky.feed.like"

	// Dispatched tracks events handed off to POST goroutines (pre-increment).
	var dispatched atomic.Int64

	// Semaphore for concurrency control.
	sem := make(chan struct{}, *concurrency)

	// HTTP client shared across goroutines.
	httpClient := &http.Client{Timeout: 10 * time.Second}

	// Context for graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown on SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// WaitGroup to track in-flight POSTs.
	var wg sync.WaitGroup

	// Periodic stats printer.
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				printStats(&sent, &skipped, &dropped, &errors)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Done channel signals main loop completion.
	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
			if err != nil {
				log.Printf("[shim] websocket connect error: %v, retrying in 1s", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				continue
			}

			log.Printf("[shim] connected to Jetstream")

			disconnected := false
			for !disconnected {
				// Check if we've dispatched enough events.
				if *maxEvents > 0 && dispatched.Load() >= *maxEvents {
					conn.Close()
					return
				}

				select {
				case <-ctx.Done():
					conn.Close()
					return
				default:
				}

				_, msg, err := conn.ReadMessage()
				if err != nil {
					errors.Add(1)
					log.Printf("[shim] websocket read error: %v, reconnecting in 1s", err)
					conn.Close()
					select {
					case <-ctx.Done():
						return
					case <-time.After(time.Second):
					}
					disconnected = true
					continue
				}

				var evt jetstreamEvent
				if err := json.Unmarshal(msg, &evt); err != nil {
					skipped.Add(1)
					continue
				}

				// Filter: only process commit events with create operation.
				if evt.Kind != "commit" || evt.Commit == nil {
					skipped.Add(1)
					continue
				}
				commit := evt.Commit
				if commit.Operation != "create" {
					skipped.Add(1)
					continue
				}
				if commit.Collection != "app.bsky.feed.post" && commit.Collection != "app.bsky.feed.like" {
					skipped.Add(1)
					continue
				}
				if commit.Record == nil {
					skipped.Add(1)
					continue
				}

				// Transform event.
				var payload map[string]any
				switch commit.Collection {
				case "app.bsky.feed.post":
					payload = transformPost(evt)
				case "app.bsky.feed.like":
					payload = transformLike(evt)
				default:
					skipped.Add(1)
					continue
				}

				// Acquire semaphore slot.
				dispatched.Add(1)
				sem <- struct{}{}
				wg.Add(1)
				capturedPayload := payload
				go func() {
					defer func() {
						<-sem
						wg.Done()
					}()
					if err := postToFruitfly(ctx, httpClient, *fruitflyURL, capturedPayload); err != nil {
						if err.Error() == "dropped after 3 retries (429 backpressure)" {
							dropped.Add(1)
						} else {
							dropped.Add(1)
							log.Printf("[shim] post error: %v", err)
						}
						return
					}
					sent.Add(1)
				}()
			}
		}
	}()

	// Wait for signal or done.
	select {
	case sig := <-sigCh:
		log.Printf("[shim] received signal %v, shutting down", sig)
		cancel()
	case <-done:
		// Reached max events.
	}

	// Wait for in-flight POSTs with 5s timeout.
	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		log.Printf("[shim] timed out waiting for in-flight POSTs")
	}

	ticker.Stop()

	log.Printf("[shim] DONE. Final: sent=%d skipped=%d dropped=%d errors=%d",
		sent.Load(), skipped.Load(), dropped.Load(), errors.Load())

	writeStatsFile(&sent, &skipped, &dropped, &errors)
}
