package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type stats struct {
	Total      atomic.Int64
	approve    atomic.Int64
	block      atomic.Int64
	review     atomic.Int64
	ByComposite sync.Map // "post:block" -> *atomic.Int64
}

func (s *stats) incrementVerdict(verdict string) {
	switch verdict {
	case "approve":
		s.approve.Add(1)
	case "block":
		s.block.Add(1)
	case "review":
		s.review.Add(1)
	}
}

func (s *stats) incrementComposite(eventType, verdict string) {
	key := eventType + ":" + verdict
	val, _ := s.ByComposite.LoadOrStore(key, &atomic.Int64{})
	val.(*atomic.Int64).Add(1)
}

func (s *stats) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	var result struct {
		EventType    string `json:"EventType"`
		FinalVerdict string `json:"FinalVerdict"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	s.Total.Add(1)
	s.incrementVerdict(result.FinalVerdict)
	if result.EventType != "" && result.FinalVerdict != "" {
		s.incrementComposite(result.EventType, result.FinalVerdict)
	}

	w.WriteHeader(http.StatusOK)
}

func (s *stats) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	byComposite := make(map[string]int64)
	s.ByComposite.Range(func(k, v any) bool {
		byComposite[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})

	resp := map[string]any{
		"total": s.Total.Load(),
		"by_verdict": map[string]int64{
			"approve": s.approve.Load(),
			"block":   s.block.Load(),
			"review":  s.review.Load(),
		},
		"by_composite": byComposite,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("failed to encode stats response: %v", err)
	}
}

func (s *stats) logPeriodically(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fmt.Printf("[receiver] total=%d approve=%d block=%d review=%d\n",
				s.Total.Load(),
				s.approve.Load(),
				s.block.Load(),
				s.review.Load(),
			)
		}
	}
}

func main() {
	addr := flag.String("addr", ":9090", "listen address")
	flag.Parse()

	s := &stats{}

	mux := http.NewServeMux()
	mux.HandleFunc("/webhook", s.handleWebhook)
	mux.HandleFunc("/stats", s.handleStats)

	server := &http.Server{
		Addr:    *addr,
		Handler: mux,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.logPeriodically(ctx)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("[receiver] shutting down...")
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("[receiver] shutdown error: %v", err)
		}
	}()

	log.Printf("[receiver] listening on %s", *addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("[receiver] server error: %v", err)
	}
}
