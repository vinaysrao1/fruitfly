package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/vinaysrao1/fruitfly/config"
	"github.com/vinaysrao1/fruitfly/executor"
	"github.com/vinaysrao1/fruitfly/ingest"
	"github.com/vinaysrao1/fruitfly/output"
	"github.com/vinaysrao1/fruitfly/rules"
	"github.com/vinaysrao1/fruitfly/types"
)

const (
	maxEventBytes   = 256 * 1024 // 256KB
	eventChanCap    = 100
	resultChanCap   = 100
	eventTimeout    = 5 * time.Second
	ruleTimeout     = 1 * time.Second
	shutdownTimeout = 10 * time.Second
)

func main() {
	configPath := flag.String("config", "fruitfly.yaml", "path to YAML config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// Configure slog based on log_level.
	var logLevel slog.Level
	switch cfg.LogLevel {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	// Create pipeline channels.
	eventChan := make(chan types.Event, eventChanCap)
	resultChan := make(chan types.Result, resultChanCap)

	// Create atomic snapshot pointer.
	var snapshotPtr atomic.Pointer[rules.Snapshot]

	// Initialize rules compiler with full UDFs.
	compiler := &rules.Compiler{UDFs: rules.DefaultUDFs()}

	// Initialize reloader (performs initial compilation).
	reloader, err := rules.NewReloader(compiler, cfg.RulesDir, &snapshotPtr)
	if err != nil {
		slog.Error("failed to load initial rules", "rules_dir", cfg.RulesDir, "error", err)
		os.Exit(1)
	}

	// Initialize output writer.
	writer, err := output.NewWriter(cfg.DuckDBPath, cfg.WebhookURL)
	if err != nil {
		slog.Error("failed to open DuckDB", "path", cfg.DuckDBPath, "error", err)
		os.Exit(1)
	}

	// Initialize executor pool.
	pool := executor.NewPool(cfg.Workers, &snapshotPtr, eventTimeout, ruleTimeout)

	// Initialize ingest server.
	ingestServer := ingest.NewServer(cfg.Address, maxEventBytes, eventChan)

	// Build HTTP mux.
	mux := http.NewServeMux()

	// Mount ingest handler - it already has POST /events registered.
	mux.Handle("/events", ingestServer.Handler())

	// Admin: liveness check (always 200).
	mux.HandleFunc("GET /admin/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// Admin: readiness check (200 when both reloader and writer are ready).
	mux.HandleFunc("GET /admin/ready", func(w http.ResponseWriter, r *http.Request) {
		if reloader.Ready.Load() && writer.Ready.Load() {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
		} else {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
		}
	})

	// Admin: current snapshot info.
	mux.HandleFunc("GET /admin/rules", func(w http.ResponseWriter, r *http.Request) {
		snap := snapshotPtr.Load()
		if snap == nil {
			http.Error(w, "no snapshot loaded", http.StatusServiceUnavailable)
			return
		}
		type ruleInfo struct {
			RuleID    string `json:"rule_id"`
			EventType string `json:"event_type"`
			Priority  int    `json:"priority"`
		}
		type response struct {
			ID        string     `json:"id"`
			RuleCount int        `json:"rule_count"`
			LoadedAt  time.Time  `json:"loaded_at"`
			Rules     []ruleInfo `json:"rules"`
		}
		ruleInfos := make([]ruleInfo, len(snap.Rules))
		for i, r := range snap.Rules {
			ruleInfos[i] = ruleInfo{
				RuleID:    r.RuleID,
				EventType: r.EventType,
				Priority:  r.Priority,
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

	// Admin: trigger rule reload.
	mux.HandleFunc("POST /admin/rules/reload", func(w http.ResponseWriter, r *http.Request) {
		reloader.Reload()
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("reload triggered"))
	})

	// Admin: metrics placeholder.
	mux.HandleFunc("GET /admin/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("# metrics placeholder\n"))
	})

	// Main context for graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())

	// Start background goroutines.
	go func() {
		if err := reloader.Run(ctx); err != nil {
			slog.Error("reloader exited with error", "error", err)
		}
	}()

	writerDone := make(chan error, 1)
	go func() {
		writerDone <- writer.Run(ctx, resultChan)
	}()

	poolDone := make(chan struct{})
	go func() {
		defer close(poolDone)
		pool.Run(ctx, eventChan, resultChan)
	}()

	// Start HTTP server.
	srv := &http.Server{
		Addr:    cfg.Address,
		Handler: mux,
	}

	srvErr := make(chan error, 1)
	go func() {
		slog.Info("fruitfly starting", "address", cfg.Address)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			srvErr <- err
		}
	}()

	// Wait for shutdown signal or server error.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-sigCh:
		slog.Info("signal received, shutting down", "signal", sig)
	case err := <-srvErr:
		slog.Error("HTTP server error", "error", err)
	}

	// 5-step graceful shutdown sequence.

	// Step 1: Stop accepting new HTTP requests.
	shutCtx, shutCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutCancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		slog.Error("HTTP server shutdown error", "error", err)
	}

	// Step 2: Cancel context (stops reloader) and close eventChan (signals workers to drain).
	cancel()
	close(eventChan)

	// Step 3: Wait for pool to finish processing all in-flight events (it closes resultChan).
	select {
	case <-poolDone:
	case <-shutCtx.Done():
		slog.Warn("pool did not finish within shutdown timeout")
	}

	// Step 4: Wait for writer to flush and close DuckDB.
	select {
	case <-writerDone:
	case <-shutCtx.Done():
		slog.Warn("writer did not finish within shutdown timeout")
	}

	// Step 5: Log completion.
	slog.Info("shutdown complete")
}
