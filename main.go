package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/vinaysrao1/fruitfly/cluster"
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
	writer.SetEmitInteresting(cfg.Emit == "interesting")

	// Initialize executor pool.
	pool := executor.NewPool(cfg.Workers, &snapshotPtr, eventTimeout, ruleTimeout)

	// Initialize ingest server.
	ingestServer := ingest.NewServer(cfg.Address, maxEventBytes, eventChan)
	ingestServer.SetRoutingField(cfg.RoutingField)

	// Cluster mode: route events to their owning peer by entity hash, and
	// restrict counter() keys to the routing entity so counts stay exact
	// across pods.
	if len(cfg.ClusterPeers) > 0 {
		router, err := cluster.NewRouter(cfg.ClusterSelf, cfg.ClusterPeers)
		if err != nil {
			slog.Error("invalid cluster configuration", "error", err)
			os.Exit(1)
		}
		ingestServer.SetRouter(router)
		pool.SetCounterAffinity(true)
		slog.Info("cluster mode enabled", "self", cfg.ClusterSelf, "peers", len(cfg.ClusterPeers))
	}

	// Build HTTP mux.
	mux := http.NewServeMux()

	// Mount ingest routes. Go 1.22 ServeMux patterns are exact-match, so
	// "/events" alone would 404 "/events/batch"; register the subtree too.
	mux.Handle("/events", ingestServer.Handler())
	mux.Handle("/events/", ingestServer.Handler())

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
		for i, r := range snap.Rules {
			ewma := r.Stats.EWMA()
			ruleInfos[i] = ruleInfo{
				RuleID:      r.RuleID,
				EventType:   r.EventType,
				Priority:    r.Priority,
				Prefiltered: r.Prefiltered.Load(),
				Evals:       r.Stats.Evals(),
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

	// Admin: trigger rule reload.
	mux.HandleFunc("POST /admin/rules/reload", func(w http.ResponseWriter, r *http.Request) {
		reloader.Reload()
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("reload triggered"))
	})

	// Admin: result counters in Prometheus text format.
	mux.HandleFunc("GET /admin/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		stats := writer.Stats()
		names := make([]string, 0, len(stats))
		for name := range stats {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			fmt.Fprintf(w, "fruitfly_%s %d\n", name, stats[name])
		}
	})

	// Two separate lifecycles. reloadCtx stops background services. pipeCtx
	// governs in-flight evaluation and output; it stays live through the
	// shutdown drain and is cancelled only as a last-resort backstop, so
	// queued events are never evaluated under a cancelled context (which
	// would fail every rule and fall open to approve).
	reloadCtx, reloadCancel := context.WithCancel(context.Background())
	pipeCtx, pipeCancel := context.WithCancel(context.Background())
	defer pipeCancel()

	// Start background goroutines.
	go func() {
		if err := reloader.Run(reloadCtx); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("reloader exited with error", "error", err)
		}
	}()

	writerDone := make(chan error, 1)
	go func() {
		writerDone <- writer.Run(pipeCtx, resultChan)
	}()

	poolDone := make(chan struct{})
	go func() {
		defer close(poolDone)
		pool.Run(pipeCtx, eventChan, resultChan)
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

	// Graceful shutdown sequence. Invariant: a queued event is either
	// evaluated under a healthy context (normal per-event timeouts apply)
	// or not at all — never under a cancelled one.

	// Step 1: Stop accepting new HTTP requests. After this no handler can
	// send to eventChan, so closing it below is safe.
	shutCtx, shutCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutCancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		slog.Error("HTTP server shutdown error", "error", err)
	}

	// Step 2: Stop the reloader; it cannot affect in-flight verdicts.
	reloadCancel()

	// Step 3: Close eventChan — the drain signal. Workers finish queued
	// events under the still-live pipeline context.
	close(eventChan)

	// Step 4: Wait for the pool (it closes resultChan when done). If it
	// exceeds the shutdown budget, force-cancel evaluation as a last
	// resort: remaining rules abort with errors rather than hanging the
	// process.
	select {
	case <-poolDone:
	case <-shutCtx.Done():
		slog.Warn("pool did not drain within shutdown timeout, force-cancelling evaluation")
		pipeCancel()
		select {
		case <-poolDone:
		case <-time.After(2 * time.Second):
			slog.Warn("pool still running after force-cancel")
		}
	}

	// Step 5: Wait for the writer to flush and close DuckDB. Fresh budget:
	// the writer only makes progress once resultChan is closed.
	select {
	case <-writerDone:
	case <-time.After(shutdownTimeout):
		slog.Warn("writer did not finish within shutdown timeout")
	}

	slog.Info("shutdown complete")
}
