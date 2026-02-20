package rules

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
)

const (
	pollInterval  = 10 * time.Second
	debounceDelay = 100 * time.Millisecond
)

// Reloader watches for rule changes and publishes new snapshots.
type Reloader struct {
	compiler *Compiler
	rulesDir string
	snapshot *atomic.Pointer[Snapshot]
	reloadCh chan struct{}
	Ready    atomic.Bool
}

// NewReloader creates a Reloader. Performs initial compilation and stores
// the first snapshot. Returns error if initial compilation fails.
func NewReloader(compiler *Compiler, rulesDir string,
	snapshot *atomic.Pointer[Snapshot]) (*Reloader, error) {

	snap, err := compiler.CompileDir(rulesDir)
	if err != nil {
		return nil, err
	}
	snapshot.Store(snap)

	r := &Reloader{
		compiler: compiler,
		rulesDir: rulesDir,
		snapshot: snapshot,
		reloadCh: make(chan struct{}, 1),
	}
	r.Ready.Store(true)

	slog.Info("initial rules loaded",
		"snapshot_id", snap.ID,
		"rule_count", len(snap.Rules),
		"loaded_at", snap.LoadedAt,
	)

	return r, nil
}

// Run starts the watch loop. Blocks until ctx is cancelled.
func (r *Reloader) Run(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		slog.Warn("fsnotify unavailable, using poll-only", "error", err)
		watcher = nil
	} else if err := watcher.Add(r.rulesDir); err != nil {
		slog.Warn("fsnotify watch failed, using poll-only", "error", err)
		watcher.Close()
		watcher = nil
	}
	if watcher != nil {
		defer watcher.Close()
	}

	pollTicker := time.NewTicker(pollInterval)
	defer pollTicker.Stop()

	var debounceTimer *time.Timer
	defer func() {
		if debounceTimer != nil {
			debounceTimer.Stop()
		}
	}()
	var debounceCh <-chan time.Time

	for {
		var fsEvents <-chan fsnotify.Event
		var fsErrors <-chan error
		if watcher != nil {
			fsEvents = watcher.Events
			fsErrors = watcher.Errors
		}

		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-r.reloadCh:
			r.doReload()

		case <-pollTicker.C:
			r.doReload()

		case <-debounceCh:
			debounceTimer = nil
			debounceCh = nil
			r.doReload()

		case _, ok := <-fsEvents:
			if !ok {
				watcher = nil
				continue
			}
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			debounceTimer = time.NewTimer(debounceDelay)
			debounceCh = debounceTimer.C

		case err, ok := <-fsErrors:
			if !ok {
				watcher = nil
				continue
			}
			slog.Warn("fsnotify error", "error", err)
		}
	}
}

// Reload signals an immediate recompilation. Non-blocking, fire-and-forget.
func (r *Reloader) Reload() {
	select {
	case r.reloadCh <- struct{}{}:
	default:
	}
}

func (r *Reloader) doReload() {
	snap, err := r.compiler.CompileDir(r.rulesDir)
	if err != nil {
		slog.Error("rule reload failed, keeping old snapshot", "error", err)
		return
	}

	r.snapshot.Store(snap)
	r.Ready.Store(true)

	slog.Info("rules reloaded",
		"snapshot_id", snap.ID,
		"rule_count", len(snap.Rules),
		"loaded_at", snap.LoadedAt,
	)
}
