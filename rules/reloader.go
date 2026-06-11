package rules

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
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
	lastHash string // content hash of the last successfully compiled rules
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
	if h, err := hashDir(rulesDir); err == nil {
		r.lastHash = h
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
	// Skip recompiling when nothing changed: poll ticks fire every 10s, and
	// republishing an identical snapshot would needlessly mint a new
	// snapshot ID and invalidate every worker's eval cache. The hash is
	// recorded only on successful compiles, so a failed reload is retried
	// until the rules change again.
	hash, hashErr := hashDir(r.rulesDir)
	if hashErr == nil && hash == r.lastHash {
		return
	}

	snap, err := r.compiler.CompileDir(r.rulesDir)
	if err != nil {
		slog.Error("rule reload failed, keeping old snapshot", "error", err)
		return
	}

	if hashErr == nil {
		r.lastHash = hash
	}
	r.snapshot.Store(snap)
	r.Ready.Store(true)

	slog.Info("rules reloaded",
		"snapshot_id", snap.ID,
		"rule_count", len(snap.Rules),
		"loaded_at", snap.LoadedAt,
	)
}

// hashDir returns a content hash of all *.star files in dir (names, sizes,
// and bytes), used to detect whether a reload would change anything.
func hashDir(dir string) (string, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.star"))
	if err != nil {
		return "", err
	}
	sort.Strings(files)
	h := sha256.New()
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(h, "%s\x00%d\x00", filepath.Base(path), len(data))
		h.Write(data)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
