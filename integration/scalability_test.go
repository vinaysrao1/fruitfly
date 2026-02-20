package integration_test

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestThroughput_100EventsPerSecond sends 100 events/sec for 10 seconds (1000 total)
// using 10 concurrent goroutines (each at 10 eps), and asserts DuckDB row count >= 900,
// all accepted events present in DuckDB, P50 < 50ms, P99 < 200ms, heap growth < 50MB.
func TestThroughput_100EventsPerSecond(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput test in short mode")
	}

	numWorkers := runtime.NumCPU()
	opts := testPipelineOpts{
		eventChanCap:  100,
		resultChanCap: 100,
		workerCount:   numWorkers,
	}
	tp := newTestPipeline(t, approveAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	handler := tp.ingestServer.Handler()

	// Capture heap baseline before load.
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// 10 goroutines, each sending 100 events at 10 eps (100ms tick) = 100 eps total.
	const numGoroutines = 10
	const eventsPerGoroutine = 100
	const goroutineTickInterval = 100 * time.Millisecond // 10 eps per goroutine

	var accepted atomic.Int64
	var rejected atomic.Int64

	// eventSeq provides a monotonically increasing counter for unique event IDs
	// across all concurrent goroutines, avoiding duplicate-key errors in DuckDB.
	var eventSeq atomic.Int64

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			ticker := time.NewTicker(goroutineTickInterval)
			defer ticker.Stop()
			sent := 0
			for range ticker.C {
				seq := eventSeq.Add(1)
				event := map[string]any{
					"event_id":   fmt.Sprintf("throughput-g%d-e%d", goroutineID, seq),
					"event_type": "test",
					"timestamp":  time.Now().UTC().Format(time.RFC3339),
					"payload":    map[string]any{"source": "throughput"},
				}
				resp := postEvent(t, handler, event)
				switch resp.Code {
				case 202:
					accepted.Add(1)
				case 429:
					rejected.Add(1)
				default:
					t.Errorf("unexpected status %d", resp.Code)
				}
				sent++
				if sent >= eventsPerGoroutine {
					return
				}
			}
		}(g)
	}

	// Wait for all sender goroutines to finish.
	wg.Wait()

	// Drain pipeline: shutdown waits for pool to drain and writer to flush all results.
	tp.shutdown(t, cancel, poolDone, writerDone)

	// Capture heap after load.
	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	acceptedCount := int(accepted.Load())
	t.Logf("Sent %d total events: accepted=%d, rejected=%d", numGoroutines*eventsPerGoroutine, acceptedCount, rejected.Load())

	// Assert DuckDB row count >= 900 (allowing for a small number of 429s under concurrency).
	dbCount := tp.queryDBCount(t)
	if dbCount < 900 {
		t.Errorf("expected >= 900 rows in DuckDB, got %d", dbCount)
	}

	// Assert all accepted events are present in DuckDB (no silent drops).
	if dbCount != acceptedCount {
		t.Errorf("accepted count %d != DuckDB row count %d (silent data loss)", acceptedCount, dbCount)
	}

	// Assert P50 < 50ms and P99 < 200ms.
	p50us, p99us := tp.queryDBLatencyPercentiles(t)
	const p50ThresholdUS = 50 * 1000  // 50ms in microseconds
	const p99ThresholdUS = 200 * 1000 // 200ms in microseconds
	if p50us >= p50ThresholdUS {
		t.Errorf("P50 latency %dµs exceeds threshold %dµs (50ms)", p50us, p50ThresholdUS)
	}
	if p99us >= p99ThresholdUS {
		t.Errorf("P99 latency %dµs exceeds threshold %dµs (200ms)", p99us, p99ThresholdUS)
	}

	// Assert heap growth < 50MB.
	const maxHeapGrowthBytes = 50 * 1024 * 1024
	if memAfter.HeapInuse > memBefore.HeapInuse+maxHeapGrowthBytes {
		heapGrowthMB := float64(memAfter.HeapInuse-memBefore.HeapInuse) / (1024 * 1024)
		t.Errorf("heap grew by %.1fMB, exceeds 50MB threshold", heapGrowthMB)
	}

	// Verify webhook was operational (webhook channel buffer is 200, so we expect
	// at least that many deliveries captured even though 1000 were sent).
	webhookCount := len(tp.webhookCh)
	t.Logf("Webhook deliveries captured: %d (channel buffer capped at 200)", webhookCount)
	if webhookCount == 0 {
		t.Error("expected webhook deliveries but got zero")
	}
}

// TestThroughput_BurstAbsorption sends 500 events as fast as possible and asserts
// >= 100 accepted, drain time < 5s, and accepted count matches DuckDB row count.
func TestThroughput_BurstAbsorption(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping burst absorption test in short mode")
	}

	opts := testPipelineOpts{
		eventChanCap:  100,
		resultChanCap: 100,
		workerCount:   runtime.NumCPU(),
	}
	tp := newTestPipeline(t, approveAllRule, opts, "")
	cancel, poolDone, writerDone := tp.start(t)

	handler := tp.ingestServer.Handler()

	const burstCount = 500

	var accepted atomic.Int64
	var rejected atomic.Int64

	// Send all 500 events in a tight loop (no rate limiting).
	for i := 0; i < burstCount; i++ {
		event := makeEvent("test")
		resp := postEvent(t, handler, event)
		switch resp.Code {
		case 202:
			accepted.Add(1)
		case 429:
			rejected.Add(1)
		default:
			t.Errorf("unexpected status %d", resp.Code)
		}
	}

	acceptedCount := int(accepted.Load())

	// Assert at least 100 events were accepted (channel buffer absorbs them).
	if acceptedCount < 100 {
		t.Errorf("expected at least 100 accepted events, got %d", acceptedCount)
	}

	// Measure drain time: shutdown waits for pool to drain and writer to flush all results.
	drainStart := time.Now()
	tp.shutdown(t, cancel, poolDone, writerDone)
	drainTime := time.Since(drainStart)

	// Assert drain time < 5s.
	if drainTime > 5*time.Second {
		t.Errorf("drain time %v exceeds 5s threshold", drainTime)
	}

	// Assert accepted count == DuckDB row count (no silent drops).
	dbCount := tp.queryDBCount(t)
	if dbCount != acceptedCount {
		t.Errorf("accepted count %d != DuckDB row count %d (silent data loss)", acceptedCount, dbCount)
	}
}
