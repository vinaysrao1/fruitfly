#!/usr/bin/env bash
set -euo pipefail

MINUTES="${1:-2}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DUCKDB_PATH="$PROJECT_DIR/jetstream_test.duckdb"

# PIDs for cleanup trap (initialized to empty so trap doesn't fail before assignment).
FRUITFLY_PID=""
RECEIVER_PID=""
SHIM_PID=""

cleanup() {
    if [[ -n "$SHIM_PID" ]]; then
        kill "$SHIM_PID" 2>/dev/null || true
        wait "$SHIM_PID" 2>/dev/null || true
        SHIM_PID=""
    fi
    if [[ -n "$FRUITFLY_PID" ]]; then
        kill "$FRUITFLY_PID" 2>/dev/null || true
    fi
    if [[ -n "$RECEIVER_PID" ]]; then
        kill "$RECEIVER_PID" 2>/dev/null || true
    fi
    rm -rf "$SCRIPT_DIR/bin"
}
trap cleanup EXIT

# Clean up from previous runs.
rm -f "$DUCKDB_PATH"
rm -f /tmp/jetstream_shim_stats.txt

# Fail fast if ports are still in use.
if lsof -ti :8080 >/dev/null 2>&1; then
    echo "ERROR: Port 8080 is already in use. Kill the stale process first."
    exit 1
fi
if lsof -ti :9090 >/dev/null 2>&1; then
    echo "ERROR: Port 9090 is already in use. Kill the stale process first."
    exit 1
fi

echo "=== Jetstream Timed Integration Test ==="
echo "Duration: ${MINUTES} minute(s)"
echo ""

# Step 1: Build all binaries.
echo "[1/7] Building binaries..."
mkdir -p "$SCRIPT_DIR/bin"
(cd "$PROJECT_DIR" && go build -o "$SCRIPT_DIR/bin/fruitfly" .)
(cd "$SCRIPT_DIR/shim" && go build -o "$SCRIPT_DIR/bin/shim" .)
(cd "$SCRIPT_DIR/receiver" && go build -o "$SCRIPT_DIR/bin/receiver" .)
(cd "$SCRIPT_DIR/validate" && go build -o "$SCRIPT_DIR/bin/validate" .)

# Step 2: Start the webhook receiver.
echo "[2/7] Starting webhook receiver on :9090..."
"$SCRIPT_DIR/bin/receiver" -addr=":9090" &
RECEIVER_PID=$!

# Give receiver a moment to bind.
sleep 1

# Step 3: Start Fruitfly.
echo "[3/7] Starting Fruitfly on :8080..."
(cd "$PROJECT_DIR" && exec "$SCRIPT_DIR/bin/fruitfly" -config="$SCRIPT_DIR/fruitfly.yaml") &
FRUITFLY_PID=$!

# Wait for Fruitfly to be ready (poll up to 30 times with 0.5s sleep = 15s max).
READY=0
for i in $(seq 1 30); do
    if curl -sf http://localhost:8080/admin/ready > /dev/null 2>&1; then
        READY=1
        break
    fi
    sleep 0.5
done

if [[ "$READY" -eq 0 ]]; then
    echo "ERROR: Fruitfly did not become ready within 15 seconds"
    exit 1
fi
echo "  Fruitfly ready."

# Step 4: Start the shim in background with unlimited events.
echo "[4/7] Starting shim (unlimited events, background)..."
"$SCRIPT_DIR/bin/shim" \
    -fruitfly-url="http://localhost:8080" \
    -max-events=0 \
    -concurrency=10 &
SHIM_PID=$!

# Record start time.
START_TIME=$(date +%s)
DURATION_SECS=$(( MINUTES * 60 ))

echo ""
echo "  Running for ${MINUTES} minute(s)... (Ctrl+C to abort)"
echo ""

# Step 5: Sleep loop with periodic status (every 30s).
ELAPSED=0
NEXT_STATUS=30
while [[ "$ELAPSED" -lt "$DURATION_SECS" ]]; do
    REMAINING=$(( DURATION_SECS - ELAPSED ))
    SLEEP_FOR=$(( REMAINING < 30 ? REMAINING : 30 ))
    sleep "$SLEEP_FOR"
    ELAPSED=$(( $(date +%s) - START_TIME ))

    if [[ "$ELAPSED" -lt "$DURATION_SECS" ]]; then
        RECEIVER_STATS=$(curl -sf http://localhost:9090/stats 2>/dev/null || echo "unavailable")
        echo "  [$(date '+%H:%M:%S')] Elapsed: ${ELAPSED}s / ${DURATION_SECS}s | Receiver stats: ${RECEIVER_STATS}"
    fi
done

ACTUAL_DURATION=$(( $(date +%s) - START_TIME ))

# Step 6: Stop the shim gracefully (SIGTERM triggers stats flush).
echo ""
echo "[5/7] Stopping shim after ${ACTUAL_DURATION}s..."
kill -TERM "$SHIM_PID" 2>/dev/null || true
wait "$SHIM_PID" 2>/dev/null || true
SHIM_PID=""

# Read shim stats: parse the sent=N line from the stats file.
SHIM_SENT=0
if [[ -f /tmp/jetstream_shim_stats.txt ]]; then
    SHIM_SENT=$(grep '^sent=' /tmp/jetstream_shim_stats.txt | cut -d= -f2 || echo "0")
fi
echo "  Shim sent: $SHIM_SENT events"

# Step 7: Wait for pipeline to drain.
echo "[6/7] Waiting for pipeline drain (10 seconds)..."
sleep 10

# Stop Fruitfly gracefully (SIGTERM triggers graceful shutdown).
echo "[7/7] Shutting down Fruitfly..."
kill -TERM "$FRUITFLY_PID" 2>/dev/null || true
wait "$FRUITFLY_PID" 2>/dev/null || true
FRUITFLY_PID=""

# Small delay for DuckDB to fully close.
sleep 2

# Guard against zero events (stats file missing or corrupt).
if [[ "$SHIM_SENT" -eq 0 ]]; then
    echo "ERROR: Shim reported 0 events sent. Check shim logs."
    exit 1
fi

# Run validation.
echo ""
echo "=== Validation ==="
VALIDATE_EXIT=0
"$SCRIPT_DIR/bin/validate" \
    -receiver-url="http://localhost:9090" \
    -duckdb-path="$DUCKDB_PATH" \
    -shim-sent="$SHIM_SENT" || VALIDATE_EXIT=$?

# Shut down receiver.
kill -TERM "$RECEIVER_PID" 2>/dev/null || true
wait "$RECEIVER_PID" 2>/dev/null || true
RECEIVER_PID=""

# Print throughput summary.
DURATION_MINS=$(( ACTUAL_DURATION / 60 ))
DURATION_SECS_REMAINDER=$(( ACTUAL_DURATION % 60 ))
if [[ "$ACTUAL_DURATION" -gt 0 ]]; then
    # Use awk for floating point division.
    EVENTS_PER_SEC=$(awk "BEGIN { printf \"%.2f\", $SHIM_SENT / $ACTUAL_DURATION }")
else
    EVENTS_PER_SEC="0.00"
fi

echo ""
echo "=== Throughput Summary ==="
echo "  Test Duration:    ${DURATION_MINS}m ${DURATION_SECS_REMAINDER}s (${ACTUAL_DURATION}s total)"
echo "  Events Sent:      ${SHIM_SENT}"
echo "  Events/sec:       ${EVENTS_PER_SEC}"
echo "  Input vs Output:  shim_sent=${SHIM_SENT} -> validated via receiver+duckdb"
echo "=========================="

exit $VALIDATE_EXIT
