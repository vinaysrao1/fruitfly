# Jetstream Integration Test Harness - Implementation Spec

## Problem Statement

We need a live integration test that connects Fruitfly to Bluesky's Jetstream firehose to prove the rules engine works against real-world social media event streams. The harness is four external processes (shim, receiver, Fruitfly, validator) wired together via HTTP. No Fruitfly core code is modified.

## Directory Structure

```
jetstream_test/
    DESIGN.md                   # this file
    run.sh                      # orchestration script
    fruitfly.yaml               # Fruitfly config for this test run
    shim/
        main.go                 # Jetstream WebSocket -> Fruitfly HTTP shim
    receiver/
        main.go                 # Webhook receiver with stats endpoint
    rules/
        spam_short_post.star    # Rule 1: short post flood detection
        spam_like_flood.star    # Rule 2: like flood detection
        numeric_content.star    # Rule 3: high numeric content flagging
        catchall.star           # Rule 4: wildcard approve-all
    validate/
        main.go                 # Post-run validation script
```

---

## 1. Event Mapping Spec

### Jetstream -> Fruitfly Transformation

The shim transforms each Jetstream commit event into a Fruitfly `POST /events` JSON body. The mapping is deterministic and stateless.

#### Posts (`app.bsky.feed.post`)

Jetstream input:
```json
{
  "did": "did:plc:abc123",
  "time_us": 1725911162329308,
  "kind": "commit",
  "commit": {
    "rev": "...",
    "operation": "create",
    "collection": "app.bsky.feed.post",
    "rkey": "3l...",
    "record": {
      "$type": "app.bsky.feed.post",
      "text": "Hello world",
      "createdAt": "2024-09-09T19:46:02.102Z",
      "langs": ["en"],
      "reply": { "root": {...}, "parent": {...} },
      "embed": {...}
    },
    "cid": "bafyrei..."
  }
}
```

Fruitfly output:
```json
{
  "event_id": "did:plc:abc123/app.bsky.feed.post/3l...",
  "event_type": "post",
  "timestamp": "2024-09-09T19:46:02.102Z",
  "entity_id": "did:plc:abc123",
  "text": "Hello world",
  "char_count": 11,
  "langs": ["en"],
  "is_reply": true,
  "created_at": "2024-09-09T19:46:02.102Z",
  "rkey": "3l...",
  "cid": "bafyrei..."
}
```

Key mapping decisions:

| Jetstream field | Fruitfly field | Notes |
|----------------|---------------|-------|
| `did` | `entity_id` (in payload) | The DID is the user identity, used by counter() for per-user rate limiting |
| `commit.collection` | `event_type` | `app.bsky.feed.post` -> `"post"`, `app.bsky.feed.like` -> `"like"` |
| `did + "/" + collection + "/" + rkey` | `event_id` | Globally unique composite key. Avoids UUID generation, enables dedup. |
| `commit.record.createdAt` | `timestamp` | Already RFC3339. Falls back to `time.Now().UTC()` if missing. |
| `commit.record.text` | `text` (in payload) | Raw post text |
| `len(text)` (rune count) | `char_count` (in payload) | Computed by shim, avoids recomputing in each rule |
| `commit.record.langs` | `langs` (in payload) | Language array, passed through |
| `commit.record.reply != nil` | `is_reply` (in payload) | Boolean: true if post is a reply |

**Important**: The entire JSON body is the Fruitfly payload (the `event_type`, `event_id`, and `timestamp` are top-level required fields; everything else lands in `event["payload"]` inside Starlark). The shim places `entity_id`, `text`, `char_count`, `langs`, `is_reply` etc. as top-level keys in the POST body. Fruitfly's ingest parses the full JSON as `payload`, so rules access them via `event["payload"]["entity_id"]`, `event["payload"]["text"]`, etc.

#### Likes (`app.bsky.feed.like`)

Jetstream input:
```json
{
  "did": "did:plc:abc123",
  "time_us": 1725911162329308,
  "kind": "commit",
  "commit": {
    "operation": "create",
    "collection": "app.bsky.feed.like",
    "rkey": "3l...",
    "record": {
      "$type": "app.bsky.feed.like",
      "subject": {
        "uri": "at://did:plc:xyz/app.bsky.feed.post/abc",
        "cid": "bafyrei..."
      },
      "createdAt": "2024-09-09T19:46:02.102Z"
    },
    "cid": "bafyrei..."
  }
}
```

Fruitfly output:
```json
{
  "event_id": "did:plc:abc123/app.bsky.feed.like/3l...",
  "event_type": "like",
  "timestamp": "2024-09-09T19:46:02.102Z",
  "entity_id": "did:plc:abc123",
  "subject_uri": "at://did:plc:xyz/app.bsky.feed.post/abc",
  "subject_cid": "bafyrei...",
  "rkey": "3l...",
  "cid": "bafyrei..."
}
```

#### Events to Skip

The shim silently drops:
- Events where `kind != "commit"` (identity, account events)
- Events where `commit.operation != "create"` (updates, deletes)
- Events where `commit.collection` is not `app.bsky.feed.post` or `app.bsky.feed.like`
- Events where `commit.record` is nil or missing

These are counted in a `skipped` atomic counter for diagnostics but not logged individually (too noisy at firehose speed).

---

## 2. Input Shim (`jetstream_test/shim/main.go`)

### Purpose

Single-file Go program. Connects to Jetstream WebSocket, transforms events, POSTs to Fruitfly.

### Configuration (flags)

| Flag | Default | Description |
|------|---------|-------------|
| `-jetstream-url` | `wss://jetstream2.us-east.bsky.network/subscribe` | Jetstream WebSocket URL |
| `-fruitfly-url` | `http://localhost:8080` | Fruitfly base URL |
| `-max-events` | `1000` | Stop after sending this many events (0 = unlimited) |
| `-concurrency` | `10` | Max concurrent HTTP POST goroutines |

### Key Implementation Details

**WebSocket connection**: Use `golang.org/x/net/websocket` or `github.com/gorilla/websocket`. Gorilla is more common for production WebSocket use in Go and handles ping/pong correctly. Connect with query params `?wantedCollections=app.bsky.feed.post&wantedCollections=app.bsky.feed.like`.

**Message loop**:
```
connect to WebSocket
for each message:
    json.Unmarshal into jetstreamEvent struct
    if kind != "commit" || operation != "create": skip
    transform to fruitfly payload (see mapping above)
    POST to fruitfly (with backpressure handling)
    increment sent counter
    if sent >= maxEvents: break
print final stats
```

**Backpressure handling**: The shim uses a semaphore (buffered channel of size `concurrency`) to limit concurrent POSTs. If Fruitfly returns 429, the shim:
1. Sleeps for 100ms (initial backoff)
2. Retries up to 3 times with exponential backoff (100ms, 200ms, 400ms)
3. After 3 failures, drops the event and increments a `dropped` counter

This is a test harness, not production infrastructure. Simple backoff is sufficient.

**Atomic counters** (printed every 5 seconds and at exit):
- `sent`: events successfully POSTed (HTTP 202)
- `skipped`: events dropped by filter (wrong kind/operation/collection)
- `dropped`: events that failed after retries
- `errors`: WebSocket read errors

**Struct definitions**:

```go
// jetstreamEvent is the top-level Jetstream message.
type jetstreamEvent struct {
    DID    string          `json:"did"`
    TimeUS int64           `json:"time_us"`
    Kind   string          `json:"kind"`
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
```

Using `map[string]any` for `Record` is intentional -- we only need a few fields and the schema varies by collection type. Type-asserting out of a generic map is simpler than maintaining full Bluesky schema structs for a test harness.

**Graceful shutdown**: Listen for SIGINT/SIGTERM. On signal, close WebSocket, wait for in-flight POSTs to complete (with 5s timeout), print final stats.

### Functions

| Function | Signature | Purpose |
|----------|-----------|---------|
| `main` | `func main()` | Parse flags, connect, run message loop, print stats |
| `transformPost` | `func transformPost(evt jetstreamEvent) map[string]any` | Map post commit to Fruitfly payload |
| `transformLike` | `func transformLike(evt jetstreamEvent) map[string]any` | Map like commit to Fruitfly payload |
| `postToFruitfly` | `func postToFruitfly(ctx context.Context, client *http.Client, url string, payload map[string]any) error` | POST with retry/backoff, returns nil on 202 |
| `printStats` | `func printStats(sent, skipped, dropped, errors *atomic.Int64)` | Log current counters |

---

## 3. Rules (`jetstream_test/rules/`)

All rules follow the Fruitfly Starlark rule contract: top-level `rule_id`, `event_type`, `priority` globals, and a `def evaluate(event)` function that returns a `verdict()`.

### Rule 1: `spam_short_post.star` -- Short Post Flood

```python
rule_id = "spam-short-post"
event_type = "post"
priority = 100

def evaluate(event):
    text = event["payload"].get("text", "")
    char_count = event["payload"].get("char_count", 0)
    entity_id = event["payload"].get("entity_id", "")

    if char_count < 10:
        count = counter(entity_id, "post", 300)
        if count > 5:
            return verdict("block", reason="spam: short post flood (" + str(count) + " posts in 5min)")

    return verdict("approve")
```

**Logic**: If a post has fewer than 10 characters AND the same DID has posted more than 5 times in the last 5 minutes (300 seconds), block as spam. The `counter()` UDF increments on every call (it both increments and reads), so every post evaluation for this entity/event_type pair increments the counter regardless of the rule outcome.

**Gotcha**: `counter()` increments on every invocation. This means the counter goes up even for posts with >= 10 characters if we called it unconditionally. By nesting the `counter()` call inside the `char_count < 10` check, we only count short posts. This is a deliberate design choice -- we want to count short posts specifically, not all posts.

Wait -- re-reading the requirement: "counter(did, 'post', 300) > 5". This counts ALL posts by the entity, not just short ones, because counter() is keyed on (entity_id, event_type). The counter is incremented by the UDF on every call. If we want to count all posts, we should call counter outside the char_count check. But the requirement says to check char_count < 10 AND counter > 5, which implies: "if this post is short AND the user is posting a lot, block it."

The question is: should the counter count ALL posts or only short posts? The requirement says `counter(did, "post", 300)` which counts all posts. But since counter() is only called when char_count < 10, only short-post evaluations increment the counter. For a test harness, this is fine -- it demonstrates the pattern. If we wanted to count all posts, we'd call counter() unconditionally and only check the threshold inside the char_count guard.

**Decision**: Call counter() only when char_count < 10. This means the counter tracks short posts only, which makes the spam detection more precise. A user posting many long posts will not be flagged. This is the more useful behavior.

### Rule 2: `spam_like_flood.star` -- Like Flood

```python
rule_id = "spam-like-flood"
event_type = "like"
priority = 100

def evaluate(event):
    entity_id = event["payload"].get("entity_id", "")

    count = counter(entity_id, "like", 600)
    if count > 5:
        return verdict("block", reason="spam: like flood (" + str(count) + " likes in 10min)")

    return verdict("approve")
```

**Logic**: If a user has liked more than 5 things in 10 minutes (600 seconds), block. Every like evaluation increments the counter. Simple rate limit.

**Note on counter thresholds**: The Jetstream firehose is high-volume. A threshold of 5 in 10 minutes will likely trigger for many real users. This is intentional for the test -- we want to see block verdicts in the output. In production, thresholds would be much higher.

### Rule 3: `numeric_content.star` -- High Numeric Content

```python
rule_id = "numeric-content"
event_type = "post"
priority = 90

def evaluate(event):
    text = event["payload"].get("text", "")

    numeric_count = 0
    for ch in text.elems():
        if ch >= "0" and ch <= "9":
            numeric_count += 1

    if numeric_count > 10:
        return verdict("review", reason="high numeric content: " + str(numeric_count) + " digits")

    return verdict("approve")
```

**Logic**: Count digit characters (0-9) in the post text. If more than 10 digits, flag for review. This catches phone numbers, credit card numbers, etc. Uses Starlark's `string.elems()` method to iterate over characters.

**Priority**: 90 (lower than spam rules at 100). If a short-post-flood AND high-numeric-content both match, the spam rule at priority 100 wins per Fruitfly's verdict resolution: highest priority verdict takes precedence.

**Note on Starlark string iteration**: Starlark strings support `.elems()` which yields each UTF-8 byte as a single-character string. For ASCII digits this is correct. For a test harness, this is sufficient.

### Rule 4: `catchall.star` -- Wildcard Approve

```python
rule_id = "catchall-approve"
event_type = "*"
priority = 1

def evaluate(event):
    return verdict("approve")
```

**Logic**: Matches all event types. Priority 1 (lowest). Ensures every event gets at least one verdict. Without this, events that match no other rule would still get the default `approve` from Fruitfly's verdict resolution, but having an explicit catch-all makes the rule set self-documenting.

### Rule Evaluation Matrix

| Event Type | Rules Evaluated (priority order) | Expected Outcome |
|-----------|--------------------------------|-----------------|
| post, short text, high post rate | spam-short-post (100), numeric-content (90), catchall (1) | block (from priority 100) |
| post, short text, low post rate | spam-short-post (100), numeric-content (90), catchall (1) | approve (spam rule approves, numeric may review) |
| post, long text, many digits | spam-short-post (100), numeric-content (90), catchall (1) | review (spam approves, numeric reviews at priority 90) |
| post, normal | spam-short-post (100), numeric-content (90), catchall (1) | approve |
| like, high like rate | spam-like-flood (100), catchall (1) | block |
| like, normal rate | spam-like-flood (100), catchall (1) | approve |

---

## 4. Webhook Receiver (`jetstream_test/receiver/main.go`)

### Purpose

Minimal HTTP server. Receives webhook POSTs from Fruitfly's output stage. Counts everything. Exposes stats via GET.

### Configuration (flags)

| Flag | Default | Description |
|------|---------|-------------|
| `-addr` | `:9090` | Listen address |

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/webhook` | Receives Fruitfly webhook POSTs |
| GET | `/stats` | Returns JSON stats |

### Stats Tracking

All counters are `sync/atomic.Int64`. No locks.

```go
type stats struct {
    Total      atomic.Int64                // total webhooks received
    ByVerdict  [3]*atomic.Int64            // indexed: 0=approve, 1=block, 2=review
    ByComposite sync.Map                   // "post:block" -> *atomic.Int64
}
```

Actually, since the number of composite keys is small and bounded (2 event types x 3 verdicts = 6 keys), use a `sync.Map` for simplicity. Each value is `*atomic.Int64`.

### Webhook POST Handler

```go
func (s *stats) handleWebhook(w http.ResponseWriter, r *http.Request) {
    body, err := io.ReadAll(r.Body)
    if err != nil {
        http.Error(w, "read error", 400)
        return
    }

    var result struct {
        EventType    string `json:"EventType"`
        FinalVerdict string `json:"FinalVerdict"`
    }
    if err := json.Unmarshal(body, &result); err != nil {
        http.Error(w, "bad json", 400)
        return
    }

    s.Total.Add(1)
    // increment by-verdict counter
    // increment composite counter: "post:block", "like:approve", etc.

    w.WriteHeader(200)
}
```

**Important**: Fruitfly's webhook serializes the `types.Result` struct directly via `json.Marshal`. The Result struct uses Go's default JSON marshaling, so field names are PascalCase: `EventType`, `FinalVerdict`, `EventID`, etc. The receiver must parse these PascalCase field names.

### Stats Endpoint Response

```json
{
  "total": 1000,
  "by_verdict": {
    "approve": 850,
    "block": 120,
    "review": 30
  },
  "by_composite": {
    "post:approve": 500,
    "post:block": 80,
    "post:review": 30,
    "like:approve": 350,
    "like:block": 40
  }
}
```

### Periodic Logging

Every 10 seconds, print a summary line to stdout:
```
[receiver] total=523 approve=410 block=88 review=25
```

Use a `time.Ticker` in a background goroutine.

### Graceful Shutdown

Listen for SIGINT/SIGTERM. Use `http.Server.Shutdown()` with a 5-second timeout.

---

## 5. Validation Script (`jetstream_test/validate/main.go`)

### Purpose

Runs after the pipeline drains. Queries the receiver stats and DuckDB, compares counts, reports discrepancies.

### Configuration (flags)

| Flag | Default | Description |
|------|---------|-------------|
| `-receiver-url` | `http://localhost:9090` | Receiver base URL |
| `-duckdb-path` | `./jetstream_test.duckdb` | Path to Fruitfly's DuckDB file |
| `-shim-sent` | (required) | Number of events the shim sent (passed from run.sh) |

### Validation Checks

The validator performs these queries and comparisons:

**Step 1: Fetch receiver stats**
```
GET http://localhost:9090/stats -> receiverStats
```

**Step 2: Query DuckDB**
```sql
-- Total results
SELECT COUNT(*) FROM results;

-- Verdict breakdown
SELECT verdict, COUNT(*) as cnt FROM results GROUP BY verdict ORDER BY verdict;

-- Event type + verdict breakdown
SELECT event_type, verdict, COUNT(*) as cnt FROM results GROUP BY event_type, verdict ORDER BY event_type, verdict;
```

DuckDB access: Open a read-only connection using `go-duckdb`. The Fruitfly process must be shut down before validation runs (DuckDB is single-writer; reading while Fruitfly holds the connection may conflict).

**Step 3: Compare counts**

| Check | Formula | Acceptable Discrepancy |
|-------|---------|----------------------|
| Shim sent vs DuckDB total | `shimSent == duckdbTotal` | 0 (exact match expected). If shim sent 1000 and DuckDB has 998, report "2 events lost in pipeline". |
| DuckDB total vs webhook total | `duckdbTotal == receiverTotal` | Small discrepancy acceptable (webhooks are best-effort). Report if > 1% difference. |
| Verdict breakdown consistency | `sum(by_verdict) == total` | Must be exact. |

**Step 4: Print summary table**

```
=== Jetstream Test Results ===

Pipeline Counts:
  Shim sent:        1000
  DuckDB results:   1000  (0 dropped)
  Webhooks received: 998  (2 missed, 0.2%)

Verdict Breakdown (DuckDB):
  approve:  850  (85.0%)
  block:    120  (12.0%)
  review:    30  ( 3.0%)

Event Type x Verdict (DuckDB):
  post:approve:   500
  post:block:      80
  post:review:     30
  like:approve:   350
  like:block:      40

Webhook Verdict Breakdown:
  approve:  848
  block:    120
  review:    30

Status: PASS (no pipeline drops, webhook loss < 1%)
```

**Exit code**: 0 if all checks pass, 1 if any discrepancy exceeds tolerance.

### Functions

| Function | Purpose |
|----------|---------|
| `main` | Parse flags, run checks, print report |
| `fetchReceiverStats` | GET /stats, unmarshal JSON |
| `queryDuckDB` | Open DuckDB, run queries, return counts |
| `printReport` | Format and print the summary table |

---

## 6. Fruitfly Configuration (`jetstream_test/fruitfly.yaml`)

```yaml
address: ":8080"
rules_dir: "./jetstream_test/rules"
duckdb_path: "./jetstream_test.duckdb"
webhook_url: "http://localhost:9090/webhook"
workers: 4
log_level: info
```

This config file is used by `run.sh` when starting Fruitfly. The DuckDB path is local to the project root. Workers set to 4 (enough for a test run, avoids saturating the machine).

---

## 7. Orchestration (`jetstream_test/run.sh`)

### Script Behavior

```bash
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
MAX_EVENTS="${1:-1000}"
DUCKDB_PATH="$PROJECT_DIR/jetstream_test.duckdb"

# Clean up from previous runs.
rm -f "$DUCKDB_PATH"

echo "=== Jetstream Integration Test ==="
echo "Max events: $MAX_EVENTS"
echo ""

# Step 1: Build all binaries.
echo "[1/6] Building binaries..."
(cd "$PROJECT_DIR" && go build -o ./jetstream_test/bin/fruitfly .)
(cd "$SCRIPT_DIR/shim" && go build -o "$SCRIPT_DIR/bin/shim" .)
(cd "$SCRIPT_DIR/receiver" && go build -o "$SCRIPT_DIR/bin/receiver" .)
(cd "$SCRIPT_DIR/validate" && go build -o "$SCRIPT_DIR/bin/validate" .)

# Step 2: Start the webhook receiver.
echo "[2/6] Starting webhook receiver on :9090..."
"$SCRIPT_DIR/bin/receiver" -addr=":9090" &
RECEIVER_PID=$!

# Give receiver a moment to bind.
sleep 1

# Step 3: Start Fruitfly.
echo "[3/6] Starting Fruitfly on :8080..."
"$SCRIPT_DIR/bin/fruitfly" -config="$SCRIPT_DIR/fruitfly.yaml" &
FRUITFLY_PID=$!

# Wait for Fruitfly to be ready.
for i in $(seq 1 30); do
    if curl -sf http://localhost:8080/admin/ready > /dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

# Verify readiness.
if ! curl -sf http://localhost:8080/admin/ready > /dev/null 2>&1; then
    echo "ERROR: Fruitfly did not become ready within 15 seconds"
    kill $FRUITFLY_PID $RECEIVER_PID 2>/dev/null
    exit 1
fi
echo "  Fruitfly ready."

# Step 4: Run the input shim.
echo "[4/6] Running shim (max $MAX_EVENTS events)..."
"$SCRIPT_DIR/bin/shim" \
    -fruitfly-url="http://localhost:8080" \
    -max-events="$MAX_EVENTS" \
    -concurrency=10

# Capture shim's exit. The shim prints final sent count to stdout as last line:
# "SENT:<count>"
# We parse this. Alternatively, the shim writes to a temp file.
SHIM_SENT=$(tail -1 /tmp/jetstream_shim_stats.txt 2>/dev/null || echo "0")

# Step 5: Wait for pipeline to drain.
echo "[5/6] Waiting for pipeline drain (10 seconds)..."
sleep 10

# Step 6: Stop Fruitfly gracefully (SIGTERM triggers graceful shutdown).
echo "[6/6] Shutting down Fruitfly..."
kill -TERM $FRUITFLY_PID 2>/dev/null
wait $FRUITFLY_PID 2>/dev/null || true

# Small delay for DuckDB to fully close.
sleep 2

# Run validation.
echo ""
echo "=== Validation ==="
"$SCRIPT_DIR/bin/validate" \
    -receiver-url="http://localhost:9090" \
    -duckdb-path="$DUCKDB_PATH" \
    -shim-sent="$SHIM_SENT"

VALIDATE_EXIT=$?

# Shut down receiver.
kill -TERM $RECEIVER_PID 2>/dev/null
wait $RECEIVER_PID 2>/dev/null || true

# Clean up.
rm -rf "$SCRIPT_DIR/bin"

exit $VALIDATE_EXIT
```

### Cleanup Trap

Add a `trap` at the top of the script to ensure background processes are killed even if the script fails:

```bash
cleanup() {
    kill $FRUITFLY_PID $RECEIVER_PID 2>/dev/null || true
    rm -rf "$SCRIPT_DIR/bin"
}
trap cleanup EXIT
```

### Shim Stats Communication

The shim writes its final counts to `/tmp/jetstream_shim_stats.txt` as a simple key=value file:
```
sent=1000
skipped=4523
dropped=3
errors=0
```

The `run.sh` script reads the `sent` value and passes it to the validator via the `-shim-sent` flag.

This avoids parsing stdout (fragile) and avoids needing the shim to expose an HTTP stats endpoint (over-engineering for a test harness).

---

## 8. Go Module Strategy

The shim, receiver, and validator are standalone Go programs. They do NOT import any Fruitfly packages. They communicate with Fruitfly purely over HTTP.

**Option A (recommended): Separate go.mod per binary**

```
jetstream_test/shim/go.mod       # depends on gorilla/websocket, net/http
jetstream_test/receiver/go.mod   # stdlib only
jetstream_test/validate/go.mod   # depends on go-duckdb
```

This keeps the test harness completely decoupled from Fruitfly's dependency tree. The shim needs a WebSocket library; the validator needs go-duckdb; the receiver needs only stdlib. None of them need go-starlark.

**Option B: Single go.mod in jetstream_test/**

Simpler to manage but mixes dependencies. Fine for a test harness.

**Recommendation**: Option A. Three tiny go.mod files. Each binary has exactly the dependencies it needs. The `run.sh` build step does `cd` into each directory and builds.

### Dependencies Per Binary

| Binary | External Dependencies |
|--------|----------------------|
| shim | `github.com/gorilla/websocket` |
| receiver | (stdlib only) |
| validate | `github.com/marcboeker/go-duckdb` |

---

## 9. Gotchas and Edge Cases

### Counter Behavior

The `counter()` UDF both increments and reads in a single call. This means:
- The first call for a new (entity, event_type) pair returns 1 (not 0)
- Threshold of "counter > 5" means the 6th invocation triggers
- Counter is per-worker-increment but cross-worker-read, so the count is approximate at high concurrency (events for the same DID hitting different workers)
- For this test, the thresholds are low enough that approximation does not matter

### Jetstream Connection Drops

The Jetstream WebSocket may disconnect. The shim should:
1. Log the disconnect
2. Reconnect after a 1-second delay
3. Continue counting from where it left off
4. Not reset the `sent` counter

No cursor tracking is needed. We do not care about missed events during reconnection -- this is a test harness, not a production consumer.

### Fruitfly Backpressure at Firehose Speed

The Jetstream firehose delivers hundreds of events per second. Fruitfly's input channel has capacity 100. At firehose speed, the shim will hit 429s frequently. This is expected and demonstrates the backpressure mechanism works. The shim's concurrency limit (10 goroutines) acts as a natural throttle.

If 429s are excessive, the shim's concurrency can be reduced to slow the send rate.

### DuckDB Concurrent Access

DuckDB does not support concurrent writers. The validation script must run after Fruitfly has fully shut down (after `Shutdown()` returns and `Writer.Close()` completes). The `run.sh` script ensures this by sending SIGTERM to Fruitfly and waiting for the process to exit before running validation.

### Webhook Field Names Are PascalCase

Fruitfly's `types.Result` struct uses Go naming. `json.Marshal` produces PascalCase by default (no json tags on the struct). The receiver must parse `FinalVerdict`, `EventType`, `EventID` -- not `final_verdict`, `event_type`, `event_id`.

Verify by checking `types/types.go` -- the Result struct has no json tags, confirming PascalCase serialization.

### Event ID Uniqueness

The composite event ID (`did/collection/rkey`) should be unique across the Jetstream stream since rkeys are unique per DID+collection. If duplicates occur (due to replays), DuckDB's `INSERT OR IGNORE` handles them gracefully -- the duplicate is silently dropped. This means the DuckDB count could be slightly less than the shim's sent count if duplicates occur. The validator should note this possibility.

### Starlark `event["payload"]` Access Pattern

In Fruitfly, the entire POST body is parsed into `event.Payload` (which is `map[string]any`). The Starlark event dict has:
- `event["event_id"]` -> string
- `event["event_type"]` -> string
- `event["timestamp"]` -> int (Unix seconds)
- `event["payload"]` -> dict (the full JSON body)

So `event["payload"]["text"]` accesses the `text` field from the POST body. The `entity_id` field is accessed as `event["payload"]["entity_id"]`. This is because Fruitfly stores the entire JSON body as the payload, and the top-level `event_type`, `event_id`, `timestamp` are extracted and also available at the event dict top level.

### Starlark `.get()` for Safe Access

Rules should use `.get("key", default)` instead of `["key"]` for payload fields that might be missing. A malformed event or a record missing the `text` field should not crash the rule. The rules above all use `.get()` with sensible defaults.

### Stats File Race Condition

The shim writes stats to `/tmp/jetstream_shim_stats.txt` synchronously before exiting. Since `run.sh` waits for the shim process to complete before reading the file, there is no race condition. The file is written, the process exits, then `run.sh` reads it.

---

## 10. What This Does NOT Do

- **No cursor management**: We do not track Jetstream cursors. Each run starts fresh from the live stream.
- **No persistence between runs**: DuckDB is wiped at the start of each run.
- **No authentication**: Jetstream is public. Fruitfly and receiver run on localhost.
- **No TLS**: All HTTP is plaintext on localhost.
- **No Docker**: Everything runs as local processes. `run.sh` manages process lifecycle.
- **No CI integration**: This is a manual test harness. Run it, look at the output.

---

## 11. Expected Test Run Output

For a 1000-event run:

```
=== Jetstream Integration Test ===
Max events: 1000

[1/6] Building binaries...
[2/6] Starting webhook receiver on :9090...
[3/6] Starting Fruitfly on :8080...
  Fruitfly ready.
[4/6] Running shim (max 1000 events)...
[shim] sent=200 skipped=1832 dropped=0 errors=0
[shim] sent=400 skipped=3641 dropped=2 errors=0
[shim] sent=600 skipped=5490 dropped=3 errors=0
[shim] sent=800 skipped=7201 dropped=3 errors=0
[shim] sent=1000 skipped=8934 dropped=5 errors=0
[shim] DONE. Final: sent=1000 skipped=8934 dropped=5 errors=0
[5/6] Waiting for pipeline drain (10 seconds)...
[6/6] Shutting down Fruitfly...

=== Validation ===

Pipeline Counts:
  Shim sent:         1000
  DuckDB results:    1000  (0 dropped)
  Webhooks received:  997  (3 missed, 0.3%)

Verdict Breakdown (DuckDB):
  approve:  782  (78.2%)
  block:    190  (19.0%)
  review:    28  ( 2.8%)

Event Type x Verdict (DuckDB):
  post:approve:   420
  post:block:     112
  post:review:     28
  like:approve:   362
  like:block:      78

Status: PASS
```

The high block rate (~19%) is expected because the counter thresholds (5 posts in 5min, 5 likes in 10min) are intentionally low. Real users on the Bluesky firehose post and like frequently enough to trigger these thresholds. This validates that the counter UDF and rate-limiting rules work correctly at scale.
