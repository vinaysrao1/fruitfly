# Fruitfly

A self-contained rules engine that evaluates [Starlark](https://github.com/google/starlark-go) rules against JSON events in real time. Zero external dependencies — no Kafka, no PostgreSQL, no Redis. All storage is embedded (DuckDB), all caching is in-process, results are sent to a webhook.

```
JSON Events (HTTP)
       |
       v
  +---------+    chan    +----------+    chan    +--------+
  | Ingest  | --------> | Executor | --------> | Output |
  |  (HTTP) |           | (Workers)|           | (1 go) |
  +---------+           +----------+           +---+----+
                             |                  |      |
                       atomic.Pointer        DuckDB  Webhook
                       (rules snapshot)     (batch)  (HTTP POST)
                             ^
                             |
                        +---------+
                        | Reloader|
                        +---------+
                       (fsnotify / poll)
```

## Install

**Prerequisites**: Go 1.25+ and a C compiler (required for DuckDB CGO bindings).

```bash
git clone https://github.com/vinaysrao1/fruitfly.git
cd fruitfly
go build -o fruitfly .
```

This produces a single binary: `./fruitfly`.

## Quick Start

### 1. Create a rule

```bash
mkdir -p rules
cat > rules/hello.star << 'EOF'
rule_id = "hello"
event_type = "*"
priority = 1

def evaluate(event):
    return verdict("approve")
EOF
```

### 2. Start Fruitfly

```bash
./fruitfly
```

Fruitfly starts on `:8080` with defaults — no config file needed.

### 3. Send an event

```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "test",
    "timestamp": "2026-02-20T12:00:00Z",
    "payload": {"message": "hello world"}
  }'
```

### 4. Check DuckDB

```bash
duckdb fruitfly.duckdb "SELECT event_id, verdict, latency_us FROM results"
```

## Configuration

Create `fruitfly.yaml` (all fields optional — defaults shown):

```yaml
address: ":8080"              # HTTP server address
rules_dir: "./rules"          # directory containing .star rule files
duckdb_path: "fruitfly.duckdb" # DuckDB database file
webhook_url: ""               # webhook endpoint for results (empty = disabled)
workers: 0                    # executor worker count (0 = NumCPU)
log_level: "info"             # debug | info | warn | error
```

```bash
./fruitfly -config fruitfly.yaml
```

## API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/events` | POST | Submit an event for evaluation |
| `/admin/health` | GET | Liveness check (always 200) |
| `/admin/ready` | GET | Readiness check (200 when rules loaded and DB ready) |
| `/admin/rules` | GET | Currently loaded rules |
| `/admin/rules/reload` | POST | Trigger immediate rule reload (202) |
| `/admin/metrics` | GET | Metrics (placeholder) |

### Event format

```json
{
  "event_id": "optional-unique-id",
  "event_type": "post",
  "timestamp": "2026-02-20T12:00:00Z",
  "payload": {"user_id": "u1", "score": 0.9}
}
```

`event_id` is auto-generated (UUIDv7) if omitted.

### Response codes

| Code | Meaning |
|------|---------|
| 202 | Accepted |
| 400 | Validation error |
| 413 | Payload too large (256KB limit) |
| 415 | Wrong Content-Type |
| 429 | Backpressure — retry later |

## Running Tests

```bash
go test ./... -race
```

Integration tests include throughput validation at 100 events/second.

## Performance

Targets 100 events/second sustained with O(100) rules on a single machine.

| Stage | Capacity |
|-------|----------|
| Ingest (HTTP + JSON) | ~100K eps |
| Executor (8 workers, 100 rules) | ~800 eps |
| DuckDB (batch writes) | ~20K eps |

Memory: ~340MB typical (256MB DuckDB + runtime).

## Documentation

| Doc | Description |
|-----|-------------|
| [Writing Rules](docs/WRITING_RULES.md) | How to write, enable, and disable Starlark rules |
| [Querying DuckDB](docs/QUERYING_DUCKDB.md) | SQL queries for metrics, debugging, and exports |
| [Event Connectors](docs/EVENT_CONNECTORS.md) | How to stream events from Kafka, databases, Redis, etc. |
| [Webhook Receivers](docs/WEBHOOK_RECEIVERS.md) | How to build a webhook endpoint to consume results |
| [Design Doc](docs/FRUITFLY_DESIGN.md) | Architecture, concurrency model, and tradeoffs |
