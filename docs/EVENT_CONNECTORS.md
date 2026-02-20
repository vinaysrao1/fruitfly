# Event Connectors

Fruitfly accepts events via `POST /events` on its HTTP endpoint. To stream events into Fruitfly, you write a connector — a small program that reads from your event source and POSTs JSON to Fruitfly.

## Event Format

```json
{
  "event_id": "evt-abc-123",
  "event_type": "post",
  "timestamp": "2026-02-20T12:00:00Z",
  "payload": {
    "user_id": "user-456",
    "body": "Hello world",
    "spam_score": 0.2
  }
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `event_id` | No (auto-generated UUIDv7 if missing) | Unique event identifier |
| `event_type` | Yes | Category string — rules match on this |
| `timestamp` | Yes | RFC3339 format |
| `payload` | No | Arbitrary JSON object accessible to rules |

### Response Codes

| Code | Meaning |
|------|---------|
| 202 | Accepted — event queued for evaluation |
| 400 | Bad request — validation failed (check response body) |
| 413 | Payload too large (max 256KB) |
| 415 | Wrong Content-Type (must be `application/json`) |
| 429 | Backpressure — pipeline is full, retry later |

## Example: curl

```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "post",
    "timestamp": "2026-02-20T12:00:00Z",
    "payload": {"user_id": "u1", "body": "hello"}
  }'
```

## Example: Python Connector

### From a database poll

```python
import requests
import time
import psycopg2

FRUITFLY_URL = "http://localhost:8080/events"

conn = psycopg2.connect("dbname=myapp")
cursor = conn.cursor()
last_id = 0

while True:
    cursor.execute(
        "SELECT id, type, created_at, data FROM events WHERE id > %s ORDER BY id LIMIT 100",
        (last_id,)
    )
    rows = cursor.fetchall()
    for row in rows:
        event = {
            "event_id": f"db-{row[0]}",
            "event_type": row[1],
            "timestamp": row[2].isoformat() + "Z",
            "payload": row[3],
        }
        resp = requests.post(FRUITFLY_URL, json=event, timeout=5)
        if resp.status_code == 429:
            time.sleep(0.1)  # backpressure — slow down
            continue
        resp.raise_for_status()
        last_id = row[0]
    time.sleep(1)  # poll interval
```

### From a Kafka topic

```python
from confluent_kafka import Consumer
import requests, json

consumer = Consumer({
    "bootstrap.servers": "kafka:9092",
    "group.id": "fruitfly-connector",
    "auto.offset.reset": "latest",
})
consumer.subscribe(["user-events"])

FRUITFLY_URL = "http://localhost:8080/events"

while True:
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue
    data = json.loads(msg.value())
    event = {
        "event_id": data.get("id", ""),
        "event_type": data["type"],
        "timestamp": data["timestamp"],
        "payload": data.get("payload", {}),
    }
    resp = requests.post(FRUITFLY_URL, json=event, timeout=5)
    if resp.status_code == 429:
        continue  # Kafka consumer will re-poll
```

### From a Redis stream

```python
import redis, requests, json
from datetime import datetime, timezone

r = redis.Redis()
last_id = "0-0"
FRUITFLY_URL = "http://localhost:8080/events"

while True:
    entries = r.xread({"events": last_id}, count=100, block=1000)
    for stream, messages in entries:
        for msg_id, fields in messages:
            event = {
                "event_type": fields[b"type"].decode(),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "payload": json.loads(fields[b"data"]),
            }
            requests.post(FRUITFLY_URL, json=event, timeout=5)
            last_id = msg_id
```

## Example: Go Connector

```go
package main

import (
    "bytes"
    "encoding/json"
    "net/http"
    "time"
)

type Event struct {
    EventType string         `json:"event_type"`
    Timestamp string         `json:"timestamp"`
    Payload   map[string]any `json:"payload"`
}

func send(event Event) error {
    body, _ := json.Marshal(event)
    resp, err := http.Post("http://localhost:8080/events", "application/json", bytes.NewReader(body))
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    if resp.StatusCode == 429 {
        time.Sleep(100 * time.Millisecond)
    }
    return nil
}
```

## Handling Backpressure

When Fruitfly returns **429**, its internal pipeline is full (channel buffer at capacity). Your connector should:

1. **Pause briefly** (100ms–1s) before retrying.
2. **Do not drop the event** — retry or buffer locally.
3. **Do not flood retries** — use exponential backoff if 429s persist.

At 100 events/second, 429s should be rare. Sustained 429s mean Fruitfly is overloaded — check rule complexity or add workers.

## Connector Best Practices

- Set `Content-Type: application/json` on every request.
- Include `event_id` if your source has a natural ID (for deduplication). Omit it to let Fruitfly auto-generate a UUIDv7.
- Keep `payload` under 256KB. If you have large blobs, store them externally and pass a reference.
- Use a persistent HTTP connection (connection pooling / keep-alive) to avoid TCP setup overhead at 100 eps.
- Log and alert on sustained 429 responses — they indicate pipeline saturation.
