# Webhook Receivers

Fruitfly sends every evaluation result to a configured webhook URL via HTTP POST. This is your real-time output — use it to trigger actions, send notifications, or feed downstream systems.

## Configuration

Set the webhook URL in `fruitfly.yaml`:

```yaml
webhook_url: "https://your-service.example.com/fruitfly/results"
```

If `webhook_url` is empty, webhook delivery is disabled (results are still written to DuckDB).

## Payload Format

Each result is POST'd as JSON:

```json
{
  "event_id": "019c7a00-1234-7abc-8def-0123456789ab",
  "event_type": "post",
  "verdict": "block",
  "triggered_rules": [
    {
      "rule_id": "spam-filter-v1",
      "verdict": "block",
      "reason": "spam score 0.95"
    },
    {
      "rule_id": "rate-limit-posts",
      "verdict": "approve",
      "reason": ""
    }
  ],
  "failed_rules": [],
  "payload": {
    "user_id": "user-456",
    "body": "Buy now!!!",
    "spam_score": 0.95
  },
  "latency_us": 2341,
  "processed_at": "2026-02-20T12:00:00.123Z"
}
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `event_id` | string | The original event ID |
| `event_type` | string | The original event type |
| `verdict` | string | Final verdict: `approve`, `block`, or `review` |
| `triggered_rules` | array | Rules that returned a verdict (with rule_id, verdict, reason) |
| `failed_rules` | array | Rules that errored (with rule_id, error message) |
| `payload` | object | The original event payload (pass-through) |
| `latency_us` | int | Total evaluation time in microseconds |
| `processed_at` | string | RFC3339 timestamp of when the result was produced |

## Delivery Semantics

- **Best-effort**: Fruitfly retries up to 3 times with exponential backoff (100ms, 200ms, 400ms). If all retries fail, the result is logged and dropped.
- **DuckDB is the source of truth**: Every result is always written to DuckDB, regardless of webhook success. Use DuckDB for replay if webhooks fail.
- **At-most-once per result**: No deduplication. If Fruitfly is restarted, there is no replay of previously-delivered results.
- **Concurrent delivery**: Up to 100 webhook calls run concurrently. Under sustained load, excess deliveries are dropped with a log warning.

## Response Expectations

Your webhook receiver should:

| Aspect | Requirement |
|--------|-------------|
| Status code | Return **2xx** (200, 201, 204) on success |
| Latency | Respond within **5 seconds** (Fruitfly's HTTP client timeout) |
| Idempotency | Handle duplicate `event_id` gracefully (just in case) |
| Availability | Be highly available — Fruitfly does not queue failed deliveries |

### How Fruitfly handles your response:

| Your Response | Fruitfly Behavior |
|---------------|-------------------|
| 2xx | Success — delivery complete |
| 429 | Retries (treated as transient) |
| 4xx (not 429) | No retry — logs the failure |
| 5xx | Retries up to 3 times |
| Timeout / error | Retries up to 3 times |

## Example: Python (Flask)

```python
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/fruitfly/results", methods=["POST"])
def handle_result():
    result = request.json
    event_id = result["event_id"]
    verdict = result["verdict"]

    if verdict == "block":
        # Take action: ban user, hide content, send alert
        user_id = result["payload"].get("user_id")
        print(f"BLOCKED event {event_id} from user {user_id}")
        block_content(event_id)
        notify_moderators(event_id, result["triggered_rules"])

    elif verdict == "review":
        # Queue for human review
        enqueue_for_review(event_id, result)

    # approve — no action needed

    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    app.run(port=9090)
```

## Example: Go

```go
package main

import (
    "encoding/json"
    "fmt"
    "net/http"
)

type Result struct {
    EventID   string `json:"event_id"`
    EventType string `json:"event_type"`
    Verdict   string `json:"verdict"`
    LatencyUS int64  `json:"latency_us"`
    Triggered []struct {
        RuleID  string `json:"rule_id"`
        Verdict string `json:"verdict"`
        Reason  string `json:"reason"`
    } `json:"triggered_rules"`
    Payload map[string]any `json:"payload"`
}

func main() {
    http.HandleFunc("/fruitfly/results", func(w http.ResponseWriter, r *http.Request) {
        var result Result
        if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
            http.Error(w, "bad request", 400)
            return
        }
        fmt.Printf("[%s] %s → %s (%dµs)\n",
            result.EventType, result.EventID, result.Verdict, result.LatencyUS)

        switch result.Verdict {
        case "block":
            // handle block
        case "review":
            // handle review
        }

        w.WriteHeader(200)
    })
    http.ListenAndServe(":9090", nil)
}
```

## Example: Node.js (Express)

```javascript
const express = require("express");
const app = express();
app.use(express.json());

app.post("/fruitfly/results", (req, res) => {
  const { event_id, verdict, triggered_rules, payload } = req.body;

  if (verdict === "block") {
    console.log(`BLOCKED ${event_id}:`, triggered_rules);
    // take action
  }

  res.sendStatus(200);
});

app.listen(9090, () => console.log("Webhook receiver on :9090"));
```

## Testing Webhooks Locally

Start a simple receiver to inspect payloads:

```bash
# Python one-liner that prints every POST body
python3 -c "
from http.server import HTTPServer, BaseHTTPRequestHandler
import json

class H(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        print(json.dumps(body, indent=2))
        self.send_response(200)
        self.end_headers()

HTTPServer(('', 9090), H).serve_forever()
"
```

Then configure Fruitfly:

```yaml
webhook_url: "http://localhost:9090"
```
