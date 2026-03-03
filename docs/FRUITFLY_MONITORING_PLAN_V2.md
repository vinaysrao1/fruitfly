# Fruitfly — Continuous Monitoring & Agent Remediation Plan (v2)

**Goal:** Detect behavioral anomalies in a running fruitfly instance and remediate automatically, in ~800 lines of Go.

**Principle:** One loop, one agent, one judge. No microservice architecture for monitoring a single-process system.

---

## Architecture

```
┌─────────────────────────────────┐
│         FRUITFLY (running)      │
│                                 │
│  DuckDB ← results table        │
│  slog   ← structured JSON logs │
│  HTTP   ← /admin/* endpoints   │
└───────┬─────────┬─────────┬─────┘
        │         │         │
        ▼         ▼         ▼
┌─────────────────────────────────┐
│         monitor.go (~300 lines) │
│                                 │
│  Every 60s: collect()           │
│    → 5 SQL queries on DuckDB   │
│    → GET /admin/rules           │
│    → GET /admin/ready           │
│    → build snapshot map         │
│                                 │
│  Every 60s: checkThresholds()   │
│    → static safety net          │
│    → fires alerts immediately   │
│                                 │
│  Every 5min: judge()            │
│    → LLM call with snapshot     │
│    → returns assessment         │
│    → dispatches agent if needed │
└───────────────┬─────────────────┘
                │
                ▼
┌─────────────────────────────────┐
│  agent.go (~200 lines)          │
│                                 │
│  Single function:               │
│    runAgent(mandate, authority)  │
│                                 │
│  Authority levels:              │
│    ReadOnly  → query DuckDB,    │
│               read rules        │
│    CanModify → + write rules,   │
│               trigger reload    │
│    CanAlert  → + send Slack     │
│                                 │
│  LLM picks tools based on       │
│  mandate + available tools      │
└─────────────────────────────────┘
```

Total: 4 files, ~800 lines. No collector module, no dispatcher module, no 5 agent implementations.

---

## File Layout

```
fruitfly/
├── monitor/
│   ├── monitor.go         # ~300 lines — collect + static alerts + judge loop
│   ├── judge.go           # ~150 lines — prompt builder + API call + response parser
│   ├── agent.go           # ~200 lines — single agent with tool dispatch
│   └── monitor_test.go    # ~150 lines
```

---

## Part 1: Data Collection — inside `monitor.go`

No separate collector module. A single `collect()` function that returns a `map[string]any`.

### What It Queries

```go
func collect(db *sql.DB, adminURL string) map[string]any
```

Five DuckDB queries, two HTTP calls. Returns one snapshot map.

#### Query 1: Verdict Distribution (last 5 min)

```sql
SELECT verdict, COUNT(*) as count,
       AVG(latency_us) as avg_latency,
       PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_us) as p99_latency
FROM results WHERE processed_at > now() - interval '5 minutes'
GROUP BY verdict
```

#### Query 2: Rule Error Rate (last 5 min)

```sql
SELECT COUNT(*) as total,
       SUM(CASE WHEN json_array_length(failed_rules) > 0 THEN 1 ELSE 0 END) as with_failures
FROM results WHERE processed_at > now() - interval '5 minutes'
```

#### Query 3: Per-Rule Trigger Counts (last 5 min)

```sql
SELECT r.rule_id, r.verdict, COUNT(*) as triggers
FROM results,
     LATERAL unnest(from_json(triggered_rules, '["json"]')) AS t(j),
     LATERAL (SELECT json_extract_string(t.j, '$.RuleID') as rule_id,
                     json_extract_string(t.j, '$.Verdict') as verdict) AS r
WHERE processed_at > now() - interval '5 minutes'
GROUP BY r.rule_id, r.verdict
```

#### Query 4: Throughput Trend (last 30 min, per minute)

```sql
SELECT date_trunc('minute', processed_at) as minute, COUNT(*) as events
FROM results WHERE processed_at > now() - interval '30 minutes'
GROUP BY 1 ORDER BY 1
```

#### Query 5: Latency Trend (last 30 min, per minute)

```sql
SELECT date_trunc('minute', processed_at) as minute,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY latency_us) as p50,
       PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_us) as p99
FROM results WHERE processed_at > now() - interval '30 minutes'
GROUP BY 1 ORDER BY 1
```

#### HTTP Calls

- `GET /admin/ready` → bool
- `GET /admin/rules` → snapshot ID, rule count, rule list

### Snapshot Format

```go
// No custom struct. A map[string]any serialized to JSON for the judge prompt.
// Example:
{
  "timestamp": "2026-02-25T12:00:00Z",
  "verdicts": {"approve": 470, "block": 22, "review": 5},
  "latency": {"p50_us": 3200, "p99_us": 18000},
  "throughput_per_min": [98, 102, 99, 101, 97],
  "rule_failures": {"total": 3, "rate": 0.006},
  "per_rule": {"spam-like-flood": {"triggers": 15, "failures": 0}, ...},
  "latency_trend_p99": [18000, 19000, 21000, 25000, 42000],
  "snapshot_id": "snap-abc123",
  "rule_count": 4,
  "ready": true
}
```

### Baseline

A JSON file loaded at startup. No weekly auto-update logic. Human updates it when intentional changes happen, or a flag recomputes it from the last 24h of DuckDB data.

```json
{
  "block_rate": {"low": 0.02, "high": 0.06},
  "review_rate": {"low": 0.005, "high": 0.02},
  "p99_latency_us": {"low": 10000, "high": 25000},
  "events_per_minute": {"low": 80, "high": 120},
  "rule_failure_rate": {"low": 0, "high": 0.001}
}
```

Recompute: `fruitfly monitor --recompute-baseline --hours 24`

---

## Part 2: Static Threshold Safety Net — inside `monitor.go`

Runs every collection cycle (60s). No LLM dependency. Catches catastrophic failures.

```go
func checkThresholds(snapshot map[string]any, alertFn func(string, string)) {
    // Each check: if violated, call alertFn(severity, message)
}
```

| Condition | Severity | Action |
|-----------|----------|--------|
| `/admin/ready` returns non-200 for 3 consecutive polls | critical | Alert immediately |
| Rule failure rate > 10% | critical | Alert + run agent (read-only) |
| p99 latency > 4,000,000 µs (approaching 5s timeout) | critical | Alert immediately |
| Zero events in 5 minutes (throughput = 0) | critical | Alert immediately |
| Block rate > 3× baseline upper bound | warning | Alert |
| Backpressure logged 3 consecutive windows | warning | Alert |

Static thresholds are the floor. The LLM judge is the ceiling. If the LLM is down, static thresholds keep working.

---

## Part 3: LLM Judge — `judge.go` (~150 lines)

### What It Does

One function call every 5 minutes:

```go
type Assessment struct {
    Status  string   `json:"status"`  // healthy | degraded | critical
    Confidence float64 `json:"confidence"`
    Findings []Finding `json:"findings"`
    Actions  []Action  `json:"recommended_actions"`
    Watch    []string  `json:"watch_list"`
}

type Finding struct {
    Signal     string `json:"signal"`
    Severity   string `json:"severity"`   // info | warning | critical
    Evidence   string `json:"evidence"`
    LikelyCause string `json:"likely_cause"`
}

type Action struct {
    Authority string         `json:"authority"` // read_only | can_modify | can_alert
    Reason    string         `json:"reason"`
    Urgency   string         `json:"urgency"`   // immediate | next_cycle | informational
    Mandate   string         `json:"mandate"`   // free-text instruction for the agent
}

func judge(current, baseline map[string]any, previous *Assessment, rulesDesc string) (*Assessment, error)
```

### Prompt

```
You are monitoring a Fruitfly rules engine instance.

Behavioral contracts:
- Every accepted event → exactly 1 DuckDB row
- Verdicts: approve, block, review. Resolved by priority-then-weight.
- Latency bounded: 5s per event, 1s per rule
- Hot reload atomic. Backpressure = 429, no silent drops.

Current rules: {rulesDesc}

Current snapshot (last 5 min): {current_json}
Baseline (normal ranges): {baseline_json}
Previous assessment (5 min ago): {previous_json}

Output valid JSON matching this schema: {schema}

Rules:
- "healthy" unless specific evidence of a problem
- Confidence < 0.7 → do not recommend actions beyond read_only
- If a finding correlates with a recent rule reload, note that
- Authority levels: read_only (investigate), can_modify (change rules), can_alert (notify human)
- Prefer read_only first. Escalate only if read_only agent already ran and found a fixable cause.
```

### Why LLM Over More Static Rules

The static safety net handles binary conditions (is it broken or not). The LLM handles:

1. **Correlation.** Block rate spike + rule reload at the same time = probably intentional. Static rules can't reason about this.
2. **Trend detection.** p99 drifting upward over 30 minutes while still within bounds. Static rules only see the current value.
3. **Severity calibration.** 5% webhook failure rate at 3am with 10 events/min is different from the same rate at peak with 100 events/min.

### Cost

One Sonnet call every 5 min, ~2K tokens in, ~500 out. ~$0.02/hour, ~$15/month.

### Failure Modes

| Failure | Handling |
|---------|----------|
| LLM API down | Skip judge this cycle. Static thresholds still run. Log warning. |
| Malformed JSON response | Retry once with "respond with valid JSON only." If still bad, skip cycle. |
| Hallucinated anomaly | Agent validates with actual data before acting. Read-only authority by default. |
| Keeps flagging same issue | Cooldown: same finding signature → skip after 3 consecutive flags without resolution. |

---

## Part 4: Agent — `agent.go` (~200 lines)

### One Agent, Authority Levels

Not 5 separate agents. One `runAgent` function with different tool sets based on authority.

```go
type Authority int
const (
    ReadOnly  Authority = iota // query DuckDB, read rule files, read logs
    CanModify                  // + write rule files, POST /admin/rules/reload
    CanAlert                   // + send Slack/email
)

type Mandate struct {
    Authority   Authority
    Instruction string            // from the judge: what to investigate/fix
    Parameters  map[string]string // focus_rule, target_snapshot, etc.
}

type AgentReport struct {
    RootCause   string `json:"root_cause"`
    Confidence  float64 `json:"confidence"`
    ActionTaken string `json:"action_taken"` // "none", "rolled back rule X", "sent alert"
    Evidence    []string `json:"evidence"`
    Suggestion  string `json:"suggestion"`   // for next judge cycle
}

func runAgent(ctx context.Context, m Mandate, db *sql.DB, rulesDir, adminURL string) (*AgentReport, error)
```

### Tools

Each tool is a simple function. The LLM gets a tool list based on authority.

```go
// Always available (ReadOnly)
func toolQueryDuckDB(db *sql.DB, sql string) (string, error)     // ~15 lines
func toolReadRule(rulesDir, ruleID string) (string, error)        // ~10 lines
func toolGetSnapshot(adminURL string) (string, error)             // ~10 lines

// CanModify
func toolWriteRule(rulesDir, filename, content string) error      // ~10 lines
func toolTriggerReload(adminURL string) error                     // ~10 lines

// CanAlert
func toolSendAlert(webhookURL, severity, message string) error    // ~15 lines
```

~70 lines for all tools combined. The rest of `agent.go` is the LLM call with tool dispatch loop.

### How the "5 Agents" Map to Authority Levels

| Original plan | Concise version |
|---------------|----------------|
| Diagnostics Agent | `runAgent(ReadOnly, "investigate why block rate spiked")` |
| Rule Rollback Agent | `runAgent(CanModify, "revert spam-check.star to previous version")` |
| Rule Tuning Agent | `runAgent(CanModify, "lower threshold in spam-check.star from 5 to 10")` |
| Capacity Agent | `runAgent(CanAlert, "system overloaded, alert human to adjust workers")` — capacity changes need a restart, so alert instead of auto-fix |
| Alerting Agent | `runAgent(CanAlert, "webhook endpoint appears down, notify ops")` |

The LLM decides which tools to call based on the mandate. "Diagnostics" and "rollback" aren't different code paths — they're the same function with different instructions and permissions.

### Agent Prompt

```
You are an operations agent for a Fruitfly rules engine.

Your mandate: {mandate.Instruction}
Parameters: {mandate.Parameters}

Available tools: {tools_for_authority_level}

Rules:
- Query actual data before concluding anything
- If your authority is read_only, report findings only
- If your authority is can_modify, make the smallest change that fixes the issue
- If modifying a rule, preserve the original value as a comment with timestamp
- If your authority is can_alert, send a concise message with evidence
- Always verify your action worked (query DuckDB or /admin/rules after changes)

Output JSON: {AgentReport schema}
```

### Guardrails

Enforced in `monitor.go`, not in the agent:

```go
var (
    lastDispatch = make(map[Authority]time.Time)
    cooldown     = 15 * time.Minute
    maxPerHour   = 6
    dispatchCount int
    dispatchHour  time.Time
)

func canDispatch(auth Authority) bool {
    if os.Getenv("FRUITFLY_AGENTS_DISABLED") == "true" { return false }
    if time.Since(lastDispatch[auth]) < cooldown { return false }
    if dispatchHour == currentHour() && dispatchCount >= maxPerHour { return false }
    return true
}
```

~20 lines. No dispatcher module.

---

## Part 5: The Main Loop — inside `monitor.go`

The entire monitor is one goroutine:

```go
func Run(ctx context.Context, db *sql.DB, adminURL, baselinePath, alertWebhook string) error {
    baseline := loadBaseline(baselinePath)
    var prevAssessment *Assessment
    var snapshots []map[string]any // rolling window of recent snapshots

    collectTick := time.NewTicker(60 * time.Second)
    judgeTick := time.NewTicker(5 * time.Minute)

    for {
        select {
        case <-ctx.Done():
            return nil

        case <-collectTick.C:
            snap := collect(db, adminURL)
            snapshots = appendCapped(snapshots, snap, 30) // keep last 30 min
            checkThresholds(snap, func(sev, msg string) {
                toolSendAlert(alertWebhook, sev, msg)
            })

        case <-judgeTick.C:
            if len(snapshots) == 0 { continue }
            current := snapshots[len(snapshots)-1]
            rulesDesc := getRulesDescription(adminURL)

            assessment, err := judge(current, baseline, prevAssessment, rulesDesc)
            if err != nil {
                slog.Warn("judge failed", "error", err)
                continue
            }
            prevAssessment = assessment

            for _, action := range assessment.Actions {
                auth := parseAuthority(action.Authority)
                if auth > ReadOnly && assessment.Confidence < 0.7 {
                    auth = ReadOnly // downgrade if judge isn't confident
                }
                if !canDispatch(auth) { continue }

                mandate := Mandate{
                    Authority:   auth,
                    Instruction: action.Mandate,
                    Parameters:  action.Parameters,
                }
                report, err := runAgent(ctx, mandate, db, rulesDir, adminURL)
                if err != nil {
                    slog.Error("agent failed", "error", err)
                    continue
                }
                recordDispatch(auth)
                slog.Info("agent completed", "report", report)
            }
        }
    }
}
```

~60 lines for the loop. That's the entire orchestration layer — no dispatcher module, no feedback loop module, no scheduling framework.

### Feedback Loop

The loop is implicit:
1. `prevAssessment` is passed to the next `judge()` call → judge sees its own history.
2. Agent actions affect fruitfly (rule reload, alert sent) → next `collect()` reflects the change.
3. If the issue resolved, judge says "healthy" next cycle. If not, it escalates.

No explicit feedback wiring needed.

### Dead Man's Switch

If the monitor process itself crashes, fruitfly keeps running (it's a sidecar, not embedded). To monitor the monitor:

```go
// In Run(), at the top of each collectTick:
os.WriteFile("/tmp/fruitfly-monitor-heartbeat", []byte(time.Now().Format(time.RFC3339)), 0644)
```

External watchdog (systemd, cron, etc.) checks if the heartbeat file is stale.

---

## Startup

```bash
# Run fruitfly
./fruitfly -config fruitfly.yaml &

# Run monitor as sidecar
./fruitfly monitor \
    --db fruitfly.duckdb \
    --admin-url http://localhost:8080 \
    --baseline baseline.json \
    --alert-webhook https://hooks.slack.com/... \
    --api-key $ANTHROPIC_API_KEY
```

Or embed it as an optional goroutine in `main.go` behind a config flag: `monitoring_enabled: true`.

---

## Implementation Roadmap

| Phase | Work | Effort | Lines |
|-------|------|--------|-------|
| **P1** | `monitor.go` — collect() + static thresholds + main loop | 2 days | ~300 |
| **P2** | `judge.go` — prompt builder + API call + response parser | 1.5 days | ~150 |
| **P3** | `agent.go` — single agent with tools + authority levels | 1.5 days | ~200 |
| **P4** | `monitor_test.go` — test with mock DuckDB + mock LLM responses | 1 day | ~150 |

**Total: ~6 days, ~800 lines.**

Start with P1. The collector and static thresholds give you monitoring with zero LLM cost. Then add P2+P3 as a layer on top.

---

## Tracking

| ID | Item | Status | Phase | Est. Lines |
|----|------|--------|-------|-----------|
| M1 | `collect()` — 5 DuckDB queries + 2 HTTP calls | ☐ | P1 | 80 |
| M2 | `checkThresholds()` — static safety net | ☐ | P1 | 40 |
| M3 | `Run()` — main loop with collect/judge/dispatch tickers | ☐ | P1 | 60 |
| M4 | Baseline loading + recompute flag | ☐ | P1 | 30 |
| M5 | Cooldown + guardrail enforcement | ☐ | P1 | 30 |
| M6 | Alert function (Slack webhook POST) | ☐ | P1 | 15 |
| M7 | Heartbeat file for dead man's switch | ☐ | P1 | 5 |
| M8 | Judge prompt builder | ☐ | P2 | 50 |
| M9 | Judge API call + structured response parsing | ☐ | P2 | 60 |
| M10 | Assessment + Finding + Action types | ☐ | P2 | 40 |
| M11 | Agent tool implementations (6 tools) | ☐ | P3 | 70 |
| M12 | Agent LLM call with tool dispatch loop | ☐ | P3 | 80 |
| M13 | Mandate + AgentReport types | ☐ | P3 | 30 |
| M14 | Authority level → tool set mapping | ☐ | P3 | 20 |
| M15 | Tests: mock DuckDB, mock LLM, verify dispatch logic | ☐ | P4 | 150 |
