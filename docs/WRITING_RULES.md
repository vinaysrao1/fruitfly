# Writing Rules

Rules are [Starlark](https://github.com/google/starlark-go) files (`.star`) in the configured `rules_dir` (default: `./rules`). Each file is one rule. Rules are hot-reloaded — drop a file in the directory and Fruitfly picks it up within seconds.

## Rule Structure

Every rule file has two parts: **metadata** (top-level assignments) and an **`evaluate` function**.

```python
# rules/my_rule.star

# --- Metadata ---
rule_id    = "my-rule-v1"      # unique identifier (required)
event_type = "post"            # which events to match, or "*" for all (required)
priority   = 100               # higher wins; ties broken by verdict weight (required)

# --- Evaluation ---
def evaluate(event):
    # event is a dict: {"event_id", "event_type", "timestamp", "payload"}
    return verdict("approve")
```

### Metadata Fields

| Field | Type | Description |
|-------|------|-------------|
| `rule_id` | string | Unique across all rules. Appears in results and logs. |
| `event_type` | string | Only events with this type trigger the rule. Use `"*"` to match all. |
| `priority` | int | Higher priority rules take precedence. Among equal priority, `block (3) > review (2) > approve (1)`. |

### The `evaluate` Function

- Receives `event`, a dict with keys: `event_id`, `event_type`, `timestamp`, `payload`.
- `payload` is the arbitrary JSON object sent by the caller.
- Must return a `verdict(...)` call.

## Verdicts

```python
verdict("approve")                           # allow the event
verdict("block",  reason="spam detected")    # block with a reason
verdict("review", reason="borderline score") # flag for human review
```

## Built-in Functions

| Function | Description | Example |
|----------|-------------|---------|
| `verdict(type, reason="")` | Return a verdict | `verdict("block", reason="spam")` |
| `counter(entity_id, event_type, window_seconds)` | Sliding-window rate counter | `counter("user:123", "post", 3600)` |
| `memo(key, func)` | Cache a value within this event | `memo("score", lambda: expensive())` |
| `now()` | Current Unix timestamp | `now()` |
| `log(message)` | Emit a structured log line | `log("checking user")` |
| `hash(value)` | SHA256 hex string | `hash(email)` |
| `regex_match(pattern, text)` | Test regex match | `regex_match("^spam", subject)` |

## Tier-1 Prefilters (`match`)

A rule may declare an optional `match` global: native predicates evaluated
in nanoseconds before any Starlark runs. If any clause fails, the rule is
skipped silently (counted per rule as `prefiltered` in `/admin/rules`).

```python
match = {
    "all": [                                  # every clause must pass
        ["payload.char_count", "<", 20],      # numbers compare as float64
        ["payload.lang", "in", ["en", "es"]], # list must be homogeneous
    ],
}
```

Supported ops: `==`, `!=`, `<`, `<=`, `>`, `>=`, `in`, `exists`, `prefix`.
Paths address the event envelope (`payload.*`, `event_type`, `event_id`,
`entity_id`, `timestamp`). A missing or type-mismatched field makes the
clause false — the rule is skipped, never errored; use `exists` to test
presence explicitly.

## Cluster mode and counter keys

In cluster mode (`cluster_peers` configured), `counter()` keys must be
affine to the event's routing entity: the entity itself or a scoped
derivation like `entity + ":likes"`. Any other key is a rule error — it
would scatter increments across pods and silently undercount. To count by
another perspective (e.g. recipient on a sender-routed event), emit a
second event routed by that perspective.

## Examples

### Spam Filter

```python
rule_id = "spam-filter-v1"
event_type = "post"
priority = 100

def evaluate(event):
    score = event["payload"].get("spam_score", 0)
    if score > 0.9:
        return verdict("block", reason="spam score " + str(score))
    if score > 0.7:
        return verdict("review", reason="borderline spam")
    return verdict("approve")
```

### Rate Limiter

```python
rule_id = "rate-limit-posts"
event_type = "post"
priority = 200

def evaluate(event):
    user = event["payload"].get("user_id", "")
    if user == "":
        return verdict("approve")
    count = counter(user, "post", 3600)  # posts in last hour
    if count > 100:
        return verdict("block", reason="rate limit: " + str(count) + " posts/hr")
    if count > 80:
        return verdict("review", reason="approaching rate limit")
    return verdict("approve")
```

### Email Domain Blocklist

```python
rule_id = "email-blocklist"
event_type = "signup"
priority = 150

BLOCKED = ["tempmail.com", "throwaway.io", "fakeinbox.net"]

def evaluate(event):
    email = event["payload"].get("email", "")
    for domain in BLOCKED:
        if email.endswith("@" + domain):
            return verdict("block", reason="blocked domain: " + domain)
    return verdict("approve")
```

### Content Regex Scanner

```python
rule_id = "content-scanner"
event_type = "comment"
priority = 100

def evaluate(event):
    body = event["payload"].get("body", "")
    if regex_match("https?://[^ ]+\\.(ru|cn)/", body):
        return verdict("review", reason="suspicious URL pattern")
    if regex_match("(?i)(buy now|free money|click here)", body):
        return verdict("review", reason="spammy keywords")
    return verdict("approve")
```

### Wildcard Audit Logger

```python
rule_id = "audit-log-all"
event_type = "*"
priority = 1  # low priority — never overrides other rules

def evaluate(event):
    log("audit: " + event["event_type"] + " " + event["event_id"])
    return verdict("approve")
```

## Enabling a Rule

1. Write your `.star` file and place it in the `rules_dir` (default `./rules/`).
2. Fruitfly detects the new file via filesystem watcher and reloads automatically.
3. To force an immediate reload:
   ```bash
   curl -X POST http://localhost:8080/admin/rules/reload
   ```
4. Verify the rule is loaded:
   ```bash
   curl -s http://localhost:8080/admin/rules | jq
   ```

## Disabling a Rule

Remove or rename the `.star` file (e.g., `mv rules/spam.star rules/spam.star.disabled`). Fruitfly reloads and the rule is gone.

## Reload Behavior

- Reloads are **all-or-nothing**. If any rule has a syntax error, the entire reload is rejected and the previous rules remain active.
- Reloads are detected by `fsnotify` (instant) with a 10-second poll fallback.
- Workers see the new rules on the next event after the atomic swap.

## Tips

- Keep rules simple. Each rule should check one thing.
- Use `priority` to layer rules: high-priority blockers, mid-priority reviewers, low-priority approvers.
- Use `memo()` if multiple rules compute the same value from the payload.
- Counter windows max out at 1 hour (3600 seconds): `counter()` returns an error
  for larger windows rather than a silently truncated count, and the rule lands
  in `failed_rules`. Data older than 1 hour is garbage collected.
- Rules run in a sandbox — no file I/O, no network, no imports. Only the built-in functions above are available.
