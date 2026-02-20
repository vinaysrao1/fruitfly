# Querying DuckDB

Fruitfly stores every evaluation result in an embedded DuckDB database (default: `fruitfly.duckdb`). This is your durable audit trail — every event, every verdict, every triggered rule.

## Schema

```sql
CREATE TABLE results (
    event_id       VARCHAR PRIMARY KEY,
    event_type     VARCHAR NOT NULL,
    verdict        VARCHAR NOT NULL,     -- approve, block, review
    triggered_rules JSON,                -- [{rule_id, verdict, reason}, ...]
    failed_rules   JSON,                 -- [{rule_id, error}, ...]
    payload        JSON,                 -- original event payload
    latency_us     BIGINT,               -- evaluation time in microseconds
    processed_at   TIMESTAMP DEFAULT current_timestamp
);
```

## Connecting

While Fruitfly is stopped (DuckDB allows only one process to hold the lock):

```bash
duckdb fruitfly.duckdb
```

Or read-only while Fruitfly is running (DuckDB supports concurrent reads):

```bash
duckdb -readonly fruitfly.duckdb
```

Programmatic access (Python):

```python
import duckdb
con = duckdb.connect("fruitfly.duckdb", read_only=True)
```

## Useful Queries

### Recent results

```sql
SELECT event_id, event_type, verdict, latency_us, processed_at
FROM results
ORDER BY processed_at DESC
LIMIT 20;
```

### Verdict breakdown (last hour)

```sql
SELECT verdict, COUNT(*) as count
FROM results
WHERE processed_at >= now() - INTERVAL 1 HOUR
GROUP BY verdict
ORDER BY count DESC;
```

### Block rate over time (5-minute buckets)

```sql
SELECT
    time_bucket(INTERVAL '5 minutes', processed_at) AS bucket,
    COUNT(*) FILTER (WHERE verdict = 'block') AS blocks,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE verdict = 'block') / COUNT(*), 1) AS block_pct
FROM results
WHERE processed_at >= now() - INTERVAL 24 HOUR
GROUP BY bucket
ORDER BY bucket;
```

### Slowest events (P99 latency)

```sql
SELECT event_id, event_type, verdict, latency_us,
       ROUND(latency_us / 1000.0, 1) AS latency_ms
FROM results
WHERE processed_at >= now() - INTERVAL 1 HOUR
ORDER BY latency_us DESC
LIMIT 10;
```

### Latency percentiles

```sql
SELECT
    ROUND(quantile_cont(latency_us, 0.50) / 1000.0, 1) AS p50_ms,
    ROUND(quantile_cont(latency_us, 0.95) / 1000.0, 1) AS p95_ms,
    ROUND(quantile_cont(latency_us, 0.99) / 1000.0, 1) AS p99_ms
FROM results
WHERE processed_at >= now() - INTERVAL 1 HOUR;
```

### Which rules are triggering most?

```sql
SELECT
    r.rule_id,
    r.verdict,
    COUNT(*) AS trigger_count
FROM results,
     UNNEST(from_json(triggered_rules, '[{"rule_id":"VARCHAR","verdict":"VARCHAR"}]')) AS r
WHERE processed_at >= now() - INTERVAL 1 HOUR
GROUP BY r.rule_id, r.verdict
ORDER BY trigger_count DESC;
```

### Which rules are failing?

```sql
SELECT
    r.rule_id,
    r.error,
    COUNT(*) AS fail_count
FROM results,
     UNNEST(from_json(failed_rules, '[{"rule_id":"VARCHAR","error":"VARCHAR"}]')) AS r
WHERE processed_at >= now() - INTERVAL 24 HOUR
GROUP BY r.rule_id, r.error
ORDER BY fail_count DESC;
```

### Events blocked for a specific user

```sql
SELECT event_id, event_type, triggered_rules, processed_at
FROM results
WHERE verdict = 'block'
  AND json_extract_string(payload, '$.user_id') = 'user-123'
ORDER BY processed_at DESC;
```

### Throughput (events per second, last 10 minutes)

```sql
SELECT
    COUNT(*) AS total_events,
    ROUND(COUNT(*) / 600.0, 1) AS avg_eps
FROM results
WHERE processed_at >= now() - INTERVAL 10 MINUTE;
```

### Data size on disk

```sql
SELECT
    COUNT(*) AS rows,
    pg_size_pretty(SUM(LENGTH(payload::VARCHAR))) AS payload_size
FROM results;
```

## Retention

Fruitfly automatically deletes results older than 30 days (runs daily). To adjust, modify the `retainDays` constant in `output/writer.go`.

To manually clean up:

```sql
DELETE FROM results WHERE processed_at < now() - INTERVAL 7 DAY;
```

## Exporting Data

```sql
-- CSV
COPY results TO 'results.csv' (HEADER, DELIMITER ',');

-- Parquet
COPY results TO 'results.parquet' (FORMAT PARQUET);

-- Filtered export
COPY (SELECT * FROM results WHERE verdict = 'block') TO 'blocks.parquet' (FORMAT PARQUET);
```
