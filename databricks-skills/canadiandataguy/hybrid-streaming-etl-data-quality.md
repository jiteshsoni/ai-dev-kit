---
name: hybrid-streaming-etl-data-quality
description: Strategies for ensuring data quality when coordinating upstream streaming jobs with downstream scheduled ETL processes. Use when designing hybrid architectures, monitoring streaming lag, or implementing data completeness checks for downstream jobs.
---

# Data Quality in Hybrid Streaming/ETL Architectures

## Overview

Coordinating upstream streaming ingestion with downstream scheduled ETL jobs requires monitoring streaming metrics and implementing data completeness checks. This skill covers strategies for ensuring downstream jobs only run when upstream data is complete and consistent.

## Quick Start

### Monitor Streaming Lag

```python
# Get streaming query metrics
query = spark.streams.active[0]
progress = query.lastProgress

# Kafka metrics
lag = progress["sources"][0]["avgOffsetsBehindLatest"]
max_lag = progress["sources"][0]["maxOffsetsBehindLatest"]

# Delta metrics
input_rate = progress["inputRowsPerSecond"]
processing_rate = progress["processedRowsPerSecond"]

# Decision: Only run downstream if lag < threshold
if max_lag < 1000:  # Less than 1000 offsets behind
    trigger_downstream_etl()
```

### StreamingQueryListener Pattern

```python
from pyspark.sql.streaming import StreamingQueryListener

class MetricsListener(StreamingQueryListener):
    def onQueryProgress(self, event):
        # Write metrics to Delta table
        metrics = {
            "timestamp": event.progress["timestamp"],
            "batchId": event.progress["batchId"],
            "inputRowsPerSecond": event.progress["inputRowsPerSecond"],
            "processedRowsPerSecond": event.progress["processedRowsPerSecond"],
            "maxOffsetsBehindLatest": event.progress["sources"][0].get("maxOffsetsBehindLatest", 0)
        }
        
        # Write to metrics table
        metrics_df = spark.createDataFrame([metrics])
        metrics_df.write.format("delta").mode("append").saveAsTable("streaming_metrics")

# Attach listener
spark.streams.addListener(MetricsListener())
```

## Common Patterns

### Pattern 1: Data Completeness Check

```python
# Check if streaming job is caught up before triggering downstream
def check_streaming_health():
    metrics = spark.sql("""
        SELECT 
            MAX(maxOffsetsBehindLatest) as max_lag,
            MAX(timestamp) as last_update
        FROM streaming_metrics
        WHERE timestamp > current_timestamp() - interval 5 minutes
    """).collect()[0]
    
    # Only proceed if lag acceptable
    if metrics.max_lag < 1000 and metrics.last_update is not None:
        return True
    return False

# Use in downstream job
if check_streaming_health():
    run_downstream_etl()
else:
    send_alert("Streaming lag too high")
```

### Pattern 2: Buffer-Based Scheduling

```python
# Wait for buffer period after expected data arrival
# Example: If data should arrive by 2 AM, wait until 2:30 AM

from datetime import datetime, timedelta

def should_run_downstream():
    # Expected completion time
    expected_time = datetime(2024, 1, 15, 2, 0, 0)
    
    # Buffer period (30 minutes)
    buffer = timedelta(minutes=30)
    
    # Current time
    now = datetime.now()
    
    # Only run after buffer period
    return now >= expected_time + buffer

# Schedule downstream job with buffer
if should_run_downstream():
    run_downstream_etl()
```

### Pattern 3: Row Count Validation

```python
# Validate expected row counts before downstream processing
def validate_data_completeness():
    # Expected rows per hour (from historical patterns)
    expected_rows_per_hour = 100000
    
    # Actual rows in last hour
    actual_rows = spark.sql("""
        SELECT COUNT(*) as cnt
        FROM streaming_table
        WHERE ingestion_time >= current_timestamp() - interval 1 hour
    """).collect()[0]['cnt']
    
    # Check if within acceptable range (90-110% of expected)
    if 0.9 * expected_rows_per_hour <= actual_rows <= 1.1 * expected_rows_per_hour:
        return True
    return False
```

## Reference Files

### Streaming Metrics by Source

**Kafka Metrics**:
- `avgOffsetsBehindLatest`: Average lag across partitions
- `maxOffsetsBehindLatest`: Maximum lag (bottleneck partition)
- `minOffsetsBehindLatest`: Minimum lag
- `estimatedTotalBytesBehindLatest`: Total bytes pending

**Delta Metrics**:
- `numInputRows`: Rows read per batch
- `inputRowsPerSecond`: Ingestion rate
- `processedRowsPerSecond`: Processing rate
- `batchId`: Batch identifier

**Kinesis Metrics**:
- `avgMsBehindLatest`: Average milliseconds behind
- `maxMsBehindLatest`: Maximum milliseconds behind
- `totalPrefetchedBytes`: Prefetched but unprocessed bytes

### Decision Flow

```
Upstream Streaming Job
    ↓
Writes to Data Lake
    ↓
Writes Metrics to Metrics Table
    ↓
Data Completeness Check
    ├─ Lag < Threshold? → Trigger Downstream ETL
    └─ Lag > Threshold? → Wait or Alert
```

## Common Issues

| Issue | Solution |
|-------|----------|
| **Downstream runs too early** | Add buffer period; check lag metrics |
| **Downstream runs on incomplete data** | Validate row counts; check timestamps |
| **Metrics not available** | Implement StreamingQueryListener |
| **False positives** | Use multiple validation methods |
| **Streaming job stuck** | Monitor and alert on high lag |

## Advanced Tips

### Multi-Metric Validation

```python
# Use multiple checks for robustness
def comprehensive_check():
    checks = {
        "lag_check": check_streaming_lag(),
        "row_count_check": validate_row_counts(),
        "timestamp_check": check_latest_timestamp(),
        "rate_check": check_processing_rate()
    }
    
    # All checks must pass
    return all(checks.values()), checks

# Run downstream only if all checks pass
all_pass, details = comprehensive_check()
if all_pass:
    run_downstream_etl()
else:
    log_failed_checks(details)
```

### Alerting Strategy

```python
# Alert when streaming job falls behind
def monitor_and_alert():
    lag = get_max_lag()
    threshold = 10000
    
    if lag > threshold:
        send_alert(
            f"Streaming lag exceeded threshold: {lag} > {threshold}",
            severity="high"
        )
    
    # Also check if metrics stopped updating
    last_update = get_last_metrics_update()
    if last_update < datetime.now() - timedelta(minutes=10):
        send_alert("Streaming metrics not updating", severity="critical")
```

## FAQ

**Q: How do I know when downstream ETL should run?**
A: Check streaming lag metrics. Only run when lag is below threshold and data is complete.

**Q: What if streaming job is stuck?**
A: Monitor metrics updates. If metrics stop updating, alert team. Don't run downstream on stale data.

**Q: Can I use dbt for data quality checks?**
A: Yes. Use dbt tests to validate data completeness, freshness, and quality before downstream processing.

**Q: How do I handle late-arriving data?**
A: Use buffer periods. Wait additional time after expected completion before running downstream jobs.

**Q: What metrics should I monitor?**
A: Lag (offsets behind), processing rate, input rate, and data freshness (latest timestamp).
