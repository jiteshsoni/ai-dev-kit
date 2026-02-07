---
name: "spark-structured-streaming-expert-pack"
description: "Comprehensive expert guide to Spark Structured Streaming covering checkpointing, idempotency, joins, aggregations, RTM, and production patterns."
tags: ["spark-streaming", "expert", "reference", "production"]
---

# Spark Structured Streaming Expert Pack

## Overview

This expert pack consolidates deep knowledge on Spark Structured Streaming for production workloads.

## Core Concepts

### Checkpointing and Exactly-Once Semantics

```python
# Checkpoint folder structure:
# ├── metadata/      # Query ID
# ├── offsets/       # Intent (what to process)
# ├── commits/       # Confirmation (what completed)
# ├── sources/       # Source metadata
# └── state/         # Stateful operations

# Exactly-once achieved via:
# 1. Checkpoint tracks progress
# 2. Delta idempotent writes (queryId + epochId)
# 3. Offset semantics (inclusive start, exclusive end)
```

### Offset Semantics

```python
# Offset file:
{
  "startOffset": {"topic": {"0": 100}},  # Inclusive
  "endOffset": {"topic": {"0": 200}}     # Exclusive
}
# Processes: 100, 101, ..., 199
# Does NOT process: 200
# Next batch starts at: 200
```

## Stream Processing Patterns

### Pattern 1: Spark to Delta Streaming

```python
(spark
    .readStream
    .format("kafka")
    .option("subscribe", "topic")
    .option("startingOffsets", "earliest")
    .load()
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/bronze")
    .trigger(availableNow=True)  # Or processingTime
    .start("/delta/bronze_table")
)
```

### Pattern 2: Checkpointing Best Practices

```python
def get_checkpoint_location(table_name):
    """Checkpoint tied to TARGET, not source"""
    return f"/Volumes/catalog/checkpoints/{table_name}"

# Why target-tied? Checkpoint already contains source info.
# Benefits: Systematic organization, easy backup/restore
```

### Pattern 3: Idempotency Configuration

```python
# For exactly-once in forEachBatch:
(df
    .write
    .format("delta")
    .option("txnVersion", batch_id)
    .option("txnAppId", "my_stream_job")
    .mode("append")
    .saveAsTable("target")
)
```

## Joins

### Stream-Stream Joins

```python
# Two streaming sources
stream1 = spark.readStream.table("stream_a")
stream2 = spark.readStream.table("stream_b")

joined = (stream1
    .join(stream2, 
          expr("""
            stream_a.key = stream_b.key AND
            stream_a.ts >= stream_b.ts - interval 5 minutes AND
            stream_a.ts <= stream_b.ts + interval 5 minutes
          """),
          "inner"
    )
    .withWatermark("ts", "10 minutes")
)
```

### Stream-Batch (Stream-Static) Joins

```python
# Stream + Delta dimension table
stream = spark.readStream.table("events")
dim = spark.table("dimensions")  # Delta table

# Left join recommended for production
enriched = stream.join(dim, "key", "left")

# Why Delta? Version checking happens per microbatch
# Non-Delta formats: Read once at startup only
```

## Aggregations and State Management

### Watermarking

```python
# Event-time processing with watermarks
windowed = (df
    .withWatermark("event_time", "10 minutes")  # Late data threshold
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("user_id")
    )
    .agg(sum("amount"))
)

# State expires after watermark duration
# Reduces state store size
```

### State Store Monitoring

```python
# Read state store directly
state_df = (spark
    .read
    .format("statestore")
    .load("/checkpoint/state")
)

# Check partition balance
state_df.groupBy("partitionId").count().show()

# State metadata
spark.read.format("state-metadata").load("/checkpoint").show()
```

## Real-Time Mode (RTM)

### RTM vs Microbatch

```python
# Microbatch (default)
.trigger(processingTime="1 second")
# - Schedules every interval
# - Higher latency (scheduling overhead)

# Real-Time Mode
.trigger(realTime=True)
# - Pre-allocated resources
# - Sub-second latency
# - Higher resource usage
```

### When to Use RTM

| Latency | Mode |
|---------|------|
| < 800ms | RTM or Flink |
| > 800ms | Microbatch (cost-effective) |

## Advanced Patterns

### Output Modes

```python
# Append: Only new rows (default for most sinks)
.writeStream.outputMode("append")

# Update: Changed rows only
.writeStream.outputMode("update")

# Complete: Entire result table (aggregations)
.writeStream.outputMode("complete")
```

### Deduplication

```python
# Drop duplicates with watermark
(df
    .withWatermark("timestamp", "10 minutes")
    .dropDuplicates(["user_id", "event_id"])
)

# State stores seen keys
# Expires after watermark duration
```

### Schema Evolution

```python
# Handle evolving schemas
(df
    .writeStream
    .option("mergeSchema", "true")
    .start("/delta/table")
)
```

### Backfill Pattern

```python
# Backfill from specific offset
(spark
    .readStream
    .format("kafka")
    .option("startingOffsets", """{"topic": {"0": 1000}}""")
    .load()
    # ... rest of pipeline
)
```

### Tuning and Triggers

```python
# Trigger options:
.trigger(processingTime="30 seconds")  # Fixed interval
.trigger(availableNow=True)            # Process all, then stop
.trigger(realTime=True)                # Low latency
# No trigger = continuous (legacy)

# Guideline: SLA / 3
# Example: 1 hour SLA → 20 minute trigger
```

### Observability

```python
# Key metrics to monitor:
# 1. Input Rate vs Processing Rate (processing > input)
# 2. Max Offsets Behind Latest (should decrease)
# 3. Batch Duration vs Trigger Interval
# 4. State Store Size

# Programmatic access:
for stream in spark.streams.active:
    print(stream.status)
    print(stream.lastProgress)
```

### Recovery Procedures

```python
# Normal recovery (automatic):
# - Spark checks offset vs commit on restart
# - Reprocesses if commit missing
# - Delta handles deduplication

# Manual recovery (lost checkpoint):
# 1. Delete checkpoint folder
# 2. Restart with startingOffsets=earliest
# 3. Reprocesses all data (idempotent if Delta sink)
```

## Production Checklist

- [ ] Checkpoint location is persistent (S3/ADLS, not DBFS)
- [ ] Unique checkpoint per stream
- [ ] Target-tied checkpoint organization
- [ ] Fixed-size cluster (no autoscaling for streaming)
- [ ] Monitoring configured (input rate, lag, batch duration)
- [ ] Alerting for falling behind
- [ ] Recovery procedure documented
- [ ] Exactly-once verified (forEachBatch uses txnVersion)
- [ ] State size monitored (watermark configured)
- [ ] Left joins for stream-static (not inner)