---
name: spark-streaming-surprising-truths
description: Four counter-intuitive truths about Spark Streaming that reshape how you think about real-time processing. Use when understanding Spark's unified batch/streaming model, choosing between continuous and scheduled streaming, or explaining why streaming can be simpler and cheaper than batch.
---

# Spark Streaming: Four Surprising Truths

## Overview

Spark Structured Streaming challenges common assumptions about real-time data processing. Understanding these four truths helps you make better architectural decisions and leverage Spark's unified API effectively.

## Quick Start

### Truth 1: Real-Time Without a New Paradigm

```python
# Same Spark APIs for batch and streaming
# Batch
df = spark.table("orders")
result = df.groupBy("customer_id").agg(sum("amount"))

# Streaming (same code!)
df = spark.readStream.table("orders")
result = df.groupBy("customer_id").agg(sum("amount"))

# No new engine, no new mental model
```

### Truth 2: Streaming = Incremental Processing

```python
# Streaming doesn't mean 24/7 continuous
# It means incremental processing

# Continuous (runs forever)
.trigger(processingTime="30 seconds")

# Scheduled (processes backlog, then stops)
.trigger(availableNow=True)  # Schedule via Jobs

# Both use the same streaming API
# Choose based on business need, not technical requirement
```

### Truth 3: Checkpointing Changes Operations

```python
# Build batch pipelines as streaming from day one
# Avoids rewrites when SLAs tighten

# Daily → Every 4 hours: Just change trigger schedule
# No code changes needed

# Checkpointing eliminates brittle input parameters
# No more process_date parameters!
# Spark tracks what's new automatically
```

### Truth 4: Latency is a Business Decision

```python
# Latency requirements drive trigger choice

# < 1 second: Real-Time Mode
.trigger(realTime=True)

# 1 second - 1 hour: Microbatch
.trigger(processingTime="30 seconds")

# > 1 hour: Scheduled streaming
.trigger(availableNow=True)  # Schedule via Jobs

# Same code, different trigger = different latency
```

## Common Patterns

### Pattern 1: Unified Batch/Streaming Code

```python
# Write transformations once, use for both batch and streaming
def transform_orders(df):
    return (df
        .filter(col("status") == "complete")
        .groupBy("customer_id")
        .agg(sum("amount").alias("total"))
    )

# Batch usage
batch_df = spark.table("orders")
batch_result = transform_orders(batch_df)

# Streaming usage
stream_df = spark.readStream.table("orders")
stream_result = transform_orders(stream_df)
```

### Pattern 2: Cost-Optimized Scheduled Streaming

```python
# Use availableNow for batch-style cost control
# with streaming-grade correctness

# Process all new data, then stop
query = (df
    .writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", checkpoint_path)
    .start("target_table")
)

# Schedule via Databricks Jobs:
# - Every 4 hours
# - Once per day
# - Custom schedule

# Cluster only runs when processing
# Same checkpoint guarantees as continuous
```

### Pattern 3: Eliminating Input Parameters

```python
# BEFORE: Batch with manual date tracking
def process_orders(process_date):
    df = spark.table("orders")
    filtered = df.filter(col("order_date") == process_date)
    # Manual tracking of what's been processed
    # Risk of missing dates or duplicates

# AFTER: Streaming with checkpointing
def process_orders():
    df = spark.readStream.table("orders")
    # Spark tracks what's new automatically
    # Checkpoint handles progress tracking
    # No date parameters needed
```

## Reference Files

### Latency Spectrum

| Latency Range | Use Case | Trigger Type |
|--------------|----------|--------------|
| < 100ms | Real-time fraud detection | Real-Time Mode |
| 100ms - 1s | Near real-time alerts | Microbatch (1s) |
| 1s - 60s | Fast processing | Microbatch (30s) |
| 1min - 1hour | Operational reporting | Scheduled (availableNow) |
| > 1 hour | Traditional batch | Scheduled (availableNow) |

### Checkpoint Benefits

- **Automatic progress tracking**: No manual bookkeeping
- **Fault tolerance**: Automatic recovery on failures
- **Exactly-once semantics**: With Delta sink
- **Cost optimization**: Only process new data

## Common Issues

| Issue | Solution |
|-------|----------|
| **"Streaming is too complex"** | Start with `availableNow` - it's incremental batch |
| **"Continuous is expensive"** | Use scheduled streaming; same guarantees, lower cost |
| **"Need separate engine for real-time"** | Spark Real-Time Mode handles sub-second latency |
| **"Can't reuse batch code"** | Same DataFrame API works for both |

## Advanced Tips

### When to Use Each Trigger

```python
# Business SLA: 1 hour
# Trigger: SLA / 3 = 20 minutes
# Why divide by 3?
# - Failure recovery time
# - Processing buffer
# - Safety margin

.trigger(processingTime="20 minutes")
```

### Migration Path: Batch → Streaming

```python
# Step 1: Add readStream
df = spark.readStream.table("orders")  # Was: spark.table()

# Step 2: Add checkpoint
.writeStream.option("checkpointLocation", checkpoint_path)

# Step 3: Choose trigger
.trigger(availableNow=True)  # Start conservative

# Step 4: Change start() to writeStream.start()
.start("target_table")  # Was: .saveAsTable()
```

### Cost Comparison

```python
# Batch approach (late-arriving data):
# Day 1: Process Day 1, Day 0, Day -1 (reprocessing)
# Day 2: Process Day 2, Day 1, Day 0 (reprocessing)
# Cost: 3x (due to reprocessing)

# Streaming approach:
# Day 1: Process Day 1 (new only)
# Day 2: Process Day 2 (new only)
# Cost: 1x (checkpoint tracks progress)
```

## FAQ

**Q: Do I need to learn a new API for streaming?**
A: No. Spark Structured Streaming uses the same DataFrame API as batch.

**Q: When should I use continuous vs scheduled streaming?**
A: Continuous for < 1 second latency. Scheduled for cost optimization with same correctness guarantees.

**Q: Can I convert batch jobs to streaming?**
A: Yes. Add `readStream`, checkpoint location, and trigger. Same transformation code works.

**Q: Is streaming more expensive than batch?**
A: Often cheaper. Checkpointing avoids reprocessing late-arriving data.

**Q: What's the learning curve?**
A: Minimal if you know Spark batch. Same engine, same abstractions, different triggers.
