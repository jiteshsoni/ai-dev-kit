---
name: "Spark Streaming Master Class: Ingest data from Kafka to Delta with Spark Streaming"
description: "Complete guide to streaming data from Kafka to Delta Lake using Spark Structured Streaming, with checkpoint internals and production patterns."
author: "Canadian Data Guy"
url: "https://www.youtube.com/watch?v=snJs2DlzA0o"
date: "2025-11-29"
tags: ["spark-streaming", "kafka", "delta", "checkpoint", "microbatch", "production"]
---

# Spark Streaming Master Class: Kafka to Delta

## Overview

Spark Structured Streaming provides a unified API for both batch and streaming workloads. This guide covers production patterns for ingesting data from Kafka into Delta Lake with deep checkpoint internals.

**Key Insight**: Streaming = Incremental Processing (not necessarily continuous)

## Quick Start

### Basic Kafka Read

```python
from pyspark.sql.functions import *

# Read from Kafka
df = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_servers)
    .option("subscribe", "topic_name")
    .option("startingOffsets", "earliest")  # or "latest"
    .option("minPartitions", "6")  # Match Kafka partitions
    .load()
)

# Parse the value
df_parsed = df.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp")
)
```

### Write to Delta

```python
# Write with checkpointing
query = (df_parsed
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/catalog/volume/checkpoints/kafka_stream")
    .queryName("kafka_to_delta_bronze")  # Human-readable name
    .trigger(processingTime="30 seconds")  # Or availableNow=True
    .start("/path/to/target_table")
)
```

### Secure Credential Management

```python
# Store secrets in Databricks secret scope
dbutils.secrets.list("kafka-scope")
kafka_key = dbutils.secrets.get("kafka-scope", "api-key")
kafka_secret = dbutils.secrets.get("kafka-scope", "api-secret")
```

## Common Patterns

### Pattern 1: Bronze Layer (Raw Ingestion)

```python
# Best practice: Minimal transformation, preserve original columns
# Why: Kafka retention is expensive (default 7 days)
# Delta provides permanent storage with full history

df_bronze = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", servers)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 10000)  # Control batch size
    .load()
)

# Write raw to bronze
df_bronze.writeStream \
    .format("delta") \
    .option("checkpointLocation", bronze_checkpoint) \
    .trigger(availableNow=True) \
    .start(bronze_table)
```

### Pattern 2: Scheduled Streaming (Cost-Optimized)

```python
# Run every 4 hours, not continuously
# Same code, just change trigger in job scheduler

.writeStream \
    .trigger(availableNow=True) \  # Process all available, then stop
    .start(table)

# In Databricks Jobs:
# - Schedule: Every 4 hours
# - Cluster: Fixed size (no autoscaling for streaming)
# - Same streaming code, batch-style execution
```

### Pattern 3: Monitor Stream Health

```python
# Key metrics in Spark UI Structured Streaming tab:
# 1. Input Rate vs Processing Rate
#    - Processing must be > Input
# 2. Max Offsets Behind Latest
#    - Should be consistent or dropping
#    - Increasing = falling behind

# Get active streams programmatically
for stream in spark.streams.active:
    print(f"Stream: {stream.name}, Status: {stream.status}")
```

## Reference Files

### Checkpoint Internals

The checkpoint contains:

```
checkpoint_location/
├── metadata/          # Stream query ID
├── offsets/           # Intent: what to process
│   ├── 0
│   ├── 1
│   └── ...
├── commits/           # Confirmation: what completed
│   ├── 0
│   ├── 1
│   └── ...
└── sources/           # Source metadata
    └── 0/
        └── 0
```

**Offset File Structure**:
```json
{
  "batchWatermarkMs": 0,
  "batchTimestampMs": 1234567890,
  "conf": [...],
  "source": [{
    "description": "KafkaSource[...]",
    "startOffset": {"topic": {"0": 100, "1": 200}},
    "endOffset": {"topic": {"0": 150, "1": 250}},
    "latestOffset": {"topic": {"0": 500, "1": 600}},
    "numInputRows": 100
  }]
}
```

### Offset Semantics

- **Start Offset (inclusive)**: First message to process
- **End Offset (exclusive)**: First message NOT to process
- Example: start=100, end=200 → process offsets 100-199

## Common Issues

| Issue | Solution |
|-------|----------|
| **No data being read** | Check `startingOffsets` - default is "latest", use "earliest" for existing data |
| **Falling behind** | Increase cluster size or reduce `maxOffsetsPerTrigger` |
| **Small files problem** | Increase trigger interval (e.g., 2 minutes instead of 15 seconds) |
| **Duplicate data after restart** | Checkpoint handles this automatically - don't delete checkpoints |
| **Can't use autoscaling** | Streaming jobs need fixed-size clusters (no autoscaling in dedicated mode) |
| **Kafka retention expired** | Ingest to Delta immediately; Kafka is temporary, Delta is permanent |

## Advanced Tips

### Checkpoint Best Practices

```python
# Create a function for consistent checkpoint locations
def get_checkpoint_location(table_name):
    """Checkpoint should be tied to TARGET, not source"""
    return f"/Volumes/catalog/volume/checkpoints/{table_name}"

# Why? Checkpoint already contains source information
# Benefits: All checkpoints in one place, systematic organization
```

### Recovery After Failure

```python
# When stream restarts:
# 1. Read latest offset file (e.g., offset 223)
# 2. Check if commit 223 exists
# 3. If yes: proceed to offset 224
# 4. If no: reprocess offset 223 (exactly-once guarantee)

# Delta handles duplicate detection via:
# - Query ID (from metadata)
# - Epoch ID (batch ID)
# - Check Delta log before writing
```

### Performance Tuning

| Parameter | Recommendation | Why |
|-----------|---------------|-----|
| minPartitions | Match Kafka partitions | Optimal parallelism |
| maxOffsetsPerTrigger | 10,000-100,000 | Balance latency vs throughput |
| shufflePartitions | 200 (default) | Usually fine |
| trigger interval | Business SLA / 3 | Recovery time buffer |

### Monitoring Queries

```python
# Programmatic monitoring
streams = spark.streams.active
for s in streams:
    status = s.status
    print(f"{s.name}: {status['message']}")
    
# Key metrics to alert on:
# - Max Offsets Behind Latest (increasing = problem)
# - Batch Duration > Trigger Interval
# - Input Rate >> Processing Rate
```

## FAQ

**Q: Can I read from multiple Kafka topics?**
A: Yes, use `option("subscribe", "topic1,topic2,topic3")` or subscribe to pattern.

**Q: How do I handle schema evolution?**
A: Delta supports schema evolution. Use `.option("mergeSchema", "true")` when writing.

**Q: What's the difference between processingTime and availableNow?**
A: `processingTime` runs continuously. `availableNow` processes backlog then stops (good for scheduled jobs).

**Q: Can I use streaming with Auto Loader?**
A: Yes, for file-based sources (S3, ADLS). Use `spark.readStream.format("cloudFiles")`.

**Q: How many streaming jobs per cluster?**
A: Tested up to 100 jobs on 8-core single-node cluster. Monitor CPU/memory.