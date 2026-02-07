---
name: mastering-checkpoints-in-spark-streaming
description: Deep dive into Spark Structured Streaming checkpoints covering stateless vs stateful jobs, state stores, and exactly-once semantics. Use when understanding checkpoint internals, debugging streaming failures, managing state stores, or implementing fault-tolerant streaming pipelines.
---

# Mastering Checkpoints in Spark Streaming

## Overview

Checkpoints are the foundation of Spark Structured Streaming's fault tolerance and exactly-once processing guarantees. Understanding checkpoint internals is essential for production streaming workloads.

**Key Concepts**:
- **Stateless jobs**: Only track offsets (metadata)
- **Stateful jobs**: Track offsets + state (e.g., duplicates, aggregations)
- **Exactly-once**: Achieved via checkpoint + Delta transaction logs

## Quick Start

### Checkpoint Folder Structure

```python
# List checkpoint contents
dbutils.fs.ls("/Volumes/catalog/volume/checkpoints/stream")

# Typical structure:
# ├── metadata/      # Query ID
# ├── offsets/       # What to process (intent)
# ├── commits/       # What completed (confirmation)
# ├── sources/       # Source metadata
# └── state/         # Stateful operations (if any)
```

### Stateless vs Stateful Checkpoints

```python
# Stateless (read from Kafka, write to Delta)
# Checkpoint: metadata, offsets, commits, sources
# No state folder

df = (spark.readStream
    .format("kafka")
    .option("subscribe", "topic")
    .load())

# Stateful (with watermark and deduplication)
# Checkpoint: + state folder
df_stateful = (df
    .withWatermark("timestamp", "10 minutes")
    .dropDuplicates(["partition", "offset"])
)
```

## Common Patterns

### Pattern 1: Read Checkpoint Contents

```python
import json

# Read offset file
offset_file = "/checkpoints/stream/offsets/223"
content = dbutils.fs.head(offset_file)
offset_data = json.loads(content)

# Pretty print
print(json.dumps(offset_data, indent=2))

# Key fields:
# - batchWatermarkMs: Watermark timestamp
# - batchTimestampMs: When batch started
# - source[0].startOffset: Beginning of batch (inclusive)
# - source[0].endOffset: End of batch (exclusive)
# - source[0].latestOffset: Current position in source
```

### Pattern 2: Read State Store

```python
# Query state store directly
state_df = (spark
    .read
    .format("statestore")
    .load("/checkpoints/stream/state")
)

state_df.show()
# Shows: key, value, partitionId, expiration timestamp

# Read state metadata
state_metadata = (spark
    .read
    .format("state-metadata")
    .load("/checkpoints/stream")
)
state_metadata.show()
# Shows: operatorName, numPartitions, minBatchId, maxBatchId
```

### Pattern 3: Stateless Joins

```python
# Stream-static join (stateless)
stream_df = spark.readStream.table("iot_stream")
static_df = spark.table("device_dimensions")

joined = stream_df.join(static_df, "device_id")
# Checkpoint has no state folder
# Each microbatch reads latest static table version
```

## Reference Files

### Offset File Format

```json
{
  "batchWatermarkMs": 0,
  "batchTimestampMs": 1704067200000,
  "conf": [
    {"key": "spark.sql.streaming.stateStore.providerClass", "value": "..."}
  ],
  "source": [{
    "description": "KafkaSource[Subscribe[topic]]",
    "startOffset": {"topic": {"0": 3171, "1": 4500}},
    "endOffset": {"topic": {"0": 3200, "1": 4550}},
    "latestOffset": {"topic": {"0": 5000, "1": 6000}},
    "numInputRows": 79
  }],
  "sink": {...}
}
```

### Commit File Format

```json
{
  "nextBatchWatermarkMs": 0
  // Mostly empty for stateless jobs
  // Contains watermark info for stateful jobs
}
```

### State Store Schema

| Column | Description |
|--------|-------------|
| key | State key (e.g., dedup key) |
| value | State value |
| partitionId | State partition (matches shuffle partitions) |
| expirationMs | Epoch timestamp when state expires |

## Common Issues

| Issue | Solution |
|-------|----------|
| **State growing too large** | Reduce watermark duration; state expires after watermark |
| **Checkpoint corruption** | Delete checkpoint and restart (reprocesses from start/earliest) |
| **Slow state operations** | Check partition balance; ensure keys are evenly distributed |
| **Can't find commit file** | Normal if job crashed; Spark will reprocess on restart |
| **Offsets out of sync** | Offsets without matching commits = unprocessed batch |

## Advanced Tips

### Watermark Best Practices

```python
# Always set watermark for stateful operations
(df
    .withWatermark("eventTime", "10 minutes")  # State kept for 10 min
    .dropDuplicates(["userId", "eventId"])      # Expires after 10 min
)

# State size = f(watermark duration, key cardinality)
# 10 min watermark × 1M events/min = manageable
# 72 hour watermark × 1M events/min = very large
```

### Monitoring State Size

```python
# Check state partition balance
state_df = spark.read.format("statestore").load(checkpoint_path)
state_df.groupBy("partitionId").count().orderBy("count", ascending=False).show()

# Look for skew - one partition with 10x others = problem
```

### Recovery Scenarios

```python
# Scenario 1: Clean restart
# - Latest offset = 223
# - Commit 223 exists
# - Next batch starts at 224

# Scenario 2: Crash during batch
# - Latest offset = 223 (written at start)
# - Commit 223 missing (crash before finish)
# - On restart: reprocess offset 223
# - Delta deduplication prevents duplicates

# Scenario 3: Lost checkpoint
# - Delete checkpoint folder
# - Restart with startingOffsets=earliest
# - Reprocesses all data (idempotent if Delta target)
```

### Checkpoint Location Strategy

```python
def get_checkpoint_path(table_name):
    """
    Checkpoint should be:
    1. Tied to TARGET table (not source)
    2. In persistent storage (UC Volume, S3, ADLS)
    3. Organized systematically
    """
    return f"/Volumes/catalog/checkpoints/{table_name}"

# Why target-tied? Checkpoint contains source info already.
# Systematic location: Easy to find, backup, manage
```

## FAQ

**Q: Can I delete old checkpoint files?**
A: No. Spark manages checkpoint files automatically. Manual deletion can cause data loss.

**Q: What happens if I lose my checkpoint?**
A: Stream restarts from beginning (or earliest offset). Data won't be duplicated if writing to Delta.

**Q: How do I migrate a checkpoint to new location?**
A: Copy checkpoint folder, update code to new path, restart. Old checkpoint remains for rollback.

**Q: Why is my state folder growing?**
A: Check watermark duration. State expires after watermark time. Reduce watermark or key cardinality.

**Q: Can multiple streams share a checkpoint?**
A: No. Each stream needs its own unique checkpoint location.

**Q: What's the relationship between offsets and commits?**
A: Offset = intent to process. Commit = confirmation of completion. Both present = success.