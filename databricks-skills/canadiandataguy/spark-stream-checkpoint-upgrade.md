---
name: spark-stream-checkpoint-upgrade
description: Step-by-step guide to upgrading Spark Streaming applications with new checkpoints. Use when making breaking changes, upgrading Spark versions, changing streaming logic, or reprocessing data from specific Kafka offsets.
---

# Upgrading Spark Stream with New Checkpoints

## Overview

Sometimes breaking changes require creating a new checkpoint. This skill covers extracting offset information from existing checkpoints and starting new streams from specific Kafka offsets, enabling controlled upgrades and reprocessing.

## Quick Start

### Extract Offset Information

```python
import json

checkpoint_location = "/checkpoint_location/checkpoint_for_kafka_to_delta"

# List commits to find latest
commits = dbutils.fs.ls(f"{checkpoint_location}/commits")
latest_commit = max([int(f.name) for f in commits])

# Read corresponding offset file
offset_content = dbutils.fs.head(f"{checkpoint_location}/offsets/{latest_commit}")
offset_data = json.loads(offset_content)

# Extract topic and partition offsets
# Format: {"topic_name": {"partition": offset, ...}}
topic_offsets = offset_data["source"][0]["endOffset"]
print(topic_offsets)
# Output: {"topic_name": {"0": 400000, "1": 300000}}
```

### Start New Stream from Specific Offset

```python
# Convert to startingOffsets format
starting_offsets = json.dumps(topic_offsets)

# Start new stream from these offsets
new_stream = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_servers)
    .option("subscribe", "topic_name")
    .option("startingOffsets", starting_offsets)  # Custom starting point
    .load()
)

# Write with NEW checkpoint location
new_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/new_checkpoint_location") \
    .start("target_table")
```

## Common Patterns

### Pattern 1: Extract and Use Offsets

```python
def extract_latest_offsets(checkpoint_path):
    """Extract latest processed offsets from checkpoint"""
    import json
    
    # Find latest commit
    commits = dbutils.fs.ls(f"{checkpoint_path}/commits")
    latest_commit = max([int(f.name) for f in commits])
    
    # Read offset file
    offset_file = f"{checkpoint_path}/offsets/{latest_commit}"
    content = dbutils.fs.head(offset_file)
    offset_data = json.loads(content)
    
    # Extract endOffset (last processed offset)
    return offset_data["source"][0]["endOffset"]

# Use extracted offsets
old_offsets = extract_latest_offsets("/old_checkpoint")
starting_offsets = json.dumps(old_offsets)

# Start new stream
new_stream = (spark
    .readStream
    .format("kafka")
    .option("startingOffsets", starting_offsets)
    .load()
)
```

### Pattern 2: Reprocess from Specific Point

```python
# Scenario: Reprocess from offset 100000 for partition 0
custom_offsets = {
    "topic_name": {
        "0": 100000,  # Start from offset 100000
        "1": "latest"  # Start from latest for other partitions
    }
}

starting_offsets = json.dumps(custom_offsets)

stream = (spark
    .readStream
    .format("kafka")
    .option("startingOffsets", starting_offsets)
    .load()
)
```

### Pattern 3: Checkpoint Structure Understanding

```python
# Checkpoint folder structure:
checkpoint_location/
├── metadata/      # Query ID, configuration
├── offsets/       # WAL: Intent to process (batch N)
│   ├── 0
│   ├── 1
│   └── 2
├── commits/       # Confirmation: Completed (batch N)
│   ├── 0
│   ├── 1
│   └── 2
├── sources/       # Source metadata (Kafka topic, starting offsets)
└── state/         # Stateful operations (if any)

# Key insight:
# - Offset file written at START of batch
# - Commit file written at END of batch
# - If commit missing → batch not completed → will reprocess
```

## Reference Files

### Offset File Format

```json
{
  "batchWatermarkMs": 0,
  "batchTimestampMs": 1674623173851,
  "conf": {
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.streaming.stateStore.providerClass": "..."
  },
  "source": [{
    "description": "KafkaSource[Subscribe[topic_name]]",
    "startOffset": {"topic_name": {"0": 399000, "1": 299000}},
    "endOffset": {"topic_name": {"0": 400000, "1": 300000}},
    "latestOffset": {"topic_name": {"0": 500000, "1": 400000}},
    "numInputRows": 2000
  }]
}
```

### When New Checkpoint Required

- **Breaking code changes**: Logic changes incompatible with old checkpoint
- **Spark version upgrade**: Spark 2.x → 3.x requires new checkpoint
- **Reprocessing needed**: Want to reprocess from specific point
- **Checkpoint corruption**: Old checkpoint unusable

### When Checkpoint Can Be Reused

- **Non-breaking changes**: Adding columns, changing output table
- **Configuration changes**: Trigger interval, output mode
- **Minor logic changes**: Compatible transformations

## Common Issues

| Issue | Solution |
|-------|----------|
| **Can't find offset file** | Check commits folder; use latest commit number |
| **Offset format incorrect** | Use JSON string format for startingOffsets |
| **New stream starts from latest** | Default is "latest"; must specify startingOffsets |
| **Checkpoint conflicts** | Use completely new checkpoint location |
| **Missing commits** | Normal if job crashed; Spark will reprocess |

## Advanced Tips

### Offset Semantics

```python
# Important: endOffset is EXCLUSIVE
# If endOffset = 400000, offsets 0-399999 were processed
# Next batch starts at 400000

# For startingOffsets:
# - Use endOffset from previous checkpoint
# - Or use startOffset if you want to reprocess that batch
```

### Safe Upgrade Procedure

```python
# 1. Stop old stream
old_query.stop()

# 2. Extract offsets
old_offsets = extract_latest_offsets("/old_checkpoint")

# 3. Start new stream with new checkpoint
new_stream = (spark
    .readStream
    .format("kafka")
    .option("startingOffsets", json.dumps(old_offsets))
    .load()
)

new_query = new_stream.writeStream \
    .option("checkpointLocation", "/new_checkpoint") \
    .start("target_table")

# 4. Keep old checkpoint for rollback
# 5. Monitor new stream for issues
```

### Verifying Offset Extraction

```python
# Pretty print offset data for verification
offset_data = json.loads(dbutils.fs.head(offset_file))
print(json.dumps(offset_data, indent=2))

# Key fields to verify:
# - endOffset: Last processed offset (use for startingOffsets)
# - latestOffset: Current position in Kafka (for reference)
# - numInputRows: Rows processed in this batch
```

## FAQ

**Q: Can I reuse the same checkpoint location?**
A: No. Use a new checkpoint location. Old checkpoint contains incompatible state.

**Q: What if I want to reprocess everything?**
A: Use `startingOffsets = "earliest"` or delete checkpoint and restart.

**Q: How do I know which offset to use?**
A: Use `endOffset` from latest commit. This is the last successfully processed offset.

**Q: What happens to data already written?**
A: If writing to Delta with idempotency (txnVersion/txnAppId), duplicates are prevented automatically.

**Q: Can I change checkpoint location without extracting offsets?**
A: No. You must extract offsets and specify startingOffsets, otherwise stream starts from "latest".
