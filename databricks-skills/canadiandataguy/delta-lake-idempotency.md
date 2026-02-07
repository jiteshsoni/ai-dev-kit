---
name: delta-lake-idempotency
description: How Delta Lake achieves exactly-once semantics through idempotent writes using query ID and epoch ID. Use when implementing fault-tolerant streaming pipelines, understanding exactly-once guarantees, or debugging duplicate data issues in streaming workloads.
---

# Delta Lake Idempotency

## Overview

Delta Lake provides exactly-once semantics through idempotent writes. By tracking query ID and epoch ID (batch ID), Delta can detect and prevent duplicate writes even if a streaming job reprocesses the same batch multiple times.

## Quick Start

### Enable Idempotent Writes

```python
# Streaming write with idempotency
df.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .option("txnAppId", "my_streaming_job") \
    .option("txnVersion", batch_id) \
    .start("target_table")

# Key parameters:
# - txnAppId: Unique identifier for your streaming job
# - txnVersion: Batch ID (monotonically increasing)
```

### How It Works

```python
# Delta transaction log tracks:
# {
#   "queryId": "stream-uuid-from-checkpoint",
#   "epochId": 223,  # Batch ID
#   "addedFiles": [...]
# }

# If same (queryId, epochId) writes again:
# - Delta detects duplicate
# - Skips write operation
# - No duplicates in table
```

## Common Patterns

### Pattern 1: Streaming with Idempotency

```python
def write_with_idempotency(df, batch_id):
    """Write microbatch with idempotency guarantees"""
    (df.write
        .format("delta")
        .option("txnAppId", "kafka_to_delta_job")
        .option("txnVersion", batch_id)  # From foreachBatch
        .mode("append")
        .saveAsTable("target_table")
    )

# Use in foreachBatch
stream.writeStream \
    .foreachBatch(write_with_idempotency) \
    .option("checkpointLocation", checkpoint_path) \
    .start()
```

### Pattern 2: Recovery After Failure

```python
# Scenario: Stream crashes during batch 223
# 1. Offset 223 written (intent)
# 2. Processing happens
# 3. Crash before commit

# On restart:
# 1. Spark reads checkpoint (offset 223)
# 2. No commit 223 found
# 3. Reprocesses batch 223 with same batch_id
# 4. Delta detects duplicate (same queryId + epochId)
# 5. Skips write (no duplicates)

# Result: Exactly-once semantics
```

### Pattern 3: Verify Idempotency

```python
# Check Delta history for duplicate detection
spark.sql("DESCRIBE HISTORY target_table").show()

# Look for:
# - Same queryId + epochId appearing multiple times
# - Indicates recovery happened
# - No duplicate data (Delta prevented it)

# Example:
# Version 100: queryId=abc, epochId=223
# Version 101: queryId=abc, epochId=223  # Duplicate detected, skipped
```

## Reference Files

### Transaction Log Entry

```json
{
  "commitInfo": {
    "operation": "STREAMING UPDATE",
    "operationParameters": {
      "queryId": "e5f6a7b8-c9d0-...",
      "epochId": "223",
      "outputMode": "Append"
    },
    "isBlindAppend": true
  },
  "add": [
    {
      "path": "part-00000-...parquet",
      "partitionValues": {},
      "size": 12345,
      "modificationTime": 1234567890
    }
  ]
}
```

### Idempotency Mechanism

1. **Query ID**: Extracted from checkpoint metadata (unique per stream)
2. **Epoch ID**: Batch ID (monotonically increasing)
3. **Duplicate Detection**: Delta checks if (queryId, epochId) already exists
4. **Skip Write**: If duplicate detected, skip write operation
5. **Exactly-Once**: Same batch can be processed multiple times, only written once

### Checkpoint Integration

```python
# Checkpoint provides queryId
# Stored in checkpoint metadata/

# Streaming job provides epochId (batch_id)
# Passed via txnVersion option

# Together: Unique identifier for each batch
```

## Common Issues

| Issue | Solution |
|-------|----------|
| **Duplicate data appearing** | Check txnAppId/txnVersion are set correctly |
| **Not using Delta sink** | Only Delta provides idempotency; other sinks may duplicate |
| **Different txnAppId** | Use consistent txnAppId per streaming job |
| **Batch ID not monotonic** | Ensure batch_id increases; don't reuse IDs |

## Advanced Tips

### Testing Idempotency

```python
# Test procedure:
# 1. Run stream for 10 batches
# 2. Stop stream
# 3. Delete commit files (not offsets!)
# 4. Restart stream
# 5. Verify: Row count same, no duplicates

# Expected: Exactly same result as first run
```

### Multiple Streams to Same Table

```python
# Each stream needs unique txnAppId
stream1.writeStream \
    .option("txnAppId", "stream1_job") \
    .start("shared_table")

stream2.writeStream \
    .option("txnAppId", "stream2_job") \
    .start("shared_table")

# Different txnAppId = different query identity
# Both can write to same table safely
```

### Monitoring Idempotency

```python
# Check for duplicate detection in history
history = spark.sql("DESCRIBE HISTORY target_table").collect()

# Look for same (queryId, epochId) multiple times
# Indicates recovery happened, duplicates prevented

# Query to find recoveries:
spark.sql("""
    SELECT 
        operationParameters.queryId,
        operationParameters.epochId,
        COUNT(*) as occurrences
    FROM (
        SELECT 
            operationParameters
        FROM delta.`/delta/target_table`
        WHERE operation = 'STREAMING UPDATE'
    )
    GROUP BY queryId, epochId
    HAVING COUNT(*) > 1
""")
```

## FAQ

**Q: Do I need to set txnAppId and txnVersion manually?**
A: For foreachBatch, yes. For direct Delta sink, Spark sets them automatically from checkpoint.

**Q: What if I change txnAppId?**
A: Delta treats it as a new query. Old batches can be reprocessed (not idempotent across AppIds).

**Q: Can I achieve exactly-once without Delta?**
A: Not easily. Delta's transaction log provides the mechanism. Other sinks may duplicate on recovery.

**Q: How does this work with multiple writers?**
A: Each writer needs unique txnAppId. Delta tracks each independently. Concurrent writes use optimistic concurrency.

**Q: What's the performance impact?**
A: Minimal. Duplicate check is fast (transaction log lookup). No data scanning required.
