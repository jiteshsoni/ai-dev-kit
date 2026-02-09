---
name: checkpoint-recovery
description: Checkpoint management and recovery patterns for Spark Structured Streaming - exactly-once semantics, failure recovery, and backfill patterns.
tags: ["streaming", "checkpoint", "recovery", "exactly-once", "backfill"]
---

# Checkpoint & Recovery

Manage checkpoints for exactly-once processing and handle failures gracefully.

## Quick Decision Matrix

| Scenario | Approach | See |
|----------|----------|-----|
| Normal operation | Automatic checkpoint recovery | [Exactly-Once Semantics](#exactly-once-semantics) |
| Lost checkpoint | Delete and restart with offsets | [Manual Recovery](#manual-recovery) |
| Reprocess historical data | Specify starting offsets | [Backfill Patterns](#backfill-patterns) |
| Schema change | New checkpoint location | [Schema Evolution](#schema-evolution-handling) |

---

## Exactly-Once Semantics

Achieve exactly-once processing through checkpointing and idempotent writes.

### Checkpoint Structure

```
checkpoint_location/
├── metadata/      # Query ID and configuration
├── offsets/       # Intent (what to process)
├── commits/       # Confirmation (what completed)
├── sources/       # Source metadata
└── state/         # Stateful operations (aggregations, joins)
```

### How Exactly-Once Works

```python
# 1. Checkpoint tracks progress
# 2. Delta idempotent writes (queryId + epochId)
# 3. Offset semantics (inclusive start, exclusive end)

# Offset file structure:
{
  "startOffset": {"topic": {"0": 100}},  # Inclusive - process this
  "endOffset": {"topic": {"0": 200}}     # Exclusive - stop before this
}
# Processes: 100, 101, ..., 199
# Does NOT process: 200
# Next batch starts at: 200
```

### Exactly-Once with forEachBatch

```python
def upsert_to_delta(microBatchDF, batch_id):
    """Idempotent write for exactly-once"""
    microBatchDF.write \
        .format("delta") \
        .option("txnVersion", batch_id) \
        .option("txnAppId", "my_stream_job_v1") \
        .mode("append") \
        .saveAsTable("target_table")

stream.writeStream \
    .foreachBatch(upsert_to_delta) \
    .option("checkpointLocation", "/checkpoints/target") \
    .start()
```

### Idempotency Keys

| Component | Purpose |
|-----------|---------|
| `txnAppId` | Unique identifier for the streaming job |
| `txnVersion` | Monotonically increasing batch ID |

---

## Checkpoint Organization

### Target-Tied Checkpoints

```python
def get_checkpoint_location(table_name):
    """Checkpoint tied to TARGET, not source"""
    return f"/Volumes/catalog/checkpoints/{table_name}"

# Why target-tied?
# - Checkpoint already contains source information
# - Systematic organization
# - Easy backup/restore
# - Clear ownership
```

### Naming Conventions

```
/Volumes/catalog/checkpoints/
├── bronze_events/           # Bronze table checkpoint
├── silver_orders/           # Silver table checkpoint
├── enriched_users/          # Enrichment job checkpoint
└── aggregated_metrics/      # Aggregation job checkpoint
```

### Persistent Storage

```python
# Use external storage (S3/ADLS), not DBFS
# DBFS is not recommended for production checkpoints

checkpoint_path = "s3://bucket/checkpoints/bronze_events"
# or
checkpoint_path = "/Volumes/catalog/checkpoints/bronze_events"
```

---

## Failure Recovery

### Automatic Recovery

Normal restart behavior:

```python
# Spark automatically:
# 1. Reads last committed offset from checkpoint
# 2. Compares with source
# 3. Reprocesses if commit missing
# 4. Delta handles deduplication via txnVersion

# Simply restart the job - no action needed
stream = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "events")
    .load()
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/events")
    .start("/delta/events")
)
```

### Manual Recovery

When checkpoint is corrupted or lost:

```python
# Step 1: Delete corrupted checkpoint
# dbutils.fs.rm("/checkpoints/events", recurse=True)

# Step 2: Restart with startingOffsets
stream = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "events")
    .option("startingOffsets", "earliest")  # or specific offsets
    .load()
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/events_v2")  # NEW location
    .start("/delta/events")
)

# Note: Delta sink handles deduplication, so reprocessing is safe
```

### Recovery Procedures

**Scenario 1: Minor checkpoint corruption**
```bash
# Delete only corrupted commit files
rm -rf /checkpoints/events/commits/*.crc
rm -rf /checkpoints/events/commits/[corrupted-file]
```

**Scenario 2: Complete checkpoint loss**
```python
# 1. Note current consumer lag
# 2. Delete checkpoint folder
# 3. Restart with appropriate startingOffsets
# 4. Monitor for duplicate processing (Delta handles dedup)
```

**Scenario 3: Schema evolution**
```python
# New checkpoint for schema changes
new_checkpoint = "/checkpoints/events_v2"
# Old checkpoint can be archived/deleted after migration
```

---

## Backfill Patterns

Reprocess historical data from specific offsets or time ranges.

### From Specific Offset

```python
# Start from specific offset
(spark
    .readStream
    .format("kafka")
    .option("subscribe", "events")
    .option("startingOffsets", '{"events":{"0":1000,"1":2000}}')
    .load()
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/backfill_2024")
    .trigger(availableNow=True)  # Process all, then stop
    .start("/delta/events_backfill")
)
```

### From Timestamp

```python
from datetime import datetime

# Convert timestamp to offsets (requires Kafka admin)
timestamp_ms = int(datetime(2024, 1, 1).timestamp() * 1000)

# Use timestamp-based starting point
(spark
    .readStream
    .format("kafka")
    .option("subscribe", "events")
    .option("startingOffsetsByTimestamp", f'{{"events":{{"0":{timestamp_ms}}}}}')
    .load()
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/backfill_jan2024")
    .trigger(availableNow=True)
    .start("/delta/events_backfill")
)
```

### Backfill with Separate Checkpoint

```python
# Always use separate checkpoint for backfills
normal_checkpoint = "/checkpoints/events"
backfill_checkpoint = "/checkpoints/events_backfill_jan2024"

# Run backfill job
(spark
    .readStream
    .format("kafka")
    .option("subscribe", "events")
    .option("startingOffsets", "earliest")
    .load()
    .writeStream
    .format("delta")
    .option("checkpointLocation", backfill_checkpoint)
    .trigger(availableNow=True)
    .start("/delta/events")
)
```

### Selective Backfill

```python
# Backfill specific partitions only
(spark
    .readStream
    .format("kafka")
    .option("subscribe", "events")
    .option("startingOffsets", '{"events":{"0":0,"1":0}}')  # Only partitions 0,1
    .option("endingOffsets", '{"events":{"0":10000,"1":10000}}')
    .load()
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/backfill_partitions_0_1")
    .trigger(availableNow=True)
    .start("/delta/events_backfill")
)
```

---

## Error Handling

### Graceful Error Handling

```python
def safe_batch_processing(batch_df, batch_id):
    """Handle errors without failing entire stream"""
    try:
        # Process batch
        batch_df.write \
            .mode("append") \
            .format("delta") \
            .saveAsTable("target")
        
        # Log success
        print(f"Batch {batch_id} processed successfully")
        
    except Exception as e:
        # Log error but don't fail stream
        print(f"Error processing batch {batch_id}: {str(e)}")
        
        # Write to error table
        (batch_df
            .withColumn("error_batch_id", lit(batch_id))
            .withColumn("error_message", lit(str(e)))
            .withColumn("error_timestamp", current_timestamp())
            .write
            .mode("append")
            .format("delta")
            .saveAsTable("error_events"))

stream.writeStream \
    .foreachBatch(safe_batch_processing) \
    .option("checkpointLocation", "/checkpoints/safe_processing") \
    .start()
```

### Dead Letter Queue

```python
def process_with_dlq(batch_df, batch_id):
    """Route bad records to DLQ"""
    
    # Identify bad records
    bad_records = batch_df.filter(col("event_type").isNull())
    good_records = batch_df.filter(col("event_type").isNotNull())
    
    # Write bad records to DLQ
    if bad_records.count() > 0:
        (bad_records
            .withColumn("dlq_reason", lit("null_event_type"))
            .withColumn("dlq_timestamp", current_timestamp())
            .write
            .mode("append")
            .format("delta")
            .saveAsTable("events_dlq"))
    
    # Process good records
    if good_records.count() > 0:
        (good_records
            .write
            .mode("append")
            .format("delta")
            .saveAsTable("events"))

stream.writeStream \
    .foreachBatch(process_with_dlq) \
    .option("checkpointLocation", "/checkpoints/dlq_processing") \
    .start()
```

---

## Schema Evolution Handling

### New Checkpoint for Schema Changes

```python
# Old checkpoint incompatible with new schema
old_checkpoint = "/checkpoints/events_v1"
new_checkpoint = "/checkpoints/events_v2"

# Use new checkpoint location for schema evolution
(spark
    .readStream
    .format("kafka")
    .option("subscribe", "events")
    .load()
    .withColumn("new_field", lit(None))  # Add new nullable field
    .writeStream
    .format("delta")
    .option("mergeSchema", "true")
    .option("checkpointLocation", new_checkpoint)
    .start("/delta/events")
)
```

---

## Production Checklist

- [ ] Checkpoint on persistent storage (S3/ADLS, not DBFS)
- [ ] Unique checkpoint per stream
- [ ] Target-tied checkpoint organization
- [ ] Idempotent writes configured (`txnAppId`, `txnVersion`)
- [ ] Recovery procedures documented
- [ ] Backfill process tested
- [ ] Monitoring for checkpoint health
- [ ] Schema evolution plan

---

## Troubleshooting

| Issue | Symptom | Solution |
|-------|---------|----------|
| Checkpoint not found | `Checkpoint not found` error | Verify path exists and is accessible |
| Corrupted checkpoint | `Invalid checkpoint` error | Delete checkpoint, restart with offsets |
| Stale data | Processing old events | Check `startingOffsets` configuration |
| Duplicate data | Multiple writes of same data | Verify `txnAppId` and `txnVersion` unique per job |
| State too large | OOM errors | Increase watermark, use RocksDB, reduce stateful operations |
