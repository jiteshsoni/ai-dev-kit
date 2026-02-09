---
name: state-management
description: State management patterns for Spark Structured Streaming - watermarks, late data handling, state store monitoring, and deduplication at scale.
tags: ["streaming", "state", "watermark", "deduplication", "late-data"]
---

# State Management

Handle stateful operations in streaming: watermarks for late data, state store monitoring, and deduplication at scale.

## Quick Decision Matrix

| Pattern | When to Use | Watermark |
|---------|-------------|-----------|
| [Late Data Handling](#late-data-handling) | Out-of-order events, delayed data | Required |
| [Deduplication](#deduplication) | Exactly-once, duplicate prevention | Required |
| [State Store Monitoring](#state-store-monitoring) | Debugging, performance tuning | Optional |

---

## Watermark Configuration

Define how long to wait for late-arriving data before considering it "too late".

### Basic Watermark

```python
from pyspark.sql.functions import col

df = (spark.readStream
    .format("kafka")
    .option("subscribe", "events")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withWatermark("event_time", "10 minutes")  # Late data threshold
)

# Watermark = latest_event_time - 10 minutes
# Events with timestamp < watermark are considered "too late"
# State for late events is automatically cleaned up
```

### Watermark Duration Selection

| Setting | Effect | Use Case |
|---------|--------|----------|
| `"10 minutes"` | Moderate latency | General streaming |
| `"1 hour"` | High completeness | Financial transactions |
| `"5 minutes"` | Low latency | Real-time analytics |
| `"24 hours"` | Batch-like | Backfill scenarios |

**Rule of thumb**: Start with 2-3× your p95 event latency. Monitor late data rate and adjust.

### Join-Specific Watermarks

Different watermarks for streams with different latencies:

```python
# Fast source: 5 minute watermark
impressions = (spark.readStream
    .format("kafka")
    .option("subscribe", "impressions")
    .load()
    .withWatermark("impression_time", "5 minutes")
)

# Slower source: 15 minute watermark  
clicks = (spark.readStream
    .format("kafka")
    .option("subscribe", "clicks")
    .load()
    .withWatermark("click_time", "15 minutes")
)

# Effective watermark = max(5, 15) = 15 minutes
joined = impressions.join(clicks, ...)
```

### Watermark Impact on Operations

| Operation | Impact with Watermark |
|-----------|----------------------|
| **Aggregations** | Late events update previous windows |
| **Deduplication** | Late events may trigger new dedup |
| **Stream-Stream Joins** | Late events matched if other side hasn't expired |
| **State Store** | Old state cleaned up after watermark |

---

## Late Data Handling

Strategies for events arriving after their watermark has passed.

### Update Output Mode

```python
from pyspark.sql.functions import window, count

windowed = (df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("user_id")
    )
    .agg(count("*").alias("event_count"))
)

# Update mode: corrected results when late data arrives
windowed.writeStream \
    .outputMode("update") \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/windowed") \
    .start("/delta/windowed_metrics")
```

### Dead Letter Queue Pattern

```python
from pyspark.sql.functions import unix_timestamp, current_timestamp, when, lit

def handle_late_data_with_dlq(batch_df, batch_id):
    """Route late data to dead letter queue"""
    
    # Calculate watermark for this batch
    watermark_time = batch_df.select(max("event_time")).first()[0] - expr("interval 10 minutes")
    
    # Identify late events
    late_events = batch_df.filter(col("event_time") < watermark_time)
    
    if late_events.count() > 0:
        # Write to DLQ
        (late_events
            .withColumn("late_by_seconds", 
                       unix_timestamp(current_timestamp()) - unix_timestamp(col("event_time")))
            .withColumn("batch_id", lit(batch_id))
            .write
            .mode("append")
            .format("delta")
            .saveAsTable("events_dlq"))
        
        # Return on-time events for processing
        on_time = batch_df.filter(col("event_time") >= watermark_time)
        return on_time
    
    return batch_df

(df
    .writeStream
    .foreachBatch(handle_late_data_with_dlq)
    .option("checkpointLocation", "/checkpoints/dlq_handler")
    .start("/delta/processed_events")
)
```

### Handling Without Watermarks

If you can't use watermarks (e.g., no timestamp column):

```python
# Process all events regardless of timing
df_no_watermark = (spark.readStream
    .format("kafka")
    .option("subscribe", "events")
    .load()
)

# Use complete output mode (not recommended for large state)
df_no_watermark.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .start()
```

---

## Deduplication

Remove duplicate events while managing state size.

### Basic Deduplication

```python
# Remove duplicates within watermark window
deduped = (df
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["event_id", "user_id"])
)

deduped.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/deduped") \
    .start("/delta/deduped_events")
```

### At-Scale Deduplication

For high-volume streams, use `forEachBatch` with Delta MERGE:

```python
def deduplicate_with_merge(batch_df, batch_id):
    """Merge-based deduplication for high volume"""
    
    # Create temp view
    batch_df.createOrReplaceTempView("new_events")
    
    # MERGE based on unique key
    spark.sql("""
        MERGE INTO deduped_events target
        USING new_events source
        ON source.event_id = target.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)

# Use microbatch approach
df.writeStream \
    .foreachBatch(deduplicate_with_merge)
    .option("checkpointLocation", "/checkpoints/dedup_merge")
    .start()
```

### Time-Based Deduplication

Remove duplicates within a time window:

```python
from pyspark.sql.functions import window

deduped_time = (df
    .withWatermark("event_time", "1 hour")
    .dropDuplicates(["user_id", window(col("event_time"), "1 minute")])
)
```

### Exact-Deduplication Without State Growth

Use Redis/Key-Value store for external deduplication:

```python
def deduplicate_with_external_store(batch_df, batch_id):
    """Use external store for exact deduplication"""
    
    # Get seen event IDs from Redis
    seen_ids = get_from_redis("seen_event_ids")
    
    # Filter out seen events
    new_events = batch_df.filter(~col("event_id").isin(seen_ids))
    
    if new_events.count() > 0:
        # Process new events
        new_events.write.mode("append").format("delta").saveAsTable("unique_events")
        
        # Add new IDs to Redis
        add_to_redis(new_events.select("event_id").collect())
```

---

## State Store Monitoring

Monitor and manage state store size and health.

### Read State Store

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

### Monitor State Size

```python
# Monitor in query progress
for stream in spark.streams.active:
    progress = stream.lastProgress
    if progress:
        state_metrics = progress.get("stateOperators", [])
        for state in state_metrics:
            print(f"State size: {state.get('numRowsTotal', 0)} rows")
            print(f"Memory used: {state.get('memoryUsedBytes', 0)} bytes")
            print(f"Custom metrics: {state.get('customMetrics', {})}")
```

### Clean Up State

If state grows too large:

1. **Increase watermark**: Expire state faster
2. **Partition state**: Distribute across partitions
3. **Use external state**: Redis/DynamoDB for large state
4. **Purge checkpoints**: Delete old checkpoint data

### State Store Configuration

```python
# Configure RocksDB state store
spark.conf.set("spark.sql.streaming.stateStore.providerClass", 
               "com.databricks.sql.streaming.state.RocksDBStateProvider")

# Adjust state retention
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.compression", "lz4")
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.thread.num", "4")
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.ttl.seconds", "3600")  # 1 hour TTL
```

---

## Best Practices

### Watermark Guidelines
- Set watermark duration based on business requirements
- Monitor `lateInputRate` metric
- Adjust watermark if late data is being dropped unnecessarily
- Consider DLQ for critical late events

### State Store Guidelines
- Start with 2-3× memory of expected state size
- Use RocksDB for large state (>100MB)
- Monitor state size growth
- Partition data to distribute state

### Deduplication Guidelines
- Use watermarks to bound state size
- Consider external stores for exact deduplication
- Monitor duplicate rate to adjust watermark
- Test with backfill data

### Performance Tuning

| Parameter | Default | Recommendation |
|-----------|---------|----------------|
| `spark.sql.shuffle.partitions` | 200 | Set to 2-3× number of cores |
| `spark.sql.streaming.stateStore.providerClass` | default | Use RocksDB for large state |
| Watermark duration | None | 2-3× p95 event latency |
| State TTL | Infinite | Set if business allows |
