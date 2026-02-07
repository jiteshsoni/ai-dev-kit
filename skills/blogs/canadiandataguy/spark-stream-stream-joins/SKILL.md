---
name: spark-stream-stream-joins
description: Deep dive on Spark Structured Streaming stream-stream joins with watermarks, temporal conditions, state management, and production patterns. Use when joining two streaming sources with event-time semantics, handling late data, or implementing windowed aggregations across streams.
---

# Spark Stream-Stream Joins

## Overview

Stream-stream joins enable you to correlate events from two streaming sources in real-time. Unlike stream-static joins, both sides are unbounded and continuously arriving, requiring careful handling of state and time semantics.

## Key Concepts

### Why Stream-Stream Joins Are Different

| Aspect | Stream-Static | Stream-Stream |
|--------|--------------|---------------|
| State | Stateless | Stateful |
| Memory | Current batch only | Buffered windows |
| Latency | Immediate | Window-dependent |
| Complexity | Simple | Requires watermarking |

### Event Time vs Processing Time

```python
# Event time: When the event actually occurred (recommended)
stream1 = spark.readStream.table("events").withWatermark("event_time", "10 minutes")

# Processing time: When Spark processes it (simpler but less accurate)
stream2 = spark.readStream.table("clicks")  # Uses processing time implicitly
```

## Inner Join with Watermarks

```python
from pyspark.sql.functions import expr

# Read both streams
impressions = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "impressions")
    .load()
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
    .withWatermark("impression_time", "10 minutes")  # Late data threshold
)

clicks = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "clicks")
    .load()
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
    .withWatermark("click_time", "10 minutes")
)

# Inner join: Only matches when both events arrive within the window
joined = (impressions
    .join(
        clicks,
        expr("""
            impressions.ad_id = clicks.ad_id AND
            clicks.click_time BETWEEN impressions.impression_time AND
                                    impressions.impression_time + interval 1 hour
        """),
        "inner"
    )
)

joined.writeStream.format("delta").start("/delta/attribution")
```

## Left Outer Join

```python
# Left outer join with time bounds
# Important: The output is delayed by the watermark duration
joined = (impressions
    .join(
        clicks,
        expr("""
            impressions.ad_id = clicks.ad_id AND
            clicks.click_time BETWEEN impressions.impression_time AND
                                    impressions.impression_time + interval 30 minutes
        """),
        "leftOuter"  # Impressions without clicks will appear after watermark expires
    )
)
```

**Key behavior**: Left outer join results for a given impression are emitted only after the watermark passes the impression's event time + window. This ensures no late clicks can match.

## Temporal Join Patterns

### 1. Time-Range Join

```python
# Match events within a time window
time_range_join = (stream1
    .join(
        stream2,
        expr("""
            stream1.user_id = stream2.user_id AND
            stream2.ts >= stream1.ts - interval 5 minutes AND
            stream2.ts <= stream1.ts + interval 5 minutes
        """),
        "inner"
    )
)
```

### 2. Session Window Join

```python
from pyspark.sql.functions import session_window

# Group by session windows before joining
sessioned1 = (stream1
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        session_window(col("event_time"), "10 minutes"),
        col("user_id")
    )
    .agg(sum("value").alias("total_value"))
)

sessioned2 = (stream2
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        session_window(col("event_time"), "10 minutes"),
        col("user_id")
    )
    .agg(sum("score").alias("total_score"))
)

joined = sessioned1.join(sessioned2, ["user_id", "session_window"], "inner")
```

## Watermark Management

### Choosing Watermark Duration

```python
# Trade-off: Late data tolerance vs state size
.withWatermark("event_time", "2 hours")   # High tolerance, large state
.withWatermark("event_time", "10 minutes") # Low tolerance, small state

# Rule of thumb: Max expected delay + safety margin
# If 99% of events arrive within 5 min, use 10 min watermark
```

### State Store Size Management

```python
# Monitor state store growth
state_metrics = spark.streams.active[0].lastProgress["stateOperators"]
for op in state_metrics:
    print(f"State rows: {op['numRowsTotal']}")
    print(f"State memory: {op['memoryUsedBytes']}")

# RocksDB for large state (Databricks)
spark.conf.set("spark.sql.streaming.stateStore.providerClass", 
               "com.databricks.sql.streaming.state.RocksDBStateProvider")
```

## Production Patterns

### 1. Idempotent Stream-Stream Joins

```python
# Add unique identifiers to prevent duplicates on restart
joined_with_id = joined.withColumn(
    "join_id", 
    concat_ws("_", col("stream1_id"), col("stream2_id"))
)

# Write with idempotency
joined_with_id.writeStream.foreachBatch(
    lambda df, batch_id: (
        df.write
        .format("delta")
        .option("txnAppId", "stream_join_job")
        .option("txnVersion", batch_id)
        .mode("append")
        .saveAsTable("joined_events")
    )
).start()
```

### 2. Handling Late Data Gracefully

```python
# Separate late data to a different table
def write_with_late_data_handling(df, batch_id):
    # On-time data
    on_time = df.filter(col("processing_delay") < "10 minutes")
    on_time.write.format("delta").mode("append").save("/delta/on_time")
    
    # Late data for manual review
    late = df.filter(col("processing_delay") >= "10 minutes")
    late.write.format("delta").mode("append").save("/delta/late_data")

stream.writeStream.foreachBatch(write_with_late_data_handling).start()
```

### 3. Multi-Stream Joins (3+ Streams)

```python
# Chain joins carefully - each adds state overhead
step1 = (stream1
    .withWatermark("ts", "10 min")
    .join(
        stream2.withWatermark("ts", "10 min"),
        expr("s1_id = s2_id AND ts_diff < 5 min"),
        "inner"
    )
)

# Result has watermark from left side (stream1)
final = step1.join(
    stream3.withWatermark("ts", "10 min"),
    expr("s1_id = s3_id AND ts_diff < 5 min"),
    "inner"
)
```

## Common Pitfalls

### Pitfall 1: Missing Watermarks

```python
# WRONG: Joining streams without watermarks
joined = stream1.join(stream2, "key")  # State grows forever!

# CORRECT: Always define watermarks
joined = (stream1
    .withWatermark("ts", "10 min")
    .join(stream2.withWatermark("ts", "10 min"), "key")
)
```

### Pitfall 2: Too-Broad Time Ranges

```python
# WRONG: Open-ended time range
expr("s2.ts >= s1.ts")  # Unbounded state!

# CORRECT: Bounded time range
expr("s2.ts BETWEEN s1.ts AND s1.ts + interval 1 hour")
```

### Pitfall 3: Different Time Zones

```python
# Ensure both streams use the same timezone
stream1 = stream1.withColumn("ts", to_utc_timestamp(col("ts"), "America/New_York"))
stream2 = stream2.withColumn("ts", to_utc_timestamp(col("ts"), "America/New_York"))
```

## Tuning Checklist

- [ ] Watermark duration matches your late data SLA
- [ ] Time range bounds are explicit and reasonable
- [ ] State store provider configured (RocksDB for large state)
- [ ] Monitoring on state size and join ratio
- [ ] Output mode is "append" (required for streaming joins)
- [ ] Checkpoint location is unique per query
- [ ] Consider broadcast hint for small stream side if applicable

## Monitoring

```python
# Key metrics to watch
query = spark.streams.active[0]
progress = query.lastProgress

print(f"Input rows/sec: {progress['inputRowsPerSecond']}")
print(f"Process rows/sec: {progress['processedRowsPerSecond']}")
print(f"State rows: {progress['stateOperators'][0]['numRowsTotal']}")
print(f"Watermark: {progress['eventTime']['watermark']}")
```
