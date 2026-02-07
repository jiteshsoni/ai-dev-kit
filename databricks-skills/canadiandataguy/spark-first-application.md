---
name: spark-first-application
description: Step-by-step guide to writing your first Spark application with stream-stream joins, including working code examples. Use when learning Spark Streaming, implementing your first streaming pipeline, or understanding stream-stream join patterns with watermarks.
---

# Your First Spark Application: Stream-Stream Joins

## Overview

Build your first Spark Streaming application with stream-stream joins. This guide provides working code examples using synthetic data generation, covering inner joins, left joins, watermarks, and event-time ordering.

## Quick Start

### Setup Synthetic Streams

```python
from faker import Faker
from faker_vehicle import VehicleProvider
from pyspark.sql import functions as F
import uuid

fake = Faker()
fake.add_provider(VehicleProvider)

# Create schema
schema_name = "test_streaming_joins"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

# Generate synthetic streaming data
def generate_stream_data(num_records=1000):
    data = []
    for _ in range(num_records):
        data.append({
            "event_id": str(uuid.uuid4()),
            "user_id": fake.uuid4(),
            "event_time": fake.date_time_between(start_date="-1h", end_date="now"),
            "event_type": fake.random_element(elements=("click", "view", "purchase"))
        })
    return data
```

### Basic Stream-Stream Join

```python
from pyspark.sql.functions import col, window, expr

# Read both streams as temporary views
stream1 = spark.readStream.table("stream1_table")
stream2 = spark.readStream.table("stream2_table")

# Inner join with watermark
joined = (stream1
    .withWatermark("event_time", "10 minutes")
    .join(
        stream2.withWatermark("event_time", "10 minutes"),
        expr("""
            stream1.user_id = stream2.user_id AND
            stream2.event_time BETWEEN stream1.event_time AND
                                    stream1.event_time + interval 30 minutes
        """),
        "inner"
    )
)

# Write joined stream
query = joined.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/stream_join") \
    .trigger(processingTime="30 seconds") \
    .start("joined_events")
```

## Common Patterns

### Pattern 1: Inner Join with Watermark

```python
# Both streams must have watermarks
# Only matches when both events arrive within window

impressions = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "impressions")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withWatermark("impression_time", "10 minutes")
)

clicks = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "clicks")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withWatermark("click_time", "10 minutes")
)

# Inner join: Only matches when both arrive
attribution = impressions.join(
    clicks,
    expr("""
        impressions.ad_id = clicks.ad_id AND
        clicks.click_time BETWEEN impressions.impression_time AND
                                    impressions.impression_time + interval 1 hour
    """),
    "inner"
)
```

### Pattern 2: Left Join with Watermark

```python
# Left join: All left side events, matched right side events
# Output delayed by watermark duration

joined = (stream1
    .withWatermark("event_time", "10 minutes")
    .join(
        stream2.withWatermark("event_time", "10 minutes"),
        expr("""
            stream1.user_id = stream2.user_id AND
            stream2.event_time BETWEEN stream1.event_time AND
                                    stream1.event_time + interval 30 minutes
        """),
        "leftOuter"  # Impressions without clicks appear after watermark expires
    )
)

# Key behavior: Left side results emitted after watermark passes
# Ensures no late right-side events can match
```

### Pattern 3: Event Time Ordering

```python
# Handle out-of-order events with withEventTimeOrder
stream = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "events")
    .load()
    .withEventTimeOrder("event_time")  # Order by event time, not processing time
    .withWatermark("event_time", "10 minutes")
)

# Benefits:
# - Processes events in event-time order
# - Handles out-of-order data better
# - More accurate results
```

## Reference Files

### Stream-Stream Join Requirements

- **Watermarks required**: Both streams must have watermarks
- **Time bounds**: Join condition must include time range
- **Output mode**: Must be "append" (streaming joins don't support complete/update)
- **Stateful**: Requires state store (checkpoint has state folder)

### Watermark Configuration

```python
# Watermark duration trade-offs:
.withWatermark("event_time", "2 hours")   # High tolerance, large state
.withWatermark("event_time", "10 minutes") # Low tolerance, small state

# Rule of thumb: Max expected delay + safety margin
# If 99% of events arrive within 5 min, use 10 min watermark
```

### Join Types Supported

- **Inner join**: Both sides must match
- **Left outer**: All left side, matched right side
- **Right outer**: All right side, matched left side
- **Full outer**: All rows from both sides

## Common Issues

| Issue | Solution |
|-------|----------|
| **Missing watermarks** | Always add watermarks to both streams |
| **Unbounded state** | Use time-bounded join conditions |
| **No matches** | Check time range; events may be outside window |
| **State store growing** | Reduce watermark duration |
| **Output mode error** | Use "append" mode for streaming joins |

## Advanced Tips

### Monitoring Stream-Stream Joins

```python
# Check state store size
query = spark.streams.active[0]
progress = query.lastProgress

state_metrics = progress["stateOperators"]
for op in state_metrics:
    print(f"State rows: {op['numRowsTotal']}")
    print(f"State memory: {op['memoryUsedBytes']}")

# Monitor watermark
print(f"Watermark: {progress['eventTime']['watermark']}")
```

### Optimizing Join Performance

```python
# 1. Reduce time window if possible
# Smaller window = less state = faster

# 2. Use RocksDB for large state
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "com.databricks.sql.streaming.state.RocksDBStateProvider"
)

# 3. Monitor state partition balance
# Check for skew in state store
```

### Testing with Synthetic Data

```python
# Use rate source for testing
stream1 = (spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 100)
    .load()
    .withColumn("user_id", (rand() * 1000).cast("int"))
    .withColumn("event_time", current_timestamp())
)

# Create second stream with matching user_ids
stream2 = (spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 50)
    .load()
    .withColumn("user_id", (rand() * 1000).cast("int"))
    .withColumn("event_time", current_timestamp())
)
```

## FAQ

**Q: Do I need watermarks for stream-stream joins?**
A: Yes. Watermarks are required to limit state and handle late data.

**Q: What's the difference between inner and left join?**
A: Inner: Only matches. Left: All left side events, matched right side (delayed by watermark).

**Q: Can I use complete/update output mode?**
A: No. Streaming joins only support "append" mode.

**Q: How do I handle out-of-order events?**
A: Use `withEventTimeOrder()` to process by event time, not processing time.

**Q: What if my state store grows too large?**
A: Reduce watermark duration. State expires after watermark time passes.
