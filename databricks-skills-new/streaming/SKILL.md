---
name: streaming
description: "Spark Structured Streaming patterns for production: Kafka integration, joins, checkpointing, state management, and real-time pipelines."
tags: ["spark-streaming", "kafka", "real-time", "stateful-processing", "production"]
---

# Spark Structured Streaming

Comprehensive guide to building production streaming pipelines with Spark Structured Streaming on Databricks.

## Overview

Spark Structured Streaming provides a unified API for batch and streaming workloads. This guide covers production patterns from basic Kafka ingestion to advanced stateful processing.

**Key Concepts:**
- **Microbatch Processing**: Default mode, processes data in small batches
- **Real-Time Mode (RTM)**: Sub-second latency for time-critical applications
- **Exactly-Once Semantics**: Guaranteed processing without duplicates
- **Stateful Operations**: Aggregations, joins, and deduplication across batches

## Quick Start

### Basic Kafka to Delta Pipeline

```python
from pyspark.sql.functions import *

# Read from Kafka
df = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "events")
    .option("startingOffsets", "latest")
    .load()
)

# Parse and write to Delta
query = (df
    .select(
        col("key").cast("string"),
        col("value").cast("string"),
        col("timestamp")
    )
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/catalog/checkpoints/events")
    .trigger(processingTime="30 seconds")
    .start("/Volumes/catalog/tables/events")
)
```

### Key Configuration

```python
# Required for exactly-once
checkpoint_location = "/Volumes/catalog/checkpoints/{table_name}"

# Trigger options
.trigger(processingTime="30 seconds")  # Fixed interval
.trigger(availableNow=True)            # Process all, then stop
.trigger(realTime=True)                # Sub-second latency
```

## Common Patterns

### Pattern 1: Stream-Static Join (Dimension Enrichment)

Enrich streaming data with slowly-changing dimension tables.

```python
# Stream of events
events = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "user_events")
    .load()
    .select(from_json(col("value").cast("string"), event_schema).alias("e"))
)

# Static dimension (Delta table - refreshed each microbatch)
users = spark.table("dimension.users")  # Delta table auto-refreshes

# Left join recommended for production
enriched = events.join(users, "user_id", "left")

(enriched
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/enriched_events")
    .start("/tables/enriched_events")
)
```

**Why Left Join?** Inner join drops events if dimension lookup fails. Left join preserves all events with null for missing dimensions.

### Pattern 2: Stream-Stream Join (Event Correlation)

Join two unbounded streaming sources with time-windowed semantics.

```python
# Impressions stream
impressions = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "impressions")
    .load()
    .select(from_json(col("value").cast("string"), impression_schema).alias("i"))
    .withWatermark("impression_time", "10 minutes")
)

# Clicks stream
clicks = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "clicks")
    .load()
    .select(from_json(col("value").cast("string"), click_schema).alias("c"))
    .withWatermark("click_time", "10 minutes")
)

# Join with time bounds
attribution = impressions.join(
    clicks,
    expr("""
        i.ad_id = c.ad_id AND
        c.click_time BETWEEN i.impression_time AND 
                           i.impression_time + interval 1 hour
    """),
    "leftOuter"
)
```

**Key Requirements:**
- Watermarks required on both sides
- Time range must be bounded (not open-ended)
- State grows with window size

### Pattern 3: Stateful Deduplication

Remove duplicate events using state store.

```python
# Deduplicate by event_id with 10-minute watermark
 deduped = (df
     .withWatermark("event_time", "10 minutes")
     .dropDuplicates(["event_id"])
 )

# Or with additional grouping
 deduped = (df
     .withWatermark("event_time", "10 minutes")
     .dropDuplicates(["user_id", "event_id"])
 )
```

**How It Works:**
- State stores seen keys with timestamp
- Keys expire after watermark duration
- Newer duplicates within window are dropped

### Pattern 4: ForEachBatch for Multi-Table Writes

Write to multiple destinations in a single transaction.

```python
def upsert_to_multiple_tables(microbatch_df, batch_id):
    """Write to silver and gold in one batch"""
    
    # Write to silver (bronze → silver transformation)
    microbatch_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("silver.events")
    
    # Aggregate for gold
    aggregated = microbatch_df.groupBy("category").count()
    aggregated.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("gold.event_summary")

# Use ForEachBatch
(df.writeStream
    .foreachBatch(upsert_to_multiple_tables)
    .option("checkpointLocation", "/checkpoints/multi_sink")
    .start()
)
```

### Pattern 5: Kafka-to-Kafka with Real-Time Mode

Low-latency event processing between Kafka topics.

```python
# Read from source
source = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "input-events")
    .load()
)

# Transform
enriched = source.select(
    col("key"),
    to_json(struct(col("value"), current_timestamp())).alias("value")
)

# Write to output with RTM for sub-second latency
(enriched
    .writeStream
    .format("kafka")
    .option("topic", "output-events")
    .trigger(realTime=True)  # Enable Real-Time Mode
    .option("checkpointLocation", "/checkpoints/kafka-pipeline")
    .start()
)
```

**RTM Requirements:**
- Photon enabled
- Fixed-size cluster (no autoscaling)
- Higher resource usage than microbatch

## Checkpoint Deep Dive

### Checkpoint Structure

```
checkpoint_location/
├── metadata/          # Stream query ID
├── offsets/           # Intent: what to process
│   ├── 0              # Offset for batch 0
│   ├── 1
│   └── ...
├── commits/           # Confirmation: what completed
│   ├── 0
│   ├── 1
│   └── ...
├── sources/           # Source metadata
└── state/             # Stateful operation state
```

### Offset Semantics

```json
{
  "batchWatermarkMs": 0,
  "batchTimestampMs": 1234567890,
  "conf": [...],
  "source": [{
    "startOffset": {"topic": {"0": 100}},  // Inclusive
    "endOffset": {"topic": {"0": 200}},    // Exclusive
    "latestOffset": {"topic": {"0": 500}}
  }]
}
```

- **Start Offset (inclusive)**: First message to process
- **End Offset (exclusive)**: First message NOT to process
- **Example**: start=100, end=200 → processes offsets 100-199

### Recovery Process

```python
# On restart, Spark:
# 1. Reads latest offset file (e.g., offset 223)
# 2. Checks if commit 223 exists
# 3. If yes: proceed to offset 224
# 4. If no: reprocess offset 223

# Exactly-once achieved via:
# - Delta idempotent writes (queryId + epochId)
# - Offset tracking
```

## Production Checklist

### Cluster Configuration
- [ ] Fixed-size cluster (no autoscaling for streaming)
- [ ] Sufficient cores for Kafka partitions
- [ ] Checkpoint in persistent storage (S3/ADLS, not DBFS)

### Code Quality
- [ ] Unique checkpoint per stream
- [ ] Target-tied checkpoint organization
- [ ] Watermarks for stateful operations
- [ ] Error handling in ForEachBatch
- [ ] Monitoring hooks

### Monitoring
- [ ] Input Rate vs Processing Rate
- [ ] Max Offsets Behind Latest
- [ ] Batch Duration vs Trigger Interval
- [ ] State Store Size
- [ ] Alerting for lag increase

## Original Sources

Content consolidated from:
- **Canadian Data Guy**: "Spark Streaming Master Class", "Mastering Checkpoints", "Stream-Stream Joins"
- **Databricksters**: "How Liquid Clustering Improves Streaming Merges", "S3 API Cost Optimization"
- **YouTube**: "Kafka to Delta", "Real-Time Mode", "Checkpoint Internals"
- **Expert Packs**: Stream-stream joins, stream-static joins, multi-sink patterns, deduplication at scale
