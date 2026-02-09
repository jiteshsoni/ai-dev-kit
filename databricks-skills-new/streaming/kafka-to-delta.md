---
name: kafka-to-delta
description: "Complete guide to streaming ingestion from Kafka to Delta Lake with checkpoint internals and production patterns."
author: "Canadian Data Guy"
source: "https://www.youtube.com/watch?v=snJs2DlzA0o"
tags: ["spark-streaming", "kafka", "delta", "checkpoint", "ingestion"]
---

# Kafka to Delta Streaming

## Overview

Stream data from Kafka to Delta Lake using Spark Structured Streaming. This pattern provides exactly-once semantics with automatic recovery and deduplication.

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
parsed = df.select(
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
query = (parsed
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/catalog/volume/checkpoints/kafka_stream")
    .queryName("kafka_to_delta_bronze")
    .trigger(processingTime="30 seconds")
    .start("/path/to/target_table")
)
```

## Common Patterns

### Pattern 1: Bronze Layer (Raw Ingestion)

```python
# Best practice: Minimal transformation, preserve original
# Why: Kafka retention is expensive; Delta provides permanent storage

bronze_stream = (spark
    .readStream
    .format("kafka")
    .option("subscribe", topic)
    .option("maxOffsetsPerTrigger", 10000)
    .load()
)

bronze_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", bronze_checkpoint) \
    .trigger(availableNow=True) \
    .start(bronze_table)
```

### Pattern 2: Scheduled Streaming (Cost-Optimized)

```python
# Run every 4 hours instead of continuously
.writeStream \
    .trigger(availableNow=True) \
    .start(table)

# In Databricks Jobs:
# - Schedule: Every 4 hours
# - Cluster: Fixed size (no autoscaling for streaming)
```

### Pattern 3: Monitor Stream Health

```python
# Key metrics to watch:
# 1. Input Rate vs Processing Rate (processing > input)
# 2. Max Offsets Behind Latest (should decrease)

for stream in spark.streams.active:
    print(f"Stream: {stream.name}, Status: {stream.status}")
```

## Kafka Configuration

| Option | Description | Recommendation |
|--------|-------------|----------------|
| startingOffsets | Where to begin | "earliest" for backfill, "latest" for new data |
| minPartitions | Spark parallelism | Match Kafka partition count |
| maxOffsetsPerTrigger | Batch size | 10,000-100,000 for balance |
| subscribe | Topic list | Single topic or comma-separated |

## Common Issues

| Issue | Solution |
|-------|----------|
| No data being read | Check `startingOffsets` - default is "latest" |
| Falling behind | Increase cluster size or reduce `maxOffsetsPerTrigger` |
| Small files problem | Increase trigger interval |
| Duplicate data | Checkpoint handles this - don't delete checkpoints |
| Can't use autoscaling | Streaming needs fixed-size clusters |
