---
name: kafka-integration
description: Complete guide for Kafka integration with Spark Structured Streaming - ingest to Delta, Kafka-to-Kafka pipelines, and real-time mode (RTM) configuration.
tags: ["kafka", "streaming", "delta", "ingestion", "real-time"]
---

# Kafka Integration

Complete patterns for integrating Kafka with Spark Structured Streaming on Databricks.

## Quick Decision Matrix

| Pattern | Latency | Use Case |
|---------|---------|----------|
| [Kafka → Delta](#kafka-to-delta) | 1-60s | Bronze layer ingestion, data lake |
| [Kafka → Kafka](#kafka-to-kafka) | <1s - 60s | Event enrichment, routing, transformation |
| [Real-Time Mode](#real-time-mode-rtm) | <800ms | Ultra-low latency requirements |

---

## Kafka to Delta

Ingest Kafka topics into Delta Lake for bronze layer storage.

### Basic Pattern

```python
from pyspark.sql.functions import col, from_json, current_timestamp

# Read from Kafka
df = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "events")
    .option("startingOffsets", "latest")
    .option("minPartitions", "6")
    .load()
)

# Parse and add metadata
df_parsed = (df
    .select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), schema).alias("data"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingestion_timestamp")
    )
    .select("key", "data.*", "topic", "partition", "offset", "kafka_timestamp", "ingestion_timestamp")
)

# Write to Delta
(df_parsed
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/catalog/checkpoints/bronze_events")
    .queryName("kafka_to_delta_bronze")
    .trigger(processingTime="30 seconds")
    .start("/delta/bronze_events")
)
```

### Scheduled Streaming (Cost-Optimized)

Use `availableNow` for periodic processing instead of continuous:

```python
# Run every 4 hours via Databricks Jobs
(df_parsed
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/catalog/checkpoints/bronze_events")
    .trigger(availableNow=True)  # Process all, then stop
    .start("/delta/bronze_events")
)
```

### Schema Evolution

```python
(df_parsed
    .writeStream
    .format("delta")
    .option("mergeSchema", "true")
    .option("checkpointLocation", "/checkpoints/bronze_events")
    .start("/delta/bronze_events")
)
```

---

## Kafka to Kafka

Read from Kafka, process, and write back to Kafka.

### Basic Pattern

```python
from pyspark.sql.functions import col, to_json, struct, current_timestamp

# Read
source = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("subscribe", "input-events")
    .option("startingOffsets", "latest")
    .load()
)

# Parse and transform
parsed = (source
    .select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), event_schema).alias("data")
    )
    .select("key", "data.*")
    .withColumn("processed_at", current_timestamp())
)

# Transform
enriched = parsed.withColumn(
    "value", 
    to_json(struct("event_id", "user_id", "event_type", "processed_at"))
)

# Write to output topic
(enriched
    .select("key", "value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("topic", "output-events")
    .option("checkpointLocation", "/checkpoints/kafka-to-kafka")
    .trigger(processingTime="30 seconds")
    .start()
)
```

### Event Enrichment (Stream-Static Join)

```python
# Dimension table (auto-refreshed each microbatch)
user_dim = spark.table("users.dimension")

# Enrich with dimension data
enriched = (parsed
    .join(user_dim, "user_id", "left")
    .withColumn("value", to_json(struct(
        col("event_id"),
        col("user_id"),
        col("user_name"),      # From dimension
        col("user_segment"),   # From dimension
        col("event_type")
    )))
)

# Write enriched events
(enriched
    .select("key", "value")
    .writeStream
    .format("kafka")
    .option("topic", "enriched-events")
    .option("checkpointLocation", "/checkpoints/enrichment")
    .start()
)
```

### Multi-Topic Routing

```python
def route_to_topics(batch_df, batch_id):
    """Route events to different topics based on criteria"""
    
    # High priority
    high = batch_df.filter(col("priority") == "high")
    if high.count() > 0:
        (high.select("key", "value")
            .write
            .format("kafka")
            .option("topic", "urgent-events")
            .save())
    
    # Standard priority
    standard = batch_df.filter(col("priority") == "standard")
    if standard.count() > 0:
        (standard.select("key", "value")
            .write
            .format("kafka")
            .option("topic", "standard-events")
            .save())

# Use foreachBatch for multi-sink
(enriched
    .writeStream
    .foreachBatch(route_to_topics)
    .option("checkpointLocation", "/checkpoints/routing")
    .start()
)
```

---

## Real-Time Mode (RTM)

Sub-second latency processing with pre-allocated resources.

### When to Use RTM

| Latency | Mode | Photon Required |
|---------|------|-----------------|
| < 800ms | RTM | Yes |
| > 800ms | Microbatch | Optional |

### Configuration

```python
# Enable RTM
query = (enriched
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("topic", "output-events")
    .trigger(realTime=True)  # Enable RTM
    .option("checkpointLocation", "/checkpoints/rtm")
    .start()
)

# Required settings
spark.conf.set("spark.databricks.photon.enabled", "true")
spark.conf.set("spark.sql.streaming.stateStore.providerClass", 
               "com.databricks.sql.streaming.state.RocksDBStateProvider")
```

### Cluster Requirements

- **Fixed-size cluster** (no autoscaling for RTM)
- **Driver**: Minimum 4 cores
- **Workers**: Fixed size with Photon enabled
- **State store**: RocksDB provider recommended

---

## Checkpoint Best Practices

### Target-Tied Organization

```python
def get_checkpoint_location(table_name):
    """Checkpoint tied to TARGET, not source"""
    return f"/Volumes/catalog/checkpoints/{table_name}"

# Why target-tied? Checkpoint already contains source info
# Benefits: Systematic organization, easy backup/restore
```

### Exactly-Once with forEachBatch

```python
def upsert_to_delta(batch_df, batch_id):
    batch_df.write
        .format("delta")
        .option("txnVersion", batch_id)
        .option("txnAppId", "kafka_stream_v1")
        .mode("append")
        .saveAsTable("target_table")

stream.writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", "/checkpoints/target")
    .start()
```

---

## Common Options

| Option | Description | Example |
|--------|-------------|---------|
| `startingOffsets` | Where to start reading | `"earliest"`, `"latest"`, `{"topic":{"0":100}}` |
| `maxOffsetsPerTrigger` | Limit batch size | `"10000"` |
| `minPartitions` | Match Kafka partitions | `"6"` |
| `failOnDataLoss` | Fail if data missing | `"false"` (for transient topics) |

---

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| High lag | Processing < input rate | Increase cluster size, optimize transformations |
| Checkpoint errors | Corrupted checkpoint | Delete checkpoint, restart with `startingOffsets` |
| Schema mismatch | Evolving schemas | Use `mergeSchema=true` or cast to schema |
| Data loss errors | Kafka retention | Set `failOnDataLoss=false` or increase retention |
