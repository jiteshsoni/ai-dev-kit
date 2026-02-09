---
name: spark-structured-streaming
description: Comprehensive guide to Spark Structured Streaming on Databricks - checkpointing, Kafka integration, joins, state management, and production patterns.
tags: ["spark", "streaming", "kafka", "delta", "real-time", "production"]
---

# Spark Structured Streaming

> Production-ready patterns for real-time and microbatch streaming on Databricks.

## Quick Reference

| Pattern | When to Use | See |
|---------|-------------|-----|
| Kafka → Delta | Ingest streaming events to bronze layer | [Kafka Integration](docs/kafka-integration.md) |
| Stream Enrichment | Join streams with dimension tables | [Joins Patterns](docs/joins-patterns.md) |
| Deduplication | Handle duplicate events at scale | [State Management](docs/state-management.md) |
| Exactly-Once | Prevent data loss or duplication | [Checkpoint & Recovery](docs/checkpoint-recovery.md) |
| Multi-Table Output | Write to multiple sinks from one stream | [Table Operations](docs/table-operations.md) |
| Cost Optimization | Right-size clusters and triggers | [Tuning](docs/optimization-tuning.md) |

## Table of Contents

### Core Docs
- [Checkpoint & Recovery](docs/checkpoint-recovery.md) — Exactly-once semantics, checkpoint organization, failure recovery
- [State Management](docs/state-management.md) — Watermarks, state stores, deduplication, late data handling
- [Kafka Integration](docs/kafka-integration.md) — Kafka to Delta, Kafka to Kafka, Real-Time Mode (RTM)
- [Joins Patterns](docs/joins-patterns.md) — Stream-stream and stream-static joins with watermarks
- [Table Operations](docs/table-operations.md) — MERGE operations, writing to multiple tables, performance optimization
- [Optimization & Tuning](docs/optimization-tuning.md) — Cost tuning, trigger configuration, partitioning strategies

### Expert Packs
- [Multi-Sink Streaming](expert-packs/multi-sink-streaming.md) — Fan-out patterns for multiple destinations
- [Streaming Best Practices](expert-packs/streaming-best-practices.md) — Production guidelines and anti-patterns
- [DLT vs Jobs](expert-packs/dlt-vs-jobs.md) — When to use Delta Live Tables vs standard streaming jobs

## Quick Start

```python
from pyspark.sql.functions import col, from_json

# Basic Kafka to Delta streaming
df = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "topic")
    .option("startingOffsets", "earliest")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/Volumes/catalog/checkpoints/stream") \
    .trigger(processingTime="30 seconds") \
    .start("/delta/target_table")
```

## Production Checklist

- [ ] **Checkpoint**: Persistent storage (S3/ADLS), unique per stream, target-organized
- [ ] **Idempotency**: Use `txnAppId` and `txnVersion` for exactly-once in `forEachBatch`
- [ ] **State Management**: Watermark configured, state size monitored
- [ ] **Joins**: Use left joins for stream-static, watermarks for stream-stream
- [ ] **Cluster**: Fixed size (no autoscaling for streaming), adequate memory for state
- [ ] **Monitoring**: Input rate vs processing rate, lag metrics, batch duration alerts
- [ ] **Recovery**: Documented procedures, tested checkpoint restore

## Quick Patterns

### With Idempotency (forEachBatch)
```python
def upsert_to_delta(microBatchDF, batchId):
    microBatchDF.write
        .format("delta")
        .option("txnVersion", batchId)
        .option("txnAppId", "streaming_job_v1")
        .mode("append")
        .saveAsTable("target_table")

stream.writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", "/checkpoints/target")
    .start()
```

### Watermarked Aggregation
```python
(df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("user_id")
    )
    .agg(sum("amount"))
)
```

## Decision Matrix

| Requirement | Recommendation |
|-------------|----------------|
| Latency < 1 second | Use Real-Time Mode (RTM) or consider Flink |
| Exactly-once delivery | Delta sink + checkpoint + idempotent writes |
| Join with dimension table | Stream-static join with Delta table (left join) |
| Join two streams | Stream-stream join with watermarks on both sides |
| Handle late data | Configure watermark, use append output mode |
| Multiple outputs | Use `foreachBatch` or multi-sink pattern |
| Schema evolution | Enable `mergeSchema` or use Auto Loader |
| Cost optimization | `availableNow` trigger, right-sized cluster, partition pruning |

---

*Each linked doc contains detailed code examples, configuration options, and troubleshooting guidance.*
