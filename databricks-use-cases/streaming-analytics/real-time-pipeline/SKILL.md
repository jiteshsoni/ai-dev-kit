---
name: real-time-analytics-pipeline
description: Build production-grade real-time analytics pipelines on Databricks using Structured Streaming. Covers sub-second latency with Real-Time Mode, custom streaming sources, stream-static joins, and operational best practices.
author: Canadian Data Guy, Artem Chebotko, Veena Ramesh
type: use-case-collection
source_blogs:
  - title: "Unlocking Sub-Second Latency with Databricks"
    url: https://www.canadiandataguy.com/p/unlocking-sub-second-latency-with
    author: Canadian Data Guy
  - title: "Stop Waiting for Connectors: Stream ANYTHING into Spark"
    url: https://www.canadiandataguy.com/p/stop-waiting-for-connectors-stream
    author: Canadian Data Guy
  - title: "4 Surprising Truths That Will Change How You Think About Spark Streaming"
    url: https://www.canadiandataguy.com/p/4-surprising-truths-that-will-change
    author: Canadian Data Guy
  - title: "Mastering Stream-Static Joins in Apache Spark"
    url: https://www.databricksters.com/p/mastering-stream-static-joins-in
    author: Artem Chebotko
---

# Real-Time Analytics Pipeline on Databricks

## Overview

This use case demonstrates how to build production-grade real-time analytics pipelines using Databricks Structured Streaming. By combining multiple techniques and best practices from industry experts, you'll learn to create pipelines that deliver sub-second latency while maintaining operational simplicity.

**Business Value:**
- Fraud detection with immediate alerting
- Real-time IoT sensor monitoring
- Live operational dashboards
- Instant data quality quarantine
- Personalized real-time offers

**Technical Approach:**
This guide synthesizes battle-tested patterns from production deployments across ad tech, fintech, and IoT industries.

## Architecture Components

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Data Sources  │────▶│  Spark Streaming │────▶│  Delta Tables   │
│  (Kafka/Custom) │     │  - Real-Time Mode│     │  (Bronze/Silver)│
└─────────────────┘     │  - Custom Sources│     └─────────────────┘
                        │  - Stream-Static │              │
                        │     Joins        │              ▼
                        └──────────────────┘     ┌─────────────────┐
                                                   │  Analytics/BI   │
                                                   │  - Dashboards   │
                                                   │  - ML Inference │
                                                   └─────────────────┘
```

## Step 1: Custom Streaming Source (If No Connector Exists)

When your data source doesn't have a built-in Spark connector, implement a custom streaming source using just 5 methods:

### Core Implementation Pattern

```python
from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType, StructField, StringType, LongType

class CustomStreamingSource(DataSource):
    """Custom streaming source for proprietary data sources."""
    
    @classmethod
    def name(cls) -> str:
        return "custom-stream"
    
    def schema(self) -> str:
        return "timestamp TIMESTAMP, value STRING, metadata MAP<STRING,STRING>"
    
    def reader(self, schema: StructType) -> DataSourceReader:
        return CustomStreamingReader(schema, self.options)

class CustomStreamingReader(DataSourceReader):
    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options
        self.api_endpoint = options.get("endpoint")
        self.auth_token = options.get("token")
    
    def read(self, partition):
        # Fetch data for this partition
        # Must be DETERMINISTIC - same input = same output
        for record in self._fetch_partition_data(partition):
            yield record
```

### The 5 Methods to Implement

| Method | Purpose | Called When |
|--------|---------|-------------|
| `initialOffset()` | Starting position | Once per query |
| `latestOffset()` | Current end position | Every batch |
| `partitions()` | Break work into chunks | Batch planning |
| `read()` | Fetch actual data | On executors |
| `commit()` | Cleanup after success | Post-batch |

**Example: Custom API Source**

```python
def initialOffset(self) -> dict:
    """Return starting position for new queries."""
    start_time = self.options.get("start_time", "2024-01-01T00:00:00Z")
    return {"timestamp": start_time}

def latestOffset(self) -> dict:
    """Check source for latest available data."""
    response = requests.get(f"{self.api_endpoint}/latest")
    return {"timestamp": response.json()["max_timestamp"]}

def partitions(self, start: dict, end: dict) -> list:
    """Split work into parallel chunks."""
    num_partitions = int(self.spark.conf.get("spark.sql.shuffle.partitions", "4"))
    # Divide time range into chunks
    partitions = []
    # ... partition logic ...
    return partitions

def read(self, partition):
    """Fetch data for assigned partition (runs on executors)."""
    # Must be deterministic for fault tolerance
    for record in self._api_call(partition):
        yield Row(**record)
```

**Critical Requirements:**
- `read()` must be **deterministic** - same partition = same output
- Use `[start, end)` exclusive ranges to prevent duplicates
- Implement retry logic for transient failures

## Step 2: Real-Time Mode for Sub-Second Latency

For operational workloads requiring immediate response (fraud detection, IoT alerts), use Real-Time Mode (DBR 16.4+):

```python
from pyspark.sql import functions as F

# Configure for low latency
spark.conf.set("spark.sql.shuffle.partitions", "8")

# Read from streaming source
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "events")
    .option("startingOffsets", "latest")
    .load()
)

# Parse and apply business rules
parsed_df = df_raw.select(
    F.col("timestamp").alias("event_time"),
    F.from_json(F.col("value").cast("string"), event_schema).alias("data")
).select("event_time", "data.*")

# Real-time guardrails
df_enriched = parsed_df.withColumn(
    "decision",
    F.when(
        (F.col("amount") > 10000) | 
        (F.col("risk_score") > 0.8),
        F.lit("QUARANTINE")
    ).otherwise(F.lit("ALLOW"))
).withColumn(
    "reasons",
    F.when(F.col("amount") > 10000, F.array(F.lit("HIGH_AMOUNT")))
    .when(F.col("risk_score") > 0.8, F.array(F.lit("HIGH_RISK")))
    .otherwise(F.array())
)

# Write with Real-Time Mode trigger
query = (
    df_enriched.writeStream
    .format("delta")
    .outputMode("update")  # Required for RTM
    .option("checkpointLocation", "/mnt/checkpoints/realtime")
    .trigger(realTime="1 minute")  # Real-Time Mode
    .table("realtime_decisions")
)
```

**Real-Time Mode Requirements:**
- Databricks Runtime 16.4 LTS or higher
- Output mode must be `update`
- Enable Real-Time Mode flag in cluster config
- Use small shuffle partitions (8-16)

**Latency Expectations:**
| Mode | Typical Latency | Use Case |
|------|-----------------|----------|
| Micro-batch | 1-10 seconds | Standard ETL |
| Real-Time | 20-300 ms | Fraud detection |
| Trigger.Once | Minutes | Batch-style |

## Step 3: Stream-Static Joins for Enrichment

Enrich streaming data with slowly-changing reference data using stateless joins:

```python
# Static dimension table (automatically refreshed)
static_dim_df = spark.read.table("dim_customers")

# Streaming events
stream_df = spark.readStream.table("bronze_events")

# Stream-static join (stateless, low latency)
enriched_df = (
    stream_df
    .join(
        static_dim_df,
        stream_df.customer_id == static_dim_df.customer_id,
        "left"
    )
    .select(
        stream_df["*"],
        static_dim_df.customer_segment,
        static_dim_df.risk_profile
    )
)
```

**Stream-Static Join Benefits:**
- **Stateless**: No checkpoint state required
- **Low latency**: No buffering delays
- **Memory efficient**: Only current batch in memory
- **Auto-refresh**: Static side updates automatically

**Multiple Enrichment Chain:**

```python
# Chain multiple stream-static joins
fully_enriched = (
    stream_df
    .join(customer_dim_df, "customer_id", "left")
    .join(product_dim_df, "product_id", "left")
    .join(location_dim_df, "location_id", "left")
)
```

## Step 4: Operational Patterns

### Pattern 1: Real-Time Guardrails

Flag suspicious events for immediate action:

```python
def apply_guardrails(df):
    """Apply business rules for real-time quarantine."""
    return df.withColumn(
        "violation_reasons",
        F.filter(
            F.array(
                F.when(F.col("amount") > F.lit(10000), F.lit("HIGH_AMOUNT")),
                F.when(F.col("velocity_1h") > F.lit(5), F.lit("HIGH_VELOCITY")),
                F.when(F.col("geo_mismatch"), F.lit("GEO_ANOMALY")),
                F.when(F.col("device_risk") > F.lit(0.7), F.lit("DEVICE_RISK"))
            ),
            lambda x: x.isNotNull()
        )
    ).withColumn(
        "decision",
        F.when(F.size(F.col("violation_reasons")) > 0, "QUARANTINE")
        .otherwise("ALLOW")
    )
```

### Pattern 2: Multi-Output Processing

Route events to different destinations:

```python
# Split stream by decision
quarantine_df = enriched_df.filter(F.col("decision") == "QUARANTINE")
allow_df = enriched_df.filter(F.col("decision") == "ALLOW")

# Write to separate tables
quarantine_query = (
    quarantine_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoints/quarantine")
    .table("quarantine_events")
)

allow_query = (
    allow_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoints/allowed")
    .table("allowed_events")
)
```

### Pattern 3: Stateful Aggregations (with Watermarking)

For windowed aggregations with late data handling:

```python
from pyspark.sql.functions import window

windowed_df = (
    stream_df
    .withWatermark("event_time", "10 minutes")  # Late data threshold
    .groupBy(
        window("event_time", "5 minutes"),  # Tumbling window
        "customer_id"
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("transaction_count")
    )
)
```

## Step 5: Production Checklist

### Pre-Deployment

- [ ] Enable checkpoint location on reliable storage
- [ ] Configure appropriate shuffle partitions (2-4x cluster cores)
- [ ] Set watermark for stateful operations
- [ ] Test failure recovery scenarios
- [ ] Validate exactly-once semantics

### Monitoring

```sql
-- Check streaming query status
SELECT *
FROM system.streaming.active_queries;

-- View checkpoint information
SELECT *
FROM system.storage.checkpoint_info;

-- Monitor input vs processing rate
SELECT 
    query_name,
    input_rows_per_second,
    processing_rate
FROM system.streaming.metrics;
```

### Common Pitfalls

| Issue | Cause | Solution |
|-------|-------|----------|
| High latency | Too many shuffle partitions | Reduce to 8-16 |
| Checkpoint failures | Storage issues | Use UC Volumes |
| Data duplication | Non-deterministic read() | Ensure deterministic reads |
| Memory errors | Large state stores | Add watermarks, reduce state |
| Late data dropped | Watermark too aggressive | Increase watermark delay |

## Complete Example: Fraud Detection Pipeline

```python
from pyspark.sql import functions as F
from pyspark.sql.types import *

# 1. Configuration
spark.conf.set("spark.sql.shuffle.partitions", "8")

# 2. Read from Kafka
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "latest")
    .load()
)

# 3. Parse events
event_schema = StructType([
    StructField("txn_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("merchant_id", StringType()),
    StructField("timestamp", TimestampType())
])

parsed_df = (
    raw_df
    .select(F.from_json(F.col("value").cast("string"), event_schema).alias("data"))
    .select("data.*")
)

# 4. Enrich with customer data
customer_dim = spark.read.table("dim_customers")
enriched_df = parsed_df.join(customer_dim, "customer_id", "left")

# 5. Apply fraud rules
fraud_scored = enriched_df.withColumn(
    "risk_score",
    F.when(F.col("amount") > 10000, 0.9)
    .when(F.col("customer_segment") == "NEW", 0.3)
    .otherwise(0.1)
).withColumn(
    "decision",
    F.when(F.col("risk_score") > 0.7, "BLOCK")
    .when(F.col("risk_score") > 0.4, "REVIEW")
    .otherwise("APPROVE")
)

# 6. Write with Real-Time Mode
query = (
    fraud_scored.writeStream
    .format("delta")
    .outputMode("update")
    .option("checkpointLocation", "/mnt/checkpoints/fraud_detection")
    .trigger(realTime="30 seconds")
    .table("fraud_decisions")
)

query.awaitTermination()
```

## Related Skills

- [delta-streaming-exactly-once](../databricks-skills/delta-streaming-exactly-once/SKILL.md) - Checkpoint mechanics and exactly-once semantics
- [spark-declarative-pipelines](../databricks-skills/spark-declarative-pipelines/SKILL.md) - DLT patterns for streaming
- [liquid-clustering-guide](../databricks-skills/liquid-clustering-guide/SKILL.md) - Optimize table layout for streaming queries

## Attribution

This use case synthesizes content from multiple expert sources:

1. **Canadian Data Guy** - Real-Time Mode patterns, custom streaming sources, and operational best practices
2. **Artem Chebotko** - Stream-static join patterns and IoT enrichment scenarios
3. **Databricks Community** - Production deployment patterns and monitoring approaches

## Best Practices Summary

1. **Design for maintainability** - One engine (Spark), one API for batch and streaming
2. **Use Real-Time Mode strategically** - Only when sub-second latency is required
3. **Implement custom sources carefully** - Ensure deterministic reads for fault tolerance
4. **Monitor checkpoint health** - Critical for exactly-once guarantees
5. **Choose latency based on business need** - Don't over-engineer; micro-batch is often sufficient
6. **Use stream-static joins for enrichment** - Stateless and efficient
7. **Design for checkpointing from day one** - Makes scaling to tighter SLAs easy later
