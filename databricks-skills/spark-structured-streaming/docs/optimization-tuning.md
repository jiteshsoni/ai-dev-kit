---
name: optimization-tuning
description: Tuning and optimization for Spark Structured Streaming - cost optimization, trigger tuning, partitioning strategies, and performance best practices.
tags: ["streaming", "optimization", "tuning", "performance", "cost"]
---

# Optimization & Tuning

Performance optimization, cost reduction, and resource tuning for Spark Structured Streaming.

## Quick Decision Matrix

| Optimization | When to Apply | Impact |
|-------------|---------------|--------|
| [Trigger Tuning](#trigger-tuning) | High latency or cost | Medium-High |
| [Partition Strategy](#partitioning-strategies) | Skewed data, slow processing | High |
| [Cost Optimization](#cost-optimization) | Budget constraints | High |
| [Cluster Tuning](#cluster-tuning) | Resource bottlenecks | Medium |

---

## Trigger Tuning

### Trigger Options

```python
# Fixed interval (microbatch)
.trigger(processingTime="30 seconds")

# Process all available data then stop (scheduled)
.trigger(availableNow=True)

# Real-Time Mode (sub-second latency)
.trigger(realTime=True)

# Continuous (legacy, not recommended)
# .trigger(continuous="1 second")
```

### Trigger Selection Guidelines

| SLA Requirement | Recommended Trigger |
|----------------|-------------------|
| < 800ms | `realTime=True` (RTM) |
| 1-30 seconds | `processingTime="1 second"` |
| 30 seconds - 1 hour | `processingTime="30 seconds"` |
| > 1 hour | `availableNow=True` scheduled job |

### SLA-Based Trigger Calculation

```python
# Rule of thumb: Trigger interval = SLA / 3
# Example: 1 hour SLA → 20 minute trigger

sla_minutes = 60
trigger_interval = f"{sla_minutes / 3:.0f} seconds"
# Result: "20 seconds"

stream.writeStream \
    .trigger(processingTime=trigger_interval) \
    .start()
```

### Backpressure Handling

```python
# Monitor and adapt to backpressure
for stream in spark.streams.active:
    progress = stream.lastProgress
    if progress:
        input_rate = progress.get("inputRowsPerSecond", 0)
        processing_rate = progress.get("processedRowsPerSecond", 0)
        
        # Backpressure detected
        if processing_rate < input_rate * 0.8:
            print(f"Backpressure detected: {stream.name}")
            # Optionally increase cluster resources
```

---

## Cost Optimization

### Scheduled vs Continuous

```python
# Continuous streaming (expensive)
(spark
    .readStream
    .format("kafka")
    .option("subscribe", "events")
    .load()
    .writeStream
    .format("delta")
    .trigger(processingTime="1 second")  # Runs continuously
    .start("/delta/events")
)

# Scheduled streaming (cost-effective)
(spark
    .readStream
    .format("kafka")
    .option("subscribe", "events")
    .load()
    .writeStream
    .format("delta")
    .trigger(availableNow=True)  # Process all, then stop
    .start("/delta/events")
)

# Schedule via Databricks Jobs:
# - Every 30 minutes
# - Cluster stops between runs
# - 90%+ cost savings for many workloads
```

### Batch Size Control

```python
# Control microbatch size for optimal cost/performance
(spark
    .readStream
    .format("kafka")
    .option("subscribe", "events")
    .option("maxOffsetsPerTrigger", 10000)  # Limit per batch
    .load()
    .writeStream
    .format("delta")
    .trigger(processingTime="30 seconds")
    .start("/delta/events")
)

# Larger batches = fewer microbatches = less overhead
```

### Photon Cost-Benefit Analysis

| Scenario | Photon | Rationale |
|----------|--------|-----------|
| Real-Time Mode | Required | RTM requires Photon |
| Microbatch with aggregation | Recommended | Better performance |
| Simple Kafka → Delta | Optional | Evaluate cost vs benefit |
| Budget constraints | Not recommended | Additional cost |

### Autoscaling vs Fixed Clusters

```python
# Fixed cluster (recommended for streaming)
# - Stable performance
# - No autoscaling overhead
# - Better cost predictability

# Autoscaling cluster (not recommended for streaming)
# - State management challenges
# - Performance variability
# - Higher cost for stable workloads
```

---

## Partitioning Strategies

### Source Partition Alignment

```python
# Match Kafka partitions for optimal parallelism
kafka_partitions = 6  # Your topic has 6 partitions

(spark
    .readStream
    .format("kafka")
    .option("subscribe", "events")
    .option("minPartitions", str(kafka_partitions))  # Match source partitions
    .load()
    .writeStream
    .format("delta")
    .start("/delta/events")
)
```

### Repartitioning Strategies

```python
# Repartition before stateful operations
df = (spark.readStream
    .format("kafka")
    .option("subscribe", "events")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

# Repartition by join key for stream-stream joins
df_repartitioned = df.repartition(200, "user_id")  # 200 partitions for shuffle

# Repartition for aggregation
df_for_agg = df.repartition(100, "region", "event_type")
```

### Skew Handling

```python
from pyspark.sql.functions import col, expr

# Identify skew
df.groupBy("user_id").count().orderBy(col("count").desc()).show()

# Handle skew with salting
df_with_salt = df.withColumn("salt", expr("cast(rand() * 10 as int)"))

# Aggregation with salt
(df_with_salt
    .groupBy("user_id", "salt")
    .agg(count("*").alias("count"))
    .groupBy("user_id")
    .agg(sum("count").alias("total_count"))
)
```

### Partitioning for Delta Sinks

```python
# Partition target table on frequently filtered columns
(spark
    .readStream
    .format("kafka")
    .option("subscribe", "events")
    .load()
    .writeStream
    .format("delta")
    .partitionBy("date", "region")  # Partition output table
    .option("checkpointLocation", "/checkpoints/events")
    .start("/delta/events")
)

# Benefits:
# - Faster queries on partition columns
# - Better file management
# - Efficient deletes/updates
```

---

## Cluster Tuning

### Driver Configuration

```python
# Driver settings for streaming jobs
# driver_cores: 4-8 for moderate workloads
# driver_memory: 8-16GB for state management
# driver_local_disk: 50-100GB for checkpoints
```

### Worker Configuration

```python
# Worker sizing guidelines
# Streaming jobs benefit from:
# - More smaller workers vs fewer larger ones
# - Stable cluster size (no autoscaling)
# - Adequate memory for state operations

# Example for 100MB/s stream:
# - 4 workers × 4 cores each
# - 16GB memory per worker
# - Total: 16 cores, 64GB memory
```

### Memory Management

```python
# State store memory allocation
spark.conf.set("spark.sql.streaming.stateStore.providerClass", 
               "com.databricks.sql.streaming.state.RocksDBStateProvider")

# Enable off-heap memory for large state
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "2g")

# Serialization for large objects
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

### Parallelism Settings

```python
# Shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")  # Default: 200

# Streaming partitions
spark.conf.set("spark.sql.streaming.stateStore.numPartitions", "32")

# Adaptive Query Execution (AQE)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

---

## Performance Best Practices

### Code Optimization

```python
# 1. Avoid unnecessary transformations in streaming
# BAD: Multiple select transformations
df.select("col1").select("col2").select("col3")

# GOOD: Single select
df.select("col1", "col2", "col3")

# 2. Use built-in functions vs UDFs
# BAD: Python UDF
from pyspark.sql.functions import udf
udf_transform = udf(lambda x: x.upper())

# GOOD: Built-in function
from pyspark.sql.functions import upper

# 3. Cache intermediate results if reused
dim_table = spark.table("dim_users").cache()
```

### State Store Optimization

```python
# Use RocksDB for large state
spark.conf.set("spark.sql.streaming.stateStore.providerClass", 
               "com.databricks.sql.streaming.state.RocksDBStateProvider")

# Configure RocksDB
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.compression", "lz4")
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.thread.num", "4")
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.ttl.seconds", "3600")
```

### Monitoring & Metrics

```python
# Key metrics to track
metrics_to_monitor = [
    "inputRowsPerSecond",
    "processedRowsPerSecond",
    "numInputRows",
    "batchDuration",
    "stateOperators[0].numRowsTotal",  # State size
    "stateOperators[0].memoryUsedBytes",
    "sources[0].metrics.latestOffset",
    "sources[0].metrics.maxOffsetsBehindLatest"
]

for stream in spark.streams.active:
    progress = stream.lastProgress
    if progress:
        for metric in metrics_to_monitor:
            value = progress.get(metric, "N/A")
            print(f"{metric}: {value}")
```

---

## Cost-Saving Patterns

### Pattern 1: Scheduled Streaming

```python
# Instead of continuous streaming, schedule runs
# Databricks Job schedule: Every 30 minutes
# Cluster stops between runs

(spark
    .readStream
    .format("kafka")
    .option("subscribe", "events")
    .load()
    .writeStream
    .format("delta")
    .trigger(availableNow=True)  # Process all available data
    .option("checkpointLocation", "/checkpoints/events")
    .start("/delta/events")
)
```

### Pattern 2: Cold Storage Archiving

```python
def archive_cold_data(batch_df, batch_id):
    """Move old data to cheaper storage"""
    
    # Recent data to hot storage
    recent = batch_df.filter(col("timestamp") >= current_date() - 7)
    (recent
        .write
        .mode("append")
        .format("delta")
        .saveAsTable("events_hot"))
    
    # Old data to cold storage
    old = batch_df.filter(col("timestamp") < current_date() - 7)
    (old
        .write
        .mode("append")
        .format("parquet")  # Cheaper than Delta
        .save("/cold_storage/events/"))
```

### Pattern 3: Adaptive Batching

```python
def adaptive_batch_size(batch_df, batch_id):
    """Adjust batch size based on load"""
    
    current_time = datetime.now().hour
    
    if 9 <= current_time <= 17:  # Business hours
        # Small batches for low latency
        batch_size = 1000
    else:
        # Large batches for cost efficiency
        batch_size = 10000
    
    # Process with appropriate batch size
    # ... implementation
```

---

## Troubleshooting Performance Issues

| Issue | Indicators | Solutions |
|-------|------------|-----------|
| High latency | Batch duration > trigger interval | Increase cluster size, optimize code, reduce batch size |
| Backpressure | Processing rate < input rate | Scale up, tune triggers, improve transformations |
| State explosion | OOM errors, slow processing | Increase watermark, use RocksDB, partition state |
| Skewed processing | Uneven task times | Repartition, salt keys, adaptive queries |
| Checkpoint lag | Offsets behind latest | Tune batch size, optimize sink writes |