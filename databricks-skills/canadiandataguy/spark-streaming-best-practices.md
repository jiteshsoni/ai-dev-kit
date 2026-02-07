---
name: spark-streaming-best-practices
description: Comprehensive checklist of Spark Streaming best practices from beginner to advanced. Use when setting up production streaming pipelines, optimizing performance, troubleshooting streaming issues, or establishing streaming standards for your team.
---

# Spark Streaming Best Practices

## Overview

Production-ready Spark Streaming requires attention to configuration, checkpointing, partitioning, and resource management. This checklist covers foundational to advanced practices that hold true in almost all scenarios.

## Quick Start

### Essential Configuration

```python
# Always set trigger interval (controls storage listing costs)
.trigger(processingTime="5 seconds")

# Name your query (identifiable in Spark UI)
.option("queryName", "IngestFromKafka")

# Unique checkpoint per stream
.option("checkpointLocation", "/checkpoints/stream_name")

# Control partition size (100-200MB target)
.option("maxFilesPerTrigger", 100)
.option("maxBytesPerTrigger", "200MB")
```

## Common Patterns

### Pattern 1: Checkpoint Management

```python
# Each stream must have its own checkpoint
# Never share checkpoints between streams

# Good: Separate checkpoints
stream1 = df1.writeStream.option("checkpointLocation", "/checkpoints/stream1")
stream2 = df2.writeStream.option("checkpointLocation", "/checkpoints/stream2")

# Bad: Shared checkpoint (causes conflicts)
stream1 = df1.writeStream.option("checkpointLocation", "/checkpoints/shared")
stream2 = df2.writeStream.option("checkpointLocation", "/checkpoints/shared")
```

### Pattern 2: Partition Size Optimization

```python
# Target: 100-200MB partitions in memory
# Use Spark UI to monitor and adjust

# Adjust based on Spark UI metrics
df = (spark
    .readStream
    .format("delta")
    .option("maxFilesPerTrigger", 100)  # Start here
    .option("maxBytesPerTrigger", "200MB")  # Or use bytes
    .load("/path/to/source")
)

# Monitor in Spark UI:
# - Shuffle read size per partition
# - Adjust maxFilesPerTrigger/maxBytesPerTrigger accordingly
```

### Pattern 3: Auto Loader Notification Mode

```python
# Switch to notification mode for Auto Loader
# Reduces listing costs significantly

df = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.useNotifications", "true")  # Enable notifications
    .option("cloudFiles.notificationLocation", "/notifications")
    .load("/path/to/source")
)
```

## Reference Files

### Beginner Checklist

- [ ] Set trigger interval (prevents excessive listing costs)
- [ ] Use Auto Loader notification mode
- [ ] Disable S3 versioning (Delta has time travel)
- [ ] Keep compute and storage in same region
- [ ] Use ADLS Gen2 on Azure (not blob storage)
- [ ] Partition on low-cardinality columns (date, region, country)
- [ ] Name streaming queries
- [ ] Unique checkpoint per stream
- [ ] Don't run multiple streams on same driver (benchmark first)
- [ ] Target partition size: 100-200MB
- [ ] Prefer broadcast hash join when possible

### Advanced Checklist

- [ ] Establish checkpoint naming convention
- [ ] Minimize shuffle spill (target: zero)
- [ ] Use RocksDB for stateful transformations
- [ ] Prefer Azure Event Hub with Kafka connector
- [ ] Always set watermark for stateful operations
- [ ] Use Delta merge for deduplication (not dropDuplicates)
- [ ] Choose instance family based on workload:
  - F-series: Map-heavy (parsing, JSON)
  - Fsv2-series: Multiple streams or spill space needed
  - DS_v2-series: Joins, aggregations, Delta optimize
  - L-series: Delta caching (direct attached SSD)
- [ ] Set shuffle partitions = total worker cores

### Checkpoint Naming Convention

```python
# Include target table and starting timestamp/version
checkpoint_path = f"{table_location}/_checkpoints/_{target_table}_startingVersion_{version}"

# Example:
# /delta/bronze/_checkpoints/_orders_bronze_startingVersion_12345
```

## Common Issues

| Issue | Solution |
|-------|----------|
| **High storage listing costs** | Set trigger interval; use Auto Loader notification mode |
| **Partition size too large** | Reduce maxFilesPerTrigger or maxBytesPerTrigger |
| **Partition size too small** | Increase maxFilesPerTrigger or maxBytesPerTrigger |
| **Shuffle spill occurring** | Increase shuffle partitions; optimize join strategy |
| **State store growing too large** | Set watermark; use Delta merge instead of dropDuplicates |
| **Multiple streams on same driver** | Benchmark stability; consider separate clusters |
| **Checkpoint conflicts** | Ensure unique checkpoint per stream |

## Advanced Tips

### Shuffle Partition Sizing

```python
# Rule: shuffle partitions = total worker cores
# Don't set too high (overhead)
# Don't set too low (underutilization)

total_cores = spark.sparkContext.defaultParallelism
spark.conf.set("spark.sql.shuffle.partitions", total_cores)

# Note: Must clear checkpoint if changing this setting
# Checkpoint stores this configuration
```

### State Store Management

```python
# For stateful operations, always set watermark
df = (df
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["user_id", "event_id"])
)

# For very large state (trillions of records):
# Use Delta merge approach instead of dropDuplicates
# Store state in Delta table with Z-order for fast lookups
```

### Instance Family Selection

```python
# F-series: Map-heavy workloads
# - JSON parsing
# - String transformations
# - Simple filters

# Fsv2-series: Multiple streams
# - Need spill space
# - Moderate compute needs

# DS_v2-series: Join/aggregation heavy
# - Stream-stream joins
# - Windowed aggregations
# - Delta optimize jobs

# L-series: Delta caching
# - Direct attached SSD
# - Fast local reads
```

### RocksDB Configuration

```python
# Enable RocksDB for stateful transformations
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "com.databricks.sql.streaming.state.RocksDBStateProvider"
)

# Benefits:
# - Handles large state stores
# - Spills to disk when needed
# - Better memory management
```

## FAQ

**Q: How do I know if my partition size is optimal?**
A: Check Spark UI shuffle read metrics. Target 100-200MB per partition. Adjust maxFilesPerTrigger accordingly.

**Q: Can I share checkpoints between streams?**
A: No. Each stream needs its own checkpoint. Sharing causes conflicts and data corruption.

**Q: When should I use notification mode for Auto Loader?**
A: Always, if possible. Reduces listing costs significantly. Requires notification infrastructure setup.

**Q: How many streams can run on one cluster?**
A: Benchmark first. Generally not recommended on same driver. Test for stability over days.

**Q: What if I need to change shuffle partitions?**
A: Clear checkpoint first. Checkpoint stores configuration and won't pick up changes.

**Q: When should I use RocksDB?**
A: For any stateful transformation (watermarks, deduplication, aggregations). Handles large state better than default.
