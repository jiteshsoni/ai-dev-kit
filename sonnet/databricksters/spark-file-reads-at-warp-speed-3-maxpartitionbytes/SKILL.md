---
name: "Spark File Read Optimization with maxPartitionBytes"
description: "Optimize Spark file reading performance by tuning spark.sql.files.maxPartitionBytes for small files, large files, and mixed file sizes."
author: "Geethu"
url: "https://www.databricksters.com/p/spark-file-reads-at-warp-speed-3"
date: "2025-11-28"
tags: ["spark", "performance", "file-reads", "partitioning", "streaming", "databricks"]
---

# Spark File Read Optimization with maxPartitionBytes

## Overview

The `spark.sql.files.maxPartitionBytes` parameter controls partition size when reading files and directly impacts parallelism and performance. Proper tuning can deliver up to 15x speedup for file reading operations. The optimal value depends on your file size distribution: small files (<2MB), large files (~1GB), or mixed sizes.

**Use this skill when:** Reading files in Spark/PySpark, optimizing streaming ingestion, or experiencing slow file reads despite sufficient cluster resources.

## Quick Start

Basic tuning for common scenarios:

```python
# Scenario 1: Many small files (streaming jobs)
spark.conf.set("spark.sql.files.maxPartitionBytes", "4mb")  # 1-16MB range

# Scenario 2: Large files (batch processing)
spark.conf.set("spark.sql.files.maxPartitionBytes", "128mb")  # Default, usually good

# Scenario 3: Mixed file sizes
spark.conf.set("spark.sql.files.maxPartitionBytes", "32mb")  # Start lower, adjust

# Read data and observe parallelism
df = spark.read.format("parquet").load("s3://bucket/data/")
print(f"Partitions created: {df.rdd.getNumPartitions()}")
```

## Common Patterns

### Pattern 1: Small Files in Streaming Jobs

When processing many small files, decreasing `maxPartitionBytes` increases parallelism and prevents core idling.

```python
# ❌ BAD: Default 128MB groups too many small files per partition
# Result: Some cores process large partitions while others idle
spark.conf.set("spark.sql.files.maxPartitionBytes", "128mb")
df = spark.readStream \
  .format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  .load("s3://bucket/small-files/")

# ✅ GOOD: Lower value creates more partitions = better parallelism
spark.conf.set("spark.sql.files.maxPartitionBytes", "4mb")
df = spark.readStream \
  .format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  .option("cloudFiles.maxFilesPerTrigger", 1000) \
  .load("s3://bucket/small-files/")

# Monitor partition distribution
df.writeStream \
  .foreachBatch(lambda batch_df, batch_id: 
    print(f"Batch {batch_id}: {batch_df.rdd.getNumPartitions()} partitions")
  ) \
  .start()
```

**Recommendation:** For small files (<2MB), set between **1MB - 16MB**. Experiment to find optimal value.

### Pattern 2: Large Files in Batch Processing

Large files benefit from splitting into optimal-sized partitions.

```python
# Large files: 1GB each
# Default 128MB creates ~8 partitions per file (1024/128 = 8)

spark.conf.set("spark.sql.files.maxPartitionBytes", "128mb")

# Read large Parquet files
df = spark.read.parquet("s3://bucket/large-files/")

# Verify partition count
expected_partitions = total_file_size_gb * 1024 / 128
print(f"Expected partitions: ~{expected_partitions}")
print(f"Actual partitions: {df.rdd.getNumPartitions()}")

# Check partition sizes
from pyspark.sql.functions import spark_partition_id, count
df.groupBy(spark_partition_id()).agg(count("*").alias("rows_per_partition")).show()
```

**Recommendation:** For large files (>100MB), **default 128MB** provides good performance. Adjust only if seeing skew or inefficiency.

### Pattern 3: Mixed File Sizes

When file sizes vary significantly (1MB to 1GB), finding the right balance is critical.

```python
# Start with lower value to handle small files
spark.conf.set("spark.sql.files.maxPartitionBytes", "32mb")

df = spark.read.parquet("s3://bucket/mixed-files/")

# Monitor and adjust based on execution
# Use Spark UI to check:
# - Task execution times
# - Data skew across tasks
# - Shuffle read/write bytes

# If seeing issues:
# - Too many small partitions → increase value
# - Data skew / long tasks → decrease value

# Iterative tuning example
for mb_size in [16, 32, 64, 128]:
    spark.conf.set("spark.sql.files.maxPartitionBytes", f"{mb_size}mb")
    df = spark.read.parquet("s3://bucket/mixed-files/")
    
    # Time the operation
    start = time.time()
    df.count()
    duration = time.time() - start
    
    print(f"Size: {mb_size}MB, Partitions: {df.rdd.getNumPartitions()}, Duration: {duration}s")
```

**Recommendation:** Start with **32MB** and monitor Spark UI. Adjust based on task execution patterns.

### Pattern 4: Handling Non-Splittable Compression

Some compression formats (gzip) create non-splittable files, limiting parallelism.

```python
# ❌ AVOID: gzip is non-splittable
# A 1GB gzip file = 1 partition regardless of maxPartitionBytes
df = spark.read.json("s3://bucket/data.json.gz")  # Only 1 task!

# ✅ GOOD: Use splittable compression
# Option 1: bzip2 (splittable but slower)
df = spark.read.json("s3://bucket/data.json.bz2")

# Option 2: snappy/lzo with Parquet (columnar + splittable)
df = spark.read.parquet("s3://bucket/data.parquet")

# Option 3: Pre-split large files before compression
# Split 10GB file into 100MB chunks:
# split -b 100M large_file.json small_chunk_

# Then compress individually (even with gzip, each file can be a partition)
```

**Best practices:**
- ❌ **Avoid gzip** for large files in Spark
- ✅ **Use bzip2, lzo, or snappy** for splittable compression
- ✅ **Use Parquet/ORC** for columnar storage with efficient compression
- ⚡ **Pre-split large files** if stuck with non-splittable formats

### Pattern 5: Streaming with Auto Loader

Auto Loader benefits from tuned `maxPartitionBytes` with small files.

```python
# Optimize Auto Loader for small files
spark.conf.set("spark.sql.files.maxPartitionBytes", "8mb")

df = spark.readStream \
  .format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  .option("cloudFiles.maxFilesPerTrigger", 1000) \
  .option("cloudFiles.schemaLocation", "/schemas/") \
  .load("s3://bucket/streaming-data/") \
  .writeStream \
  .format("delta") \
  .option("checkpointLocation", "/checkpoints/") \
  .trigger(processingTime="30 seconds") \
  .table("bronze_table")

# Monitor micro-batch performance
# Spark UI > Streaming tab > Check "Processing Time" per batch
```

## Reference Files

- [Spark Configuration Guide](https://spark.apache.org/docs/latest/configuration.html)
- [Auto Loader Best Practices](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
- [Spark Partitioning Documentation](https://spark.apache.org/docs/latest/sql-performance-tuning.html)

## Common Issues

| Issue | Solution |
|-------|----------|
| **Cores idling with small files** | Decrease `maxPartitionBytes` to 1-16MB to create more partitions. |
| **Too many small partitions** | Increase `maxPartitionBytes` or use `coalesce()` after reading. |
| **Gzip files not parallelizing** | Switch to bzip2/snappy or pre-split files before compression. |
| **Data skew in reads** | Adjust `maxPartitionBytes` or repartition after reading. |
| **Streaming batches too slow** | Lower `maxPartitionBytes` and increase `maxFilesPerTrigger`. |
| **Memory pressure** | Increase `maxPartitionBytes` to reduce partition count. |

## Advanced Tips

### Calculating Optimal Value

```python
import os

# Calculate average file size
def get_avg_file_size(path):
    dbutils = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    )
    
    files = dbutils.listStatus(spark._jvm.org.apache.hadoop.fs.Path(path))
    total_size = sum(f.getLen() for f in files)
    avg_size = total_size / len(files)
    
    return avg_size / 1024 / 1024  # MB

avg_mb = get_avg_file_size("s3://bucket/data/")
print(f"Average file size: {avg_mb:.2f} MB")

# Rule of thumb: maxPartitionBytes = avg_file_size * 2-4
recommended = int(avg_mb * 3)
print(f"Recommended maxPartitionBytes: {recommended}MB")
```

### Monitoring Impact

```python
# Before tuning
spark.conf.set("spark.sql.files.maxPartitionBytes", "128mb")
df1 = spark.read.parquet("s3://bucket/data/")
partitions_before = df1.rdd.getNumPartitions()

# After tuning
spark.conf.set("spark.sql.files.maxPartitionBytes", "32mb")
df2 = spark.read.parquet("s3://bucket/data/")
partitions_after = df2.rdd.getNumPartitions()

print(f"Partitions before: {partitions_before}")
print(f"Partitions after: {partitions_after}")
print(f"Parallelism increase: {partitions_after / partitions_before:.2f}x")

# Check Spark UI for actual performance improvement
```

### Adaptive Query Execution (AQE)

```python
# Note: maxPartitionBytes applies to file reads only
# AQE handles optimization AFTER initial read

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# AQE will combine small partitions post-shuffle
# But initial file read still uses maxPartitionBytes
```

## FAQ

**Q: Does this apply to Delta Lake tables?**  
A: Yes, when reading Delta tables, Spark reads Parquet files and `maxPartitionBytes` applies.

**Q: Should I always use a small value?**  
A: No. Too small creates overhead. Match to your file size distribution.

**Q: Does this affect shuffles?**  
A: No. This only affects file reading. Shuffles use `spark.sql.shuffle.partitions`.

**Q: How do I know if my value is optimal?**  
A: Check Spark UI:
- Task times should be similar (no skew)
- All cores should be utilized
- Individual tasks should take 30s-2min

**Q: Can I set this per-query?**  
A: Yes, set the config before each read operation. It takes effect immediately.
