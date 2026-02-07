---
name: spark-shuffle-partitions
description: Modern approach to setting Spark shuffle partitions in 2025 with Adaptive Query Execution. Use when tuning Spark performance, understanding AQE's automatic partition management, or determining when manual partition tuning is still needed.
---

# Spark Shuffle Partitions in 2025

## Overview

With Adaptive Query Execution (AQE), manually tuning shuffle partitions is less critical than in earlier Spark versions. AQE automatically adjusts partition counts based on runtime statistics. However, understanding when and how to set shuffle partitions remains valuable for specific scenarios.

## Quick Start

### Default Behavior (2025)

```python
# Default: 200 partitions
# AQE automatically coalesces/adjusts at runtime

# For most workloads: Let AQE handle it
spark.conf.set("spark.sql.adaptive.enabled", "true")
# No need to manually set shuffle partitions
```

### When to Manually Set

```python
# Rule of thumb: shuffle partitions = total worker cores
total_cores = spark.sparkContext.defaultParallelism
spark.conf.set("spark.sql.shuffle.partitions", total_cores)

# Example: 8 workers × 4 cores = 32 partitions
spark.conf.set("spark.sql.shuffle.partitions", 32)
```

## Common Patterns

### Pattern 1: Let AQE Handle It (Recommended)

```python
# Enable AQE (default in Spark 3.2+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# AQE will:
# - Coalesce small partitions
# - Split large partitions
# - Adjust based on actual data sizes
# - No manual tuning needed
```

### Pattern 2: Calculate Based on Data Size

```python
# Important: Use in-memory size, not on-disk size
# Parquet/Avro compression: 2-8x larger in memory

# Measure actual shuffle read size in Spark UI
df = spark.read.load("path/to/data")
df.write.format("noop").mode("overwrite").save()

# Check Spark UI → SQL/DataFrame tab → Shuffle read size
# Target: 100-200MB per partition

# Calculate partitions:
# shuffle_partitions = total_data_size_mb / 200
```

### Pattern 3: Streaming-Specific Settings

```python
# For streaming: Match to worker cores
# Don't set too high (overhead)
# Don't set too low (underutilization)

# Streaming best practice:
spark.conf.set("spark.sql.shuffle.partitions", total_worker_cores)

# Note: Must clear checkpoint if changing this setting
# Checkpoint stores configuration
```

## Reference Files

### AQE Automatic Adjustments

AQE automatically:
- **Coalesces small partitions**: Merges partitions < target size
- **Splits large partitions**: Divides partitions > target size
- **Adjusts at runtime**: Based on actual data sizes, not estimates
- **Handles skew**: Splits skewed partitions automatically

### Historical Context

| Era | Approach | Reason |
|-----|----------|--------|
| **2015-2019** | Manual tuning critical | No AQE, defaults often wrong |
| **2020-2022** | AQE introduced | Automatic adjustments available |
| **2023+** | AQE default | Manual tuning rarely needed |

### Decision Tree

```
Do you have AQE enabled?
├─ Yes → Let AQE handle it (default)
└─ No → Set to worker cores
    └─ Still having issues?
        └─ Check Spark UI shuffle read sizes
            └─ Adjust based on 100-200MB target
```

## Common Issues

| Issue | Solution |
|-------|----------|
| **Too many small partitions** | Enable AQE coalescing; increase target size |
| **Too few large partitions** | Enable AQE splitting; decrease target size |
| **Checkpoint not picking up changes** | Clear checkpoint; it stores configuration |
| **Still slow after AQE** | Check if bottleneck is elsewhere (network, I/O) |
| **Memory issues** | Reduce partitions; increase executor memory |

## Advanced Tips

### Measuring In-Memory Size

```python
# On-disk size ≠ in-memory size
# Parquet compression: 2-8x expansion in memory

# Measure actual size:
df = spark.read.parquet("path/to/data")
df.write.format("noop").mode("overwrite").save()

# Check Spark UI:
# SQL/DataFrame tab → Shuffle read size
# Use this for partition calculations
```

### AQE Configuration

```python
# Fine-tune AQE behavior
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "200")

# Target partition size
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "200MB")
```

### When Manual Tuning Still Matters

```python
# Scenarios where manual tuning helps:

# 1. Non-shuffle operations (no AQE benefit)
# 2. Very specific performance requirements
# 3. Legacy Spark versions (< 3.0)
# 4. Debugging AQE behavior

# Rule: Start with AQE, tune only if needed
```

## FAQ

**Q: Should I still set shuffle partitions manually in 2025?**
A: Usually no. Let AQE handle it. Only set manually if AQE is disabled or you have specific requirements.

**Q: How do I know if my partitions are optimal?**
A: Check Spark UI shuffle read sizes. Target 100-200MB per partition. AQE adjusts automatically.

**Q: What if AQE isn't helping?**
A: Check if bottleneck is elsewhere (I/O, network, serialization). AQE only helps with partition sizing.

**Q: Can I change shuffle partitions mid-job?**
A: No. Configuration is set at job start. For streaming, must clear checkpoint to change.

**Q: What's the relationship between shuffle partitions and worker cores?**
A: Optimal: shuffle partitions = total worker cores. More partitions = more overhead. Fewer partitions = underutilization.
