---
name: spark-skewed-joins-groupby
description: Comprehensive strategies for detecting and mitigating data skew in Spark joins and GroupBy operations. Use when experiencing straggler tasks, out-of-memory errors during joins, or performance bottlenecks in aggregations due to uneven data distribution.
---

# Spark Skewed Joins and GroupBy Bottlenecks

## Overview

Data skew occurs when data is unevenly distributed across partitions, causing hotspots and straggler tasks during shuffle-intensive operations like joins and aggregations. This skill covers detection methods and mitigation strategies from automatic (AQE) to manual techniques.

## Quick Start

### Detecting Skew

```python
# 1. Check Spark UI: Task duration discrepancy
# - Most tasks finish quickly
# - Few tasks hang for long time

# 2. Check shuffle read sizes
# - Significant difference between min/max indicates skew

# 3. Count rows by key
skew_check = df.groupBy("join_key").count().orderBy("count", ascending=False)
skew_check.show(20)  # Top keys show skew

# 4. Monitor data spills
# - Many spills despite tuning = likely skew
```

### Enable AQE (Automatic Skew Handling)

```python
# Enable Adaptive Query Execution (default in Spark 3.2+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# AQE automatically detects and splits skewed partitions
# No code changes needed!
```

## Common Patterns

### Pattern 1: Adaptive Query Execution (AQE)

```python
# AQE automatically handles skew at runtime
# Configuration:
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5.0")

# How it works:
# 1. After shuffle, measures partition sizes
# 2. Identifies skewed partitions (> threshold AND > factor × median)
# 3. Splits skewed partitions into sub-tasks
# 4. Replicates matching rows from other side
# 5. Runs sub-tasks in parallel

# Pros: Zero code changes, runtime intelligence
# Cons: Only works for shuffle joins, adds some overhead
```

### Pattern 2: Broadcast Hash Join

```python
from pyspark.sql.functions import broadcast

# If one side is small, broadcast eliminates skew
result = large_df.join(broadcast(small_df), "join_key")

# Or increase threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")

# Or use SQL hint
# SELECT /*+ BROADCAST(small_table) */ * FROM large_table l
# JOIN small_table s ON l.key = s.key

# Pros: No shuffle on large side, eliminates skew
# Cons: Small table must fit in executor memory
```

### Pattern 3: Handle Skewed Keys Separately

```python
# Split data into skewed vs rest
skewed_keys = ["USA", "China"]  # Known hot keys

# Filter each dataset
large_skewed = large_df.filter(col("country").isin(skewed_keys))
large_rest = large_df.filter(~col("country").isin(skewed_keys))

small_skewed = small_df.filter(col("country").isin(skewed_keys))
small_rest = small_df.filter(~col("country").isin(skewed_keys))

# Join rest normally (balanced)
result_rest = large_rest.join(small_rest, "country")

# Join skewed with broadcast (if small) or salting
result_skewed = large_skewed.join(broadcast(small_skewed), "country")

# Union results
final_result = result_rest.unionByName(result_skewed)
```

### Pattern 4: Salting (Uniform Distribution)

```python
from pyspark.sql.functions import concat_ws, abs, hash, lit

# Salt every key uniformly across N buckets
N = 10  # Number of salt buckets

# Add salt column to both tables
large_salted = large_df.withColumn(
    "salt",
    abs(hash(concat_ws("_", col("join_key"), col("other_col")))) % N
).withColumn(
    "salted_key",
    concat_ws("_", col("join_key"), col("salt"))
)

small_salted = small_df.withColumn(
    "salt",
    abs(hash(concat_ws("_", col("join_key"), col("other_col")))) % N
).withColumn(
    "salted_key",
    concat_ws("_", col("join_key"), col("salt"))
)

# Join on salted key
result = large_salted.join(small_salted, "salted_key")

# Remove salt column if needed
result = result.drop("salt", "salted_key").withColumnRenamed("join_key", "original_key")
```

## Reference Files

### Skew Detection Methods

| Method | How to Use | What It Shows |
|--------|------------|---------------|
| **Spark UI** | Check task duration | Straggler tasks indicate skew |
| **Shuffle Read Size** | Compare min vs max | Large discrepancy = skew |
| **Row Counts** | Group by join key | Uneven distribution |
| **Data Spills** | Monitor spills | Many spills = potential skew |
| **Compression Ratios** | Check table stats | High compression affects estimation |

### Mitigation Strategies Comparison

| Strategy | When to Use | Code Changes | Effectiveness |
|----------|-------------|--------------|---------------|
| **AQE** | Shuffle joins | None (config only) | High (automatic) |
| **Broadcast** | One side small | Hint or threshold | Very High |
| **Split Skewed** | Known hot keys | Filter + separate join | High |
| **Salting** | Unknown/unpredictable skew | Add salt column | Very High |
| **Increase Partitions** | Mild skew | Config change | Low-Medium |

### AQE Configuration

```python
# Enable AQE (default in Spark 3.2+)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Skew join settings
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5.0")

# Force skew optimization (Spark 3.3+)
spark.conf.set("spark.sql.adaptive.forceOptimizeSkewedJoin", "true")
```

## Common Issues

| Issue | Solution |
|-------|----------|
| **Straggler tasks** | Enable AQE; use broadcast if possible |
| **Out of memory during join** | Use broadcast; split skewed keys; salt |
| **Skew in GroupBy** | Salt grouping key; increase partitions |
| **AQE not detecting skew** | Check thresholds; verify AQE enabled |
| **Broadcast causing OOM** | Reduce broadcast threshold; split data |
| **Salting too complex** | Start with AQE; only salt if needed |

## Advanced Tips

### GroupBy Skew Mitigation

```python
# GroupBy also suffers from skew
# Solution: Salt the grouping key

N = 10
df_salted = df.withColumn(
    "salt",
    abs(hash(col("group_key"))) % N
).withColumn(
    "salted_group_key",
    concat_ws("_", col("group_key"), col("salt"))
)

# Aggregate on salted key
aggregated = df_salted.groupBy("salted_group_key").agg(
    sum("value").alias("total")
)

# Remove salt and re-aggregate
final = aggregated.withColumn(
    "group_key",
    split(col("salted_group_key"), "_")[0]
).groupBy("group_key").agg(
    sum("total").alias("final_total")
)
```

### Choosing Salt Count (N)

```python
# Rule of thumb: Target 100-300MB per salted partition
# Use Spark UI to measure shuffle read sizes

# Example calculation:
# - Total data: 10GB
# - Target partition: 200MB
# - N = 10GB / 200MB = 50 buckets

# Start conservative (N=10), increase if needed
```

### Broadcast Threshold Tuning

```python
# Default: 10MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")

# Increase for larger small tables (up to ~1GB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")

# Consider executor memory:
# - 8GB executor: ~100MB safe
# - 16GB executor: ~200MB safe
# - 32GB executor: ~500MB safe
```

### Monitoring Skew

```python
# Programmatic skew detection
def detect_skew(df, key_col, threshold=5.0):
    """Detect if key distribution is skewed"""
    counts = df.groupBy(key_col).count()
    stats = counts.agg(
        avg("count").alias("avg"),
        stddev("count").alias("stddev"),
        max("count").alias("max"),
        min("count").alias("min")
    ).collect()[0]
    
    skew_ratio = stats.max / stats.avg if stats.avg > 0 else 0
    return skew_ratio > threshold

# Use before joins
if detect_skew(large_df, "join_key"):
    # Apply mitigation strategy
    pass
```

## FAQ

**Q: When should I use AQE vs manual techniques?**
A: Always enable AQE first. Use manual techniques (salting, splitting) only if AQE doesn't solve the problem or you need more control.

**Q: How do I choose the salt count N?**
A: Target 100-300MB per salted partition. Measure shuffle read sizes in Spark UI and adjust N accordingly.

**Q: Can I use broadcast join for both sides large?**
A: No. Broadcast only works when one side fits in executor memory. For both large, use AQE or salting.

**Q: Does AQE work for GroupBy operations?**
A: AQE primarily helps with joins. For GroupBy skew, use salting or increase partitions.

**Q: How do I identify which keys are skewed?**
A: Group by join key and count. Keys with counts significantly higher than average are skewed.

**Q: What if I have multiple skewed keys?**
A: Use uniform salting (salt all keys) rather than handling each key separately. More scalable.
