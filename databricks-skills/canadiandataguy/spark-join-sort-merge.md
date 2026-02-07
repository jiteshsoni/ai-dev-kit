---
name: spark-join-sort-merge
description: Sort-Merge Join is Spark's default join strategy for large datasets, shuffling and sorting both sides then merging. Use when joining large tables, understanding Spark's default join behavior, or when other strategies aren't applicable.
---

# Spark Join Strategies: Sort-Merge Join

## Overview

Sort-Merge Join (SMJ) is Spark's default and most robust join strategy for large datasets. It shuffles both datasets, sorts them by join key, then merges sorted partitions. While CPU and network intensive, SMJ can handle very large tables and supports all join types.

## Quick Start

### Default Behavior

```python
# Spark defaults to SMJ when:
# - Neither side small enough for broadcast
# - Large datasets
# - All join types supported

# No configuration needed - it's the default
result = large_df1.join(large_df2, "join_key")

# Query plan shows: SortMergeJoin
```

### Execution Phases

```python
# SMJ has three phases:

# 1. Shuffle: Repartition both datasets by join key
# 2. Sort: Sort each partition by join key
# 3. Merge: Merge sorted partitions (linear time)

# All phases visible in query plan
```

## Common Patterns

### Pattern 1: Understanding SMJ Execution

```python
# Physical plan shows:
# - Exchange (shuffle) on both sides
# - Sort on both sides
# - SortMergeJoin operator

# Example plan:
# SortMergeJoin [id], [id], Inner
#   :- Sort [id ASC]
#   :  +- Exchange hashpartitioning(id, 200)
#   +- Sort [id ASC]
#      +- Exchange hashpartitioning(id, 200)
```

### Pattern 2: Tuning SMJ Performance

```python
# Key tuning parameters:

# 1. Shuffle partitions (target: 100-200MB per partition)
spark.conf.set("spark.sql.shuffle.partitions", 200)

# 2. Enable AQE for dynamic optimization
spark.conf.set("spark.sql.adaptive.enabled", "true")

# 3. Reduce data volume before join
df1.filter(...).select(...).join(df2.filter(...).select(...))
```

### Pattern 3: AQE Optimizations

```python
# AQE improves SMJ automatically:

# 1. Dynamic partition coalescing
# - Merges small partitions after shuffle
# - Reduces task overhead

# 2. Skew handling
# - Splits large skewed partitions
# - Eliminates straggler tasks

# 3. Strategy conversion
# - Converts SMJ to BHJ if small side detected
# - Converts SMJ to SHJ if appropriate
```

## Reference Files

### SMJ Characteristics

| Aspect | Details |
|--------|---------|
| **Shuffle** | Both sides shuffled (network I/O) |
| **Sort** | Both sides sorted (CPU intensive) |
| **Memory** | Can spill to disk (stable) |
| **Join Types** | All types supported |
| **Scalability** | Handles very large tables |

### Why SMJ is Stable

```python
# SMJ can handle data larger than memory:
# - External sort spills to disk
# - Graceful degradation (slower, not failure)
# - No OOM errors (unlike hash joins)

# Trade-off: Slower when spilling
# But: Won't crash
```

### Supported Join Types

- **Inner join**: Most common
- **Left/Right outer**: All rows from one side
- **Full outer**: All rows from both sides
- **Semi/Anti joins**: Existence checks

## Common Issues

| Issue | Solution |
|-------|----------|
| **Slow performance** | Increase shuffle partitions; enable AQE |
| **Disk spilling** | Increase partitions; reduce data volume |
| **Data skew** | Enable AQE skew handling; use salting |
| **Too many partitions** | Let AQE coalesce; reduce shuffle partitions |

## Advanced Tips

### Optimizing SMJ

```python
# 1. Reduce shuffle volume
# Project only needed columns before join
df1.select("id", "col1").join(df2.select("id", "col2"), "id")

# 2. Filter early
# Push filters down before join
df1.filter(col("status") == "active").join(df2, "id")

# 3. Use bucketing (if applicable)
# Pre-partitioned and sorted data avoids shuffle/sort
```

### Monitoring SMJ Performance

```python
# Check Spark UI for:
# - Shuffle read/write sizes
# - Sort spill metrics
# - Task duration distribution
# - Skew indicators

# Target metrics:
# - Shuffle read: 100-200MB per partition
# - Sort spill: Minimal (ideally zero)
# - Task duration: Even distribution
```

### AQE Benefits for SMJ

```python
# AQE automatically:
# 1. Coalesces small partitions (reduces overhead)
# 2. Splits skewed partitions (eliminates stragglers)
# 3. Converts to better strategy if possible

# Enable AQE (default in Spark 3.2+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

## FAQ

**Q: When does Spark use SMJ?**
A: By default for large datasets when broadcast isn't possible. Most common join strategy.

**Q: Can SMJ handle data larger than memory?**
A: Yes. External sort spills to disk. Slower but won't crash (unlike hash joins).

**Q: How do I optimize SMJ performance?**
A: Increase shuffle partitions, enable AQE, reduce data volume before join, handle skew.

**Q: What's the difference between SMJ and SHJ?**
A: SMJ sorts data (can spill). SHJ builds hash tables (must fit in memory). SMJ more stable.

**Q: Can I skip the sort phase?**
A: Yes, if data is bucketed and sorted on join key. Requires both sides bucketed identically.
