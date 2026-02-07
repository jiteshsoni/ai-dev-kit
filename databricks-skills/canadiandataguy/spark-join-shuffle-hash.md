---
name: spark-join-shuffle-hash
description: Shuffle Hash Join strategy in Spark that shuffles both tables and builds in-memory hash tables per partition. Use when joining large tables where neither side is small enough to broadcast, avoiding sort overhead while managing memory constraints.
---

# Spark Join Strategies: Shuffle Hash Join

## Overview

Shuffle Hash Join (SHJ) is a hybrid join strategy that shuffles both datasets like Sort-Merge Join but builds in-memory hash tables instead of sorting. It eliminates sort overhead but requires careful memory management since hash tables must fit in memory per partition.

## Quick Start

### When Spark Uses SHJ

```python
# Spark 3.x with AQE can choose SHJ when:
# - Neither side small enough for broadcast
# - Smaller side partitions fit in memory
# - Avoiding sort overhead is beneficial

# Enable AQE (default in Spark 3.2+)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# AQE dynamically converts SMJ to SHJ when appropriate
```

### Force Shuffle Hash Join

```python
# Disable sort-merge preference
spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")

# Or use SQL hint
spark.sql("""
    SELECT /*+ SHUFFLE_HASH(table1) */ *
    FROM table1 JOIN table2 ON table1.id = table2.id
""")
```

## Common Patterns

### Pattern 1: Understanding SHJ Execution

```python
# SHJ has two phases:

# Phase 1: Shuffle
# - Both datasets shuffled by join key
# - Matching keys co-located in same partition
# - Network I/O for both sides

# Phase 2: Hash Join
# - Build hash table from smaller side per partition
# - Probe hash table with larger side
# - No sorting required
```

### Pattern 2: Memory Considerations

```python
# Critical: Hash table must fit in memory per partition
# If partition too large → OOM error

# Check partition size:
# - Use Spark UI shuffle read metrics
# - Target: < 100MB per partition for hash table
# - Adjust shuffle partitions if needed

spark.conf.set("spark.sql.shuffle.partitions", 200)
# More partitions = smaller hash tables per partition
```

### Pattern 3: AQE Dynamic Conversion

```python
# AQE can convert SMJ to SHJ at runtime
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", "64MB")

# AQE measures actual partition sizes
# Converts to SHJ if smaller side partitions < threshold
# No code changes needed
```

## Reference Files

### SHJ vs Other Strategies

| Strategy | Shuffle | Sort | Memory | Use Case |
|----------|---------|------|--------|----------|
| **Broadcast** | No | No | High (broadcast) | One side small |
| **Shuffle Hash** | Yes | No | High (hash table) | Moderate sizes |
| **Sort-Merge** | Yes | Yes | Low (spills) | Large tables |

### Execution Phases

```
1. Shuffle Phase
   - Repartition both datasets by join key
   - Network I/O for both sides
   ↓
2. Hash Join Phase
   - Build hash table (smaller side per partition)
   - Probe hash table (larger side)
   - No sorting
```

### When to Use SHJ

- **Moderate dataset sizes**: Too large for broadcast, not huge
- **Memory available**: Hash tables fit in executor memory
- **Avoid sorting**: Sort overhead would be significant
- **AQE enabled**: Let Spark decide dynamically

## Common Issues

| Issue | Solution |
|-------|----------|
| **OOM errors** | Increase shuffle partitions; reduce partition size |
| **Not being used** | Check AQE enabled; verify partition sizes |
| **Slow performance** | Check if hash tables spilling; increase memory |
| **Memory pressure** | Use Sort-Merge Join instead (can spill) |

## Advanced Tips

### Tuning SHJ Performance

```python
# 1. Control partition size
spark.conf.set("spark.sql.shuffle.partitions", 200)
# Target: 100-200MB per partition

# 2. Monitor hash table size
# Check Spark UI → Shuffle read size
# If > 100MB per partition → increase partitions

# 3. Use AQE for dynamic optimization
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

### Memory Requirements

```python
# Hash table size = smaller side partition size
# Must fit in executor memory

# Example:
# - Smaller table: 5GB total
# - Shuffle partitions: 100
# - Per partition: ~50MB hash table
# - Executor memory: 8GB → Safe

# If partition > 200MB → Risk of OOM
```

### AQE Configuration

```python
# Threshold for SHJ conversion
spark.conf.set(
    "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold",
    "64MB"  # Default
)

# AQE checks if smaller side partition < threshold
# If yes → Use SHJ
# If no → Use SMJ (can spill)
```

## FAQ

**Q: When should I use SHJ over SMJ?**
A: When you have moderate-sized tables, sufficient memory, and want to avoid sort overhead. Let AQE decide.

**Q: What if hash table doesn't fit in memory?**
A: Spark will OOM. Use Sort-Merge Join instead (can spill to disk) or increase shuffle partitions.

**Q: Can I force SHJ?**
A: Yes, but risky. Better to let AQE decide based on actual data sizes at runtime.

**Q: How do I know if SHJ is being used?**
A: Check query plan for "ShuffledHashJoin" operator. Spark UI shows execution details.

**Q: What's the performance benefit?**
A: Eliminates sort phase (O(n log n)). Faster when hash tables fit in memory. Risk: OOM if misjudged.
