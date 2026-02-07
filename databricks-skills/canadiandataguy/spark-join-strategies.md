---
name: spark-join-strategies-handbook
description: Visual handbook explaining Spark join strategies with execution plans, performance characteristics, and when to use each strategy. Use when understanding join execution, optimizing join performance, or explaining join strategies to team members.
---

# Spark Join Strategies: Visual Handbook

## Overview

Spark employs multiple join strategies to efficiently combine datasets. Understanding when Spark chooses each strategy and how to influence those choices is essential for optimizing join performance. This handbook covers Broadcast Hash Join, Shuffle Hash Join, and Sort-Merge Join with visual explanations.

## Quick Start

### Join Strategy Selection

```python
# Spark chooses join strategy based on:
# 1. Dataset sizes (statistics)
# 2. Join type
# 3. Available memory
# 4. Configuration settings

# View chosen strategy in query plan
df.explain(extended=True)

# Look for:
# - BroadcastHashJoin
# - ShuffledHashJoin  
# - SortMergeJoin
```

### Strategy Comparison

| Strategy | Shuffle | Sort | Memory | Speed |
|----------|---------|------|--------|-------|
| **Broadcast Hash** | No | No | High | Fastest |
| **Shuffle Hash** | Yes | No | High | Fast |
| **Sort-Merge** | Yes | Yes | Low | Stable |

## Common Patterns

### Pattern 1: Understanding Execution Plans

```python
# Broadcast Hash Join Plan:
# BroadcastExchange → BroadcastHashJoin
# - Small table broadcasted
# - No shuffle of large table
# - Fast local hash join

# Shuffle Hash Join Plan:
# Exchange → ShuffledHashJoin
# - Both sides shuffled
# - Hash table built per partition
# - No sorting

# Sort-Merge Join Plan:
# Exchange → Sort → SortMergeJoin
# - Both sides shuffled and sorted
# - Merge sorted partitions
# - Most stable
```

### Pattern 2: Influencing Join Strategy

```python
# Method 1: Increase broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")

# Method 2: Use broadcast hint
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")

# Method 3: SQL hint
spark.sql("SELECT /*+ BROADCAST(small) */ * FROM large l JOIN small s ON l.id = s.id")

# Method 4: Disable sort-merge preference
spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")
```

### Pattern 3: Join Order Optimization

```python
# Counter-intuitive: Do broadcast joins LAST
# Why?
# - Broadcast joins don't require shuffle
# - If done first, joined data needs shuffle for next join
# - Doing last avoids extra shuffle

# Order joins:
# 1. Shuffle joins first (data already shuffled)
# 2. Broadcast joins last (no shuffle needed)
```

## Reference Files

### Join Strategy Decision Tree

```
Start: Join operation
│
├─ One side < broadcast threshold?
│  └─ Yes → Broadcast Hash Join
│
├─ Smaller side partitions fit in memory?
│  └─ Yes → Shuffle Hash Join (if AQE enabled)
│
└─ Default → Sort-Merge Join
```

### Execution Characteristics

**Broadcast Hash Join**:
- No shuffle of large table
- Fastest when applicable
- Memory: Small table × # executors
- Limit: ~8GB broadcast size

**Shuffle Hash Join**:
- Shuffles both sides
- Hash table per partition
- Memory: Smaller side partition size
- Risk: OOM if partition too large

**Sort-Merge Join**:
- Shuffles both sides
- Sorts both sides
- Can spill to disk
- Most stable, supports all join types

## Common Issues

| Issue | Solution |
|-------|----------|
| **Not using broadcast** | Check table size; increase threshold if appropriate |
| **Broadcast OOM** | Reduce threshold; check actual in-memory size |
| **Slow sort-merge** | Increase shuffle partitions; enable AQE |
| **Hash join OOM** | Use sort-merge instead; increase partitions |

## Advanced Tips

### Monitoring Join Performance

```python
# Check Spark UI for:
# - Shuffle read/write sizes
# - Sort spill metrics
# - Task duration distribution
# - Broadcast time

# Target metrics:
# - Shuffle: 100-200MB per partition
# - Sort spill: Minimal (ideally zero)
# - Broadcast: < 1 second
```

### AQE Benefits

```python
# AQE automatically:
# 1. Converts SMJ to BHJ if small side detected
# 2. Converts SMJ to SHJ if appropriate
# 3. Handles skew in joins
# 4. Coalesces small partitions

# Enable AQE (default in Spark 3.2+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

## FAQ

**Q: How does Spark choose join strategy?**
A: Based on dataset sizes, join type, and configuration. AQE can change strategy at runtime.

**Q: Can I force a specific join strategy?**
A: Yes, using hints or configuration. But let AQE decide in production when possible.

**Q: Which strategy is fastest?**
A: Broadcast Hash Join (when applicable). Then Shuffle Hash. Then Sort-Merge.

**Q: Which strategy is most stable?**
A: Sort-Merge Join. Can handle data larger than memory, won't OOM.

**Q: How do I optimize join performance?**
A: Enable AQE, tune shuffle partitions, reduce data volume before join, handle skew.
