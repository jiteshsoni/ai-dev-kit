---
name: spark-join-broadcast-hash
description: Broadcast Hash Join strategy in Spark that broadcasts small tables to all executors for fast joins without shuffling the large table. Use when joining large fact tables with small dimension tables, optimizing join performance, or understanding when Spark automatically chooses broadcast joins.
---

# Spark Join Strategies: Broadcast Hash Join

## Overview

Broadcast Hash Join (BHJ) is Spark's fastest join strategy when one side is small. It broadcasts the entire small dataset to all executors, eliminating the need to shuffle the large table and enabling local hash joins on each executor.

## Quick Start

### Automatic Broadcast

```python
# Spark automatically broadcasts if small table < threshold
# Default threshold: 10MB (open-source), ~30MB (Databricks)

# Small dimension table
dim_df = spark.table("dim_customers")  # < 10MB

# Large fact table
fact_df = spark.table("fact_orders")  # > 1GB

# Join: Spark automatically broadcasts dim_df
result = fact_df.join(dim_df, "customer_id")

# Look for "BroadcastHashJoin" in query plan
```

### Force Broadcast

```python
from pyspark.sql.functions import broadcast

# Explicitly force broadcast
result = fact_df.join(broadcast(dim_df), "customer_id")

# Or SQL hint
spark.sql("""
    SELECT /*+ BROADCAST(dim) */ *
    FROM fact f
    JOIN dim d ON f.id = d.id
""")
```

## Common Patterns

### Pattern 1: Dimension Table Enrichment

```python
# Common use case: Enrich fact table with dimension data
orders = spark.table("orders")  # Large
customers = spark.table("customers")  # Small (< 10MB)

# Automatic broadcast
enriched = orders.join(customers, "customer_id")

# Benefits:
# - No shuffle of orders table
# - Fast local hash join on each executor
# - 10-100x faster than shuffle join
```

### Pattern 2: Increase Broadcast Threshold

```python
# For larger dimension tables
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")

# Or Databricks-specific (higher default)
spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", "200MB")

# Use when:
# - Dimension table 50-200MB
# - Executors have sufficient memory
# - Driver has enough memory
```

### Pattern 3: AQE Dynamic Broadcast

```python
# Adaptive Query Execution can convert to broadcast at runtime
spark.conf.set("spark.sql.adaptive.enabled", "true")

# AQE measures actual data sizes
# Converts sort-merge join to broadcast if small enough
# No code changes needed
```

## Reference Files

### Broadcast Process

```
1. Driver collects small table
   ↓
2. Converts to hash map
   ↓
3. Broadcasts to all executors (torrent-like)
   ↓
4. Each executor caches broadcast data
   ↓
5. Local hash join with large table partition
```

### When Spark Uses BHJ

- **Size**: Small table < broadcast threshold (default 10MB)
- **Join type**: Equi-join (equality condition)
- **Supported**: Inner, left, semi, anti joins
- **Not supported**: Full outer joins

### Memory Considerations

- **Driver memory**: Small table must fit in driver
- **Executor memory**: Each executor stores broadcast copy
- **Total memory**: # executors × small table size
- **Limit**: Hard limit ~8GB per broadcast

## Common Issues

| Issue | Solution |
|-------|----------|
| **Broadcast OOM** | Reduce threshold; check actual in-memory size |
| **Not broadcasting** | Check table size; increase threshold if appropriate |
| **Slow broadcast** | Table too large; use shuffle join instead |
| **Driver OOM** | Reduce broadcast threshold; use larger driver |

## Advanced Tips

### Measuring Table Size

```python
# On-disk size ≠ in-memory size
# Parquet compression: 2-8x expansion

# Measure actual size:
df.write.format("noop").mode("overwrite").save()

# Check Spark UI → SQL tab → Data size
# Use this for broadcast decisions
```

### Join Order Optimization

```python
# Counter-intuitive: Do broadcast joins LAST
# Why?
# - Broadcast joins don't require shuffle
# - If done first, joined data needs shuffle for next join
# - Doing last avoids extra shuffle

# Order:
# 1. Shuffle joins first (data already shuffled)
# 2. Broadcast joins last (no shuffle needed)
```

### Production Best Practices

```python
# 1. Validate table size before forcing broadcast
dim_size = dim_df.count() * avg_row_size
if dim_size > 100MB:
    # Don't force broadcast
    pass

# 2. Monitor Spark UI for broadcast metrics
# - Broadcast time
# - Memory usage
# - GC pressure

# 3. Use hints for ad-hoc queries
# 4. Let AQE handle it in production (if enabled)
```

## FAQ

**Q: When should I force a broadcast?**
A: When you know table is small but Spark statistics are wrong, or in ad-hoc queries. In production, let AQE handle it.

**Q: What's the maximum broadcast size?**
A: Hard limit ~8GB. Practical limit depends on executor memory. Usually safe up to 100-200MB per executor.

**Q: Can I broadcast both sides?**
A: No. Only one side can be broadcast. Spark chooses the smaller side automatically.

**Q: Does broadcast work with streaming?**
A: Yes, for stream-static joins. The static side can be broadcast if small enough.

**Q: What if my dimension table grows?**
A: Monitor size. If exceeds threshold, Spark automatically switches to shuffle join. Update threshold if needed.
