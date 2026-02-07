---
name: liquid-clustering-vs-partitioning
description: Decision guide for choosing between Liquid Clustering and Partitioning with Z-Order in Delta Lake. Use when designing Delta table organization strategies, optimizing query performance, or deciding on data layout for streaming vs batch workloads.
---

# Liquid Clustering vs Partitioning

## Overview

Delta Lake offers two primary data organization strategies: Liquid Clustering (flexible, modern) and Partitioning with Z-Order (traditional, fine-grained control). Choosing the right approach depends on table size, query patterns, data distribution, and ingestion patterns.

## Quick Start

### Liquid Clustering

```python
# Create table with liquid clustering
spark.sql("""
    CREATE TABLE events (
        event_id STRING,
        user_id STRING,
        event_time TIMESTAMP,
        category STRING
    ) USING DELTA
    CLUSTER BY (user_id, category)
""")

# Benefits:
# - Flexible: Change clustering columns anytime
# - Works without partitioning
# - Efficient: Doesn't re-cluster existing files
```

### Partitioning with Z-Order

```python
# Create partitioned table with Z-order
spark.sql("""
    CREATE TABLE events (
        event_id STRING,
        user_id STRING,
        event_time TIMESTAMP,
        category STRING
    ) USING DELTA
    PARTITIONED BY (event_date DATE)
""")

# Optimize with Z-order
spark.sql("""
    OPTIMIZE events
    ZORDER BY (user_id, category)
""")
```

## Common Patterns

### Pattern 1: Decision by Table Size

```python
# Small tables (< 10TB):
# - Liquid Clustering: Good for 2-column lookups
# - Partition + Z-order: Better for 3+ column lookups

# Medium tables (10TB - 500TB):
# - Partition cardinality < 5,000: Partition + Z-order
# - Partition cardinality > 5,000: Liquid Clustering

# Large tables (> 500TB):
# - Consult Databricks representative
```

### Pattern 2: Streaming vs Batch

```python
# Streaming Ingestion:

# Low latency priority:
# - Liquid Clustering WITHOUT eager clustering
# - Reduces shuffle overhead during ingestion
# - Query performance improves later via Predictive I/O

# Fast lookups priority:
# - Liquid Clustering WITH eager clustering
# - Data well-clustered on write
# - Follow-up OPTIMIZE improves further

# Batch Ingestion:
# - Liquid Clustering: Strong default choice
# - Eager clustering can be enabled
```

### Pattern 3: Query Pattern Considerations

```python
# If queries ALWAYS include partition column:
# - Partitioning very effective
# - Partition pruning eliminates data scanning

# If queries VARIABLE (sometimes include, sometimes don't):
# - Liquid Clustering more flexible
# - Works well for diverse query patterns

# Example:
# - Always filter by date → Partition by date
# - Sometimes filter by user, sometimes by category → Liquid cluster
```

## Reference Files

### Decision Tree Summary

```
Start: Choose data organization strategy
│
├─ Table size?
│  ├─ < 10TB → Liquid Clustering (2 cols) or Partition+Z-order (3+ cols)
│  ├─ 10-500TB → Check partition cardinality
│  │   ├─ < 5,000 → Partition + Z-order
│  │   └─ > 5,000 → Liquid Clustering
│  └─ > 500TB → Consult Databricks
│
├─ Query patterns?
│  ├─ Always include partition col → Partitioning
│  └─ Variable patterns → Liquid Clustering
│
└─ Ingestion pattern?
   ├─ Streaming (low latency) → Liquid (no eager)
   ├─ Streaming (fast lookups) → Liquid (with eager)
   └─ Batch → Liquid Clustering (default)
```

### Liquid Clustering Benefits

- **Flexibility**: Change clustering columns anytime
- **No over-partitioning**: Avoids partition explosion
- **Efficient updates**: Doesn't re-cluster existing files
- **Works unpartitioned**: No partition column required

### Partitioning Benefits

- **Partition pruning**: Eliminates data scanning
- **Parallel writes**: Better for concurrent writes
- **Fine-grained control**: Optimize specific partitions
- **Proven**: Traditional, well-understood approach

## Common Issues

| Issue | Solution |
|-------|----------|
| **Over-partitioning** | Use Liquid Clustering if > 5,000 partitions |
| **Uneven partition sizes** | Liquid Clustering handles skew better |
| **Query performance poor** | Check if queries include partition column |
| **Streaming latency high** | Disable eager clustering for Liquid |
| **Too many small files** | Use Liquid Clustering; better file management |

## Advanced Tips

### Partition Column Selection

```python
# Good partition columns:
# - Immutable (date, region, country)
# - Low cardinality (< 10,000 distinct values)
# - Even distribution
# - Frequently filtered in queries

# Bad partition columns:
# - High cardinality (timestamp, user_id)
# - Skewed distribution
# - Rarely filtered

# For timestamps: Create derived date column
df = df.withColumn("event_date", to_date(col("event_timestamp")))
```

### Liquid Clustering Configuration

```python
# Enable eager clustering (Databricks Runtime 13.3+)
spark.conf.set("spark.databricks.delta.autoOptimize.eagerClustering.enabled", "true")

# Benefits:
# - Data clustered on write
# - Immediate query performance
# - Trade-off: Slight write overhead

# For low-latency streaming: Disable eager clustering
```

### Optimizing Partitioned Tables

```python
# Optimize specific partitions
spark.sql("""
    OPTIMIZE events
    WHERE event_date = '2024-01-15'
    ZORDER BY (user_id, category)
""")

# Benefits:
# - Target specific partitions
# - Z-order improves lookup performance
# - Can run in parallel for different partitions
```

## FAQ

**Q: Can I use both partitioning and liquid clustering?**
A: Yes, but usually not needed. Choose one primary strategy. Liquid clustering works well unpartitioned.

**Q: How do I change clustering columns?**
A: With Liquid Clustering, you can change columns anytime. New writes use new columns. Old files remain.

**Q: What if I have > 10,000 partitions?**
A: Consider Liquid Clustering instead. Over-partitioning causes metadata overhead and slow queries.

**Q: Which is better for streaming?**
A: Liquid Clustering generally better. Can disable eager clustering for low latency, or enable for fast lookups.

**Q: Can I migrate from partitioning to liquid clustering?**
A: Yes. Create new table with liquid clustering, copy data, switch applications. Old partitioned table can remain.
