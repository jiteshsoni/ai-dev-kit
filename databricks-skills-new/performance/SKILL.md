---
name: performance
description: "Performance tuning and optimization patterns for Databricks workloads."
tags: ["performance", "optimization", "tuning", "cost-reduction"]
---

# Performance Tuning

Optimization patterns for Spark, Delta Lake, and streaming workloads.

## Overview

**Key Areas:**
- **Query Optimization**: Spark SQL tuning, partitioning
- **Streaming Performance**: Cost optimization, latency reduction
- **Storage Optimization**: File sizing, layout, VACUUM
- **Cost Reduction**: S3 API optimization, caching strategies

## Quick Start

### Stream Cost Optimization

```python
# Reduce S3 API calls
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
```

### Query Tuning

```python
# Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

## Available Skills

- [Streaming Cost Optimization](./streaming-cost-optimization.md) - S3 API optimization
- [Query Tuning](./query-tuning.md) - Spark query optimization
- [Checkpoint Optimization](./checkpoint-optimization.md) - Checkpoint tuning
- [Partitioning Strategy](./partitioning-strategy.md) - Data layout optimization
- [Storage Optimization](./storage-optimization.md) - File compaction and layout
- [Cache Warming](./cache-warming.md) - SQL warehouse cache optimization
- [Autoscaling Guide](./autoscaling-guide.md) - Cluster autoscaling patterns
- [File Read Optimization](./file-read-optimization.md) - maxPartitionBytes tuning
