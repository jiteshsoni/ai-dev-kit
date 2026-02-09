---
name: query-tuning
description: "Spark SQL query optimization and performance tuning patterns."
tags: ["spark", "query", "performance", "tuning"]
---

# Query Tuning

## Overview

Optimize Spark SQL queries for better performance and cost efficiency.

## Quick Start

### Enable Adaptive Query Execution

```python
# AQE optimizes at runtime
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### Set Proper Shuffle Partitions

```python
# Default: 200 partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")

# For small datasets
spark.conf.set("spark.sql.shuffle.partitions", "8")

# For large datasets
spark.conf.set("spark.sql.shuffle.partitions", "1000")
```

## Common Patterns

### Pattern 1: File Sizing

```python
# Control partition size
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
spark.conf.set("spark.sql.files.minPartitionBytes", "67108864")    # 64MB
```

### Pattern 2: Broadcast Join

```python
# Small table join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB
```

### Pattern 3: Z-ORDER Clustering

```sql
-- Optimize query performance
OPTIMIZE table_name ZORDER BY (date, user_id);
```
