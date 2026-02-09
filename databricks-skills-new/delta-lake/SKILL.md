---
name: delta-lake
description: "Delta Lake patterns for table optimization, storage management, and concurrent operations."
tags: ["delta-lake", "optimization", "storage", "concurrency"]
---

# Delta Lake

Comprehensive guide to Delta Lake features: VACUUM, Liquid Clustering, Deletion Vectors, and concurrent operations.

## Overview

Delta Lake provides ACID transactions, time travel, and optimized performance on data lakes.

**Key Features:**
- **ACID Transactions**: Consistent reads and writes
- **Time Travel**: Query historical versions
- **Optimization**: Liquid Clustering, Z-ORDER, OPTIMIZE
- **Concurrency**: Row-Level Concurrency for parallel operations

## Quick Start

### Creating a Delta Table

```sql
CREATE TABLE events (
    id STRING,
    timestamp TIMESTAMP,
    data STRING
) USING DELTA
LOCATION '/path/to/table'
```

### Basic Operations

```python
# Write
df.write.format("delta").mode("append").saveAsTable("events")

# Read
spark.read.table("events")

# Time travel
spark.read.format("delta").option("versionAsOf", 0).load("/path/to/table")
```

## Common Patterns

### Pattern 1: Enable Advanced Features

```sql
-- Enable Liquid Clustering, DV, RLC
ALTER TABLE target_table 
SET TBLPROPERTIES (
  'delta.enableDeletionVectors' = true,
  'delta.enableRowLevelConcurrency' = true,
  'delta.liquid.clustering' = true
);
```

### Pattern 2: Streaming MERGE

```python
def upsert_to_delta(microbatch_df, batch_id):
    microbatch_df._jdf.sparkSession().sql("""
        MERGE INTO target_table t
        USING updates s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
```

## Available Skills

- [VACUUM Strategies](./vacuum-strategies.md) - Storage cleanup
- [Liquid Clustering](./liquid-clustering.md) - Auto-optimization
- [Streaming Merges](./streaming-merges.md) - Concurrent updates
- [Deletion Vectors](./deletion-vectors.md) - Soft deletes
- [Time Travel](./time-travel.md) - Historical queries
- [Schema Evolution](./schema-evolution.md) - Handle changes
- [CDC Patterns](./cdc-patterns.md) - Change data capture
