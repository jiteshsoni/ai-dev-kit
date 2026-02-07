---
name: "How Liquid, DV & RLC help improve Streaming Merges and P99 Latency"
description: "Use Liquid Clustering, Deletion Vectors, and Row-Level Concurrency to eliminate streaming merge conflicts and reduce P99 latency."
author: "Canadian Data Guy"
url: "https://www.youtube.com/watch?v=miM1B-D8Eaw"
date: "2025-11-29"
tags: ["spark-streaming", "liquid-clustering", "deletion-vectors", "row-level-concurrency", "merge", "performance"]
---

# Streaming Merges with Liquid, DV & RLC

## Overview

Modern Delta Lake features enable concurrent streaming merges and optimize operations without conflicts:
- **Liquid Clustering**: Auto-optimization without manual intervention
- **Deletion Vectors (DV)**: Soft deletes without file rewrite
- **Row-Level Concurrency (RLC)**: Concurrent updates to different rows in same file

**Result**: P99 latency reduction by eliminating optimize pauses.

## Quick Start

### Enable Required Features

```sql
-- Enable table features
ALTER TABLE target_table 
SET TBLPROPERTIES (
  'delta.enableDeletionVectors' = true,
  'delta.enableRowLevelConcurrency' = true,
  'delta.liquid.clustering' = true
);
```

### Streaming Merge Pattern

```python
# Before: Had to pause stream to run optimize
# Now: Stream continues, optimize runs in parallel

def upsert_to_delta(microbatch_df, batch_id):
    microbatch_df.createOrReplaceTempView("updates")
    
    microbatch_df._jdf.sparkSession().sql("""
        MERGE INTO target_table t
        USING updates s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# Streaming merge without optimize pauses
stream = (spark
    .readStream
    .table("source_stream")
    .writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", checkpoint_path)
    .start()
)

# Optimize runs separately via Predictive Optimization
# or manual: OPTIMIZE target_table
```

## Common Patterns

### Pattern 1: Before vs After

```python
# BEFORE: Old pattern with pauses
class OldPattern:
    def __init__(self):
        self.batch_count = 0
    
    def process(self, df, batch_id):
        # Run merge
        merge_data(df)
        
        # Pause stream every N batches to optimize
        self.batch_count += 1
        if self.batch_count % 100 == 0:
            spark.sql("OPTIMIZE target_table")  # PAUSES STREAM!
        # P99 latency spikes here

# AFTER: Modern pattern - no pauses needed
class ModernPattern:
    def process(self, df, batch_id):
        # Just run merge
        merge_data(df)
        # Optimize happens in parallel via Liquid + DV + RLC
        # No code needed, no latency spike
```

### Pattern 2: High-Update Workload

```python
# Scenario: 10% updates, 90% inserts
# Without DV: Full file rewrites for updates
# With DV: Mark rows deleted, write new file

# Configuration for high-update workloads
spark.sql("""
    CREATE TABLE high_update_table (
        id STRING,
        value STRING,
        updated_at TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = true,
        'delta.enableRowLevelConcurrency' = true
    )
""")
```

### Pattern 3: Concurrent Operations

```python
# With RLC, these can happen simultaneously:
# - Streaming merge updating row A
# - Optimize compacting file X
# - Another merge updating row B in same file X

# Without RLC: Operations conflict, one must retry/fail
# With RLC: Both proceed if updating different rows
```

## Reference Files

### Deletion Vectors Explained

```
Without DV:
┌─────────────┐     Update      ┌─────────────┐
│ File A.parquet│  ─────────→  │ File A'.parquet│
│ 1000 rows   │   (rewrite)    │ 1000 rows   │
└─────────────┘                └─────────────┘

With DV:
┌─────────────┐     Update      ┌─────────────┐
│ File A.parquet│  ─────────→  │ File A.parquet │
│ 1000 rows   │                │ 1000 rows   │
└─────────────┘                │ + DV file   │
                               │ (10 rows    │
                               │  marked del)│
                               └─────────────┘
```

### Row-Level Concurrency

```
File X: 4 rows
┌────┬───────────┬─────────┐
│ ID │ Row ID    │ Version │
├────┼───────────┼─────────┤
│ 0  │ row-uuid-1│ 1       │
│ 1  │ row-uuid-2│ 1       │
│ 2  │ row-uuid-3│ 1       │
│ 3  │ row-uuid-4│ 1       │
└────┴───────────┴─────────┘

Transaction 1: Update ID=0 → OK
Transaction 2: Update ID=1 → OK (different row)
Transaction 3: Update ID=0 → CONFLICT (same row)
```

## Common Issues

| Issue | Solution |
|-------|----------|
| **Conflicts still happening** | Verify RLC is enabled on table |
| **DV not being used** | Check table property; not all operations use DV |
| **Optimize still causes latency** | Enable Predictive Optimization for automatic scheduling |
| **Storage increase with DV** | DV are small; compact during optimize |

## Advanced Tips

### P99 Latency Optimization

```python
# P99 latency spikes occur when:
# 1. Stream pauses for optimize
# 2. Large files being rewritten
# 3. Conflict retries

# Solution: Liquid + DV + RLC eliminates all three

# Monitor P99:
# - Spark UI: Batch duration percentiles
# - Driver logs: "spark.streaming made progress" 
# - Custom metrics: Track end-to-end latency
```

### Validation Query

```sql
-- Verify table features are enabled
SHOW TBLPROPERTIES target_table;

-- Look for:
-- delta.enableDeletionVectors = true
-- delta.enableRowLevelConcurrency = true
```

### Predictive Optimization

```sql
-- Enable automatic optimization (Databricks)
ALTER TABLE target_table 
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = true,
  'delta.autoOptimize.autoCompact' = true
);

-- Now optimize happens automatically
-- No manual pauses needed
```

## FAQ

**Q: Do I need all three features (Liquid, DV, RLC)?**
A: For streaming merges with optimal latency: Yes. DV + RLC for conflicts, Liquid for clustering.

**Q: What's the overhead of DV?**
A: Minimal. DV files are small bitmaps. Read overhead is negligible.

**Q: Can I use this with existing tables?**
A: Yes, enable properties on existing tables. New writes use features.

**Q: Does this work with all merge patterns?**
A: Works with standard merge. Some complex conditions may not use RLC.

**Q: What's the latency improvement?**
A: Eliminates optimize pauses. P99 becomes more consistent, approaching P50.