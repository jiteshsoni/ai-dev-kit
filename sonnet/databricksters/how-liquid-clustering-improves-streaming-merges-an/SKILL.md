---
name: "Streaming Merges with Liquid Clustering and DV/RLC"
description: "Eliminate streaming merge conflicts and reduce P99 latency using Liquid Clustering, Deletion Vectors, and Row-Level Concurrency for concurrent operations."
author: "Canadian Data Guy"
url: "https://www.databricksters.com/p/how-liquid-dv-and-rlc-help-improve"
date: "2025-09-16"
tags: ["streaming", "liquid-clustering", "deletion-vectors", "row-level-concurrency", "merge", "performance", "databricks"]
---

# Streaming Merges with Liquid Clustering and DV/RLC

## Overview

Traditional streaming MERGE operations require pausing pipelines to run OPTIMIZE, causing P99 latency spikes. Liquid Clustering + Deletion Vectors + Row-Level Concurrency eliminate conflicts and enable concurrent streaming merges alongside OPTIMIZE operations. Result: consistent sub-second latency without manual pauses.

**Use this skill when:** Implementing streaming merges, experiencing merge conflicts, or needing to optimize streaming pipelines without pausing for maintenance.

## Quick Start

Enable the three critical features for conflict-free streaming merges:

```sql
-- Enable Liquid Clustering, Deletion Vectors, and Row-Level Concurrency
ALTER TABLE target_table 
SET TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowLevelConcurrency' = 'true'
);

ALTER TABLE target_table 
CLUSTER BY (customer_id, event_date);
```

```python
# Streaming merge without OPTIMIZE pauses
def upsert_to_delta(microbatch_df, batch_id):
    microbatch_df.createOrReplaceTempView("updates")
    
    microbatch_df._jdf.sparkSession().sql("""
        MERGE INTO target_table t
        USING updates s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# Stream continuously - OPTIMIZE runs in parallel
stream = (spark
    .readStream
    .table("source_stream")
    .writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", "/checkpoints/")
    .start()
)

# OPTIMIZE runs separately via Predictive Optimization
# No conflicts, no latency spikes!
```

## Common Patterns

### Pattern 1: Before vs After Architecture

Understanding the problem and solution:

```python
# ❌ OLD PATTERN: Manual pauses required
class OldStreamingMerge:
    def __init__(self):
        self.batch_count = 0
    
    def process(self, df, batch_id):
        # Run merge
        df.createOrReplaceTempView("updates")
        spark.sql("""
            MERGE INTO target_table t
            USING updates s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        # Pause every N batches to OPTIMIZE
        self.batch_count += 1
        if self.batch_count % 100 == 0:
            spark.sql("OPTIMIZE target_table")  # ⚠️ PAUSES STREAM!
            # P99 latency spikes here!

# ✅ NEW PATTERN: No pauses needed
class ModernStreamingMerge:
    def process(self, df, batch_id):
        # Just run merge - no pause logic needed
        df.createOrReplaceTempView("updates")
        spark.sql("""
            MERGE INTO target_table t
            USING updates s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        # Liquid + DV + RLC handles optimization automatically
        # P99 latency remains consistent!
```

**Result:** Eliminates optimize-induced latency spikes entirely.

### Pattern 2: How Deletion Vectors Optimize Merges

Deletion Vectors avoid full file rewrites for updates/deletes.

```python
# Without Deletion Vectors:
# UPDATE 10 rows → Rewrite entire 1000-row Parquet file
# High write amplification, slow merges

# With Deletion Vectors:
# UPDATE 10 rows → Mark 10 rows deleted, append new version
# Minimal write amplification, fast merges

# Visual representation:
"""
Without DV:
┌─────────────────┐   UPDATE   ┌─────────────────┐
│ File A.parquet  │  ────────→ │ File A'.parquet │
│ 1000 rows       │  (rewrite) │ 1000 rows       │
└─────────────────┘            └─────────────────┘

With DV:
┌─────────────────┐   UPDATE   ┌─────────────────┐
│ File A.parquet  │  ────────→ │ File A.parquet  │
│ 1000 rows       │            │ 1000 rows       │
└─────────────────┘            │ + DV file       │
                               │ (10 rows marked)│
                               └─────────────────┘
"""

# Enable on streaming target table
spark.sql("""
    ALTER TABLE streaming_target 
    SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
""")
```

**Impact:** Merge operations run 10-100x faster for high-update workloads.

### Pattern 3: Row-Level Concurrency Explained

RLC allows concurrent updates to different rows in the same file.

```python
# File X contains 4 rows with row IDs
"""
File X: 4 rows
┌────┬───────────────┬─────────┐
│ ID │ Row ID        │ Version │
├────┼───────────────┼─────────┤
│ 0  │ row-uuid-1    │ 1       │
│ 1  │ row-uuid-2    │ 1       │
│ 2  │ row-uuid-3    │ 1       │
│ 3  │ row-uuid-4    │ 1       │
└────┴───────────────┴─────────┘

Concurrent Operations:
Transaction 1: Update ID=0 → ✅ OK
Transaction 2: Update ID=1 → ✅ OK (different row)
Transaction 3: Update ID=0 → ❌ CONFLICT (same row)
Transaction 4: OPTIMIZE File X → ✅ OK (doesn't conflict with above)
"""

# Without RLC: All transactions conflict (file-level locking)
# With RLC: Only same-row updates conflict

# Verify RLC is enabled
spark.sql("SHOW TBLPROPERTIES target_table").filter(
    "key = 'delta.enableRowLevelConcurrency'"
).show()
```

**Result:** Streaming merges and OPTIMIZE can run simultaneously without conflicts.

### Pattern 4: Complete High-Update Streaming Pipeline

Full implementation for a workload with frequent updates:

```python
# Scenario: 10% updates, 90% inserts, ~1000 events/sec
spark.conf.set("spark.databricks.delta.merge.enableLowShuffle", "true")

# Create target table with all features
spark.sql("""
    CREATE TABLE IF NOT EXISTS high_update_target (
        id STRING,
        value STRING,
        updated_at TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.enableRowLevelConcurrency' = 'true'
    )
""")

# Enable Liquid Clustering
spark.sql("""
    ALTER TABLE high_update_target 
    CLUSTER BY (id)
""")

# Streaming merge with foreachBatch
def merge_updates(batch_df, batch_id):
    batch_df.createOrReplaceTempView("batch_updates")
    
    spark.sql("""
        MERGE INTO high_update_target t
        USING batch_updates s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET 
            t.value = s.value,
            t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT (id, value, updated_at)
        VALUES (s.id, s.value, s.updated_at)
    """)
    
    print(f"Batch {batch_id} merged successfully")

# Start streaming
stream = (spark.readStream
    .format("delta")
    .table("source_stream")
    .writeStream
    .foreachBatch(merge_updates)
    .option("checkpointLocation", "/checkpoints/high_update")
    .trigger(processingTime="10 seconds")
    .start()
)

# Separate job: Predictive Optimization handles OPTIMIZE automatically
# Or manual OPTIMIZE in parallel:
# spark.sql("OPTIMIZE high_update_target")
```

### Pattern 5: Monitor and Validate

Verify the features are working correctly:

```sql
-- Check table properties
SHOW TBLPROPERTIES target_table;

-- Verify Deletion Vectors are being used
DESCRIBE DETAIL target_table;
-- Look for: deletionVectorCount > 0

-- Check Row-Level Concurrency
SELECT * FROM target_table._metadata_log
WHERE operation = 'MERGE'
ORDER BY timestamp DESC
LIMIT 10;

-- Monitor merge performance
SELECT 
    operation,
    operationMetrics.numTargetRowsUpdated as updates,
    operationMetrics.numTargetRowsInserted as inserts,
    operationMetrics.executionTimeMs as duration_ms
FROM (
    DESCRIBE HISTORY target_table
)
WHERE operation = 'MERGE'
ORDER BY timestamp DESC;
```

## Reference Files

- [Deletion Vectors Documentation](https://docs.databricks.com/en/delta/deletion-vectors.html)
- [Row Tracking and RLC](https://docs.delta.io/delta-row-tracking/)
- [Row-Level Concurrency Deep Dive](https://www.databricks.com/blog/deep-dive-how-row-level-concurrency-works-out-box)
- [Liquid Clustering Guide](https://docs.databricks.com/en/delta/clustering.html)

## Common Issues

| Issue | Solution |
|-------|----------|
| **Still getting merge conflicts** | Verify RLC is enabled: `SHOW TBLPROPERTIES table`. Must be `true`. |
| **Deletion Vectors not being used** | Check table feature: `DESCRIBE DETAIL table`. Enable if missing. |
| **OPTIMIZE still causes latency spikes** | Enable Predictive Optimization for automatic scheduling. |
| **Storage increasing with DV** | DVs are small. Run periodic OPTIMIZE to compact. |
| **Merge slower than expected** | Enable Liquid Clustering on join keys for better file pruning. |
| **Row tracking overhead** | Normal 5-10% overhead. Benefits far outweigh costs for update workloads. |

## Advanced Tips

### Liquid Clustering for Merge Efficiency

```sql
-- Choose clustering keys based on MERGE join condition
-- If merging on: customer_id, event_date
ALTER TABLE target 
CLUSTER BY (customer_id, event_date);

-- Liquid ensures sorted data for efficient file pruning
-- Fewer files scanned = faster merges
```

### Enable Predictive Optimization

```sql
-- Automatic OPTIMIZE scheduling
ALTER TABLE target_table 
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Monitor PO activity
SELECT * FROM system.storage.predictive_optimization_history
WHERE table_name = 'catalog.schema.target_table'
ORDER BY operation_start_time DESC;
```

### P99 Latency Monitoring

```python
# Track batch durations over time
from pyspark.sql.functions import percentile_approx

# Collect batch metrics
batch_metrics = []

def merge_with_metrics(batch_df, batch_id):
    import time
    start = time.time()
    
    # Perform merge
    merge_updates(batch_df, batch_id)
    
    duration = time.time() - start
    batch_metrics.append(duration)
    
    # Calculate P99
    if len(batch_metrics) >= 100:
        sorted_metrics = sorted(batch_metrics)
        p99_idx = int(len(sorted_metrics) * 0.99)
        p99 = sorted_metrics[p99_idx]
        print(f"P99 latency: {p99:.2f}s")

# Usage
stream = (spark.readStream
    .table("source")
    .writeStream
    .foreachBatch(merge_with_metrics)
    .start()
)
```

## FAQ

**Q: Do I need all three features (Liquid, DV, RLC)?**  
A: For conflict-free streaming merges with optimal latency: Yes. DV + RLC handle conflicts, Liquid handles clustering efficiency.

**Q: What's the storage overhead of Deletion Vectors?**  
A: Minimal. DV files are small bitmaps (<1% of table size). Compacted during OPTIMIZE.

**Q: Can I enable on existing streaming tables?**  
A: Yes! Enable properties on existing tables. New writes immediately use the features.

**Q: Will this work with all merge patterns?**  
A: Works with standard MERGE operations. Complex multi-table updates may have limitations.

**Q: What's the typical latency improvement?**  
A: Eliminates OPTIMIZE pauses. P99 becomes consistent, often approaching P50 latency.

**Q: Does Predictive Optimization conflict with streaming?**  
A: No! PO is designed to run alongside streaming workloads without conflicts.
