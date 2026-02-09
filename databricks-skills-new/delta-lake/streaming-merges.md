---
name: streaming-merges
description: "Concurrent streaming merges with Deletion Vectors and Row-Level Concurrency for P99 latency optimization."
author: "Canadian Data Guy"
source: "https://www.youtube.com/watch?v=miM1B-D8Eaw"
tags: ["delta", "streaming", "merge", "deletion-vectors", "row-level-concurrency"]
---

# Streaming Merges with DV & RLC

## Overview

Enable concurrent streaming merges and eliminate optimize pauses using:
- **Liquid Clustering**: Incremental optimization
- **Deletion Vectors (DV)**: Soft deletes without rewrite
- **Row-Level Concurrency (RLC)**: Concurrent row updates

## Quick Start

```sql
-- Enable required features
ALTER TABLE target_table 
SET TBLPROPERTIES (
  'delta.enableDeletionVectors' = true,
  'delta.enableRowLevelConcurrency' = true,
  'delta.liquid.clustering' = true
);
```

```python
# Streaming merge without pauses
def upsert_to_delta(microbatch_df, batch_id):
    microbatch_df._jdf.sparkSession().sql("""
        MERGE INTO target_table t
        USING updates s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

stream = (spark
    .readStream
    .table("source_stream")
    .writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", checkpoint_path)
    .start()
)
# Optimize runs separately - no stream pause needed
```

## How It Works

### Deletion Vectors
```
Without DV: File rewrite on update
With DV: Mark rows deleted + write new file
```

### Row-Level Concurrency
```
Transaction 1: Update row A → OK
Transaction 2: Update row B → OK (different row)
Transaction 3: Update row A → CONFLICT
```

## Common Patterns

### Pattern 1: Before vs After

```python
# BEFORE: Had to pause for optimize
if batch_count % 100 == 0:
    spark.sql("OPTIMIZE target_table")  # PAUSES STREAM!

# AFTER: No pauses needed
# Optimize happens in parallel via Liquid + DV + RLC
```

### Pattern 2: Validation

```sql
-- Verify features enabled
SHOW TBLPROPERTIES target_table;
-- Look for:
-- delta.enableDeletionVectors = true
-- delta.enableRowLevelConcurrency = true
```
