---
name: foreachbatch-patterns
description: "Advanced ForEachBatch patterns for multi-table writes, custom logic, and complex streaming operations."
author: "Canadian Data Guy"
source: "https://www.youtube.com/watch?v=XXXXXXXXXXX"
tags: ["spark-streaming", "foreachbatch", "multi-table", "transactions"]
---

# ForEachBatch Patterns

## Overview

ForEachBatch enables custom logic on each microbatch, including multiple writes and complex transformations.

## Quick Start

```python
def custom_logic(microbatch_df, batch_id):
    """Process each microbatch"""
    # Custom transformations
    processed = microbatch_df.filter(col("status") == "active")
    
    # Write to Delta
    processed.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("processed_events")

stream.writeStream \
    .foreachBatch(custom_logic) \
    .option("checkpointLocation", "/checkpoints/custom") \
    .start()
```

## Common Patterns

### Pattern 1: Multi-Table Writes

```python
def write_to_multiple_tables(df, batch_id):
    """Write to silver and gold in one batch"""
    
    # Raw to Silver
    df.write.format("delta").mode("append").saveAsTable("silver.events")
    
    # Aggregated to Gold
    summary = df.groupBy("category").count()
    summary.write.format("delta").mode("overwrite").saveAsTable("gold.summary")
```

### Pattern 2: MERGE in ForEachBatch

```python
def upsert_to_delta(microbatch_df, batch_id):
    """Upsert using MERGE"""
    microbatch_df.createOrReplaceTempView("updates")
    
    microbatch_df._jdf.sparkSession().sql("""
        MERGE INTO target_table t
        USING updates s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
```

### Pattern 3: Idempotent Writes

```python
def idempotent_write(df, batch_id):
    """Exactly-once with transaction IDs"""
    df.write \
        .format("delta") \
        .option("txnAppId", "my_stream") \
        .option("txnVersion", batch_id) \
        .mode("append") \
        .save()
```
