---
name: "Harnessing Spark Streaming: Introduction to the ForEachBatch Function"
description: "Use ForEachBatch to write to any target, perform custom logic, and migrate from batch to streaming incrementally."
author: "Canadian Data Guy"
url: "https://www.youtube.com/watch?v=gur1oLe5t2o"
date: "2025-11-29"
tags: ["spark-streaming", "foreachbatch", "custom-sink", "migration", "batch-to-streaming"]
---

# Introduction to ForEachBatch

## Overview

`forEachBatch` is a powerful Spark Streaming feature that enables:
- Writing to targets not natively supported (PostgreSQL, SQL Server, etc.)
- Custom processing logic per microbatch
- Incremental migration from batch to streaming

**Key Insight**: Process each microbatch as a static DataFrame with full Spark API access.

## Quick Start

### Basic ForEachBatch

```python
def process_microbatch(df, batch_id):
    """
    df: Microbatch DataFrame (static)
    batch_id: Monotonically increasing batch identifier
    """
    # df is just a regular DataFrame
    count = df.count()
    print(f"Batch {batch_id}: {count} rows")
    
    # Write to any target
    (df
        .write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "target_table")
        .mode("append")
        .save()
    )

# Apply to stream
stream = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "events")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .writeStream
    .foreachBatch(process_microbatch)
    .option("checkpointLocation", checkpoint_path)
    .start()
)
```

### Supported Targets

```python
# With forEachBatch, you can write to:
# - PostgreSQL
# - SQL Server
# - MongoDB
# - Elasticsearch
# - Custom APIs
# - Multiple targets in parallel

def write_to_multiple_targets(df, batch_id):
    # Write to Postgres
    df.write.jdbc(url, "table1", mode="append")
    
    # Write to Delta
    df.write.format("delta").mode("append").save("/delta/table")
    
    # Call custom API
    send_to_api(df.toPandas())  # Note: toPandas() for small data only
```

## Common Patterns

### Pattern 1: Batch-to-Streaming Migration

```python
# BEFORE: Batch job
def batch_process():
    df = spark.table("source")
    transformed = complex_transformations(df)
    transformed.write.saveAsTable("target")

# AFTER: Streaming job (minimal changes)
def stream_process():
    (spark
        .readStream
        .table("source")
        .writeStream
        .foreachBatch(lambda df, id: complex_transformations(df).write.saveAsTable("target"))
        .option("checkpointLocation", checkpoint_path)
        .start()
    )

# Same transformation logic, just wrapped in forEachBatch
```

### Pattern 2: Custom Upsert Logic

```python
def upsert_microbatch(df, batch_id):
    """Perform merge/upsert for each microbatch"""
    df.createOrReplaceTempView("updates")
    
    df._jdf.sparkSession().sql("""
        MERGE INTO target_table t
        USING updates s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

stream = (spark
    .readStream
    .format("delta")
    .load("/source")
    .writeStream
    .foreachBatch(upsert_microbatch)
    .option("checkpointLocation", checkpoint_path)
    .start()
)
```

### Pattern 3: Parameterized Processing

```python
class ForEachBatchProcessor:
    """Class-based approach for parameterized processing"""
    
    def __init__(self, catalog, database, target_tables):
        self.catalog = catalog
        self.database = database
        self.targets = target_tables
    
    def process(self, df, batch_id):
        # Access parameters via self
        for table in self.targets:
            target_path = f"{self.catalog}.{self.database}.{table}"
            (df.filter(col("type") == table)
               .write
               .format("delta")
               .mode("append")
               .saveAsTable(target_path)
            )

# Usage
processor = ForEachBatchProcessor("prod", "analytics", ["a", "b", "c"])
stream = (df
    .writeStream
    .foreachBatch(processor.process)
    .start()
)
```

## Reference Files

### ForEachBatch Function Signature

```python
def process_microbatch(
    df: DataFrame,      # Microbatch as static DataFrame
    batch_id: int       # Monotonically increasing ID
) -> None:
    """
    Process one microbatch.
    
    Called for each trigger interval with:
    - df: All data in microbatch
    - batch_id: Unique identifier (0, 1, 2, ...)
    """
    pass
```

### Microbatch DataFrame

```python
# Inside forEachBatch, df is a STATIC DataFrame
# You can use any DataFrame operation:

def process(df, batch_id):
    # All DataFrame operations work
    df.filter(...).groupBy(...).agg(...)
    df.join(other_table, ...)
    df.write.format(...)
    
    # NOT available (these are streaming-only):
    # df.writeStream  <-- Can't use inside forEachBatch!
```

## Common Issues

| Issue | Solution |
|-------|----------|
| **"writeStream not available"** | Inside forEachBatch, use `write` (static), not `writeStream` |
| **toPandas() on large data** | Only use for small lookup tables; causes OOM on big data |
| **Slow performance** | Cache df if multiple actions; enable parallel writes |
| **Duplicate data** | Use Delta merge with txnVersion/batch_id for idempotency |
| **External system overload** | Rate limit writes; use connection pooling |

## Advanced Tips

### Idempotent Writes

```python
def idempotent_write(df, batch_id):
    """Ensure exactly-once even if batch reprocessed"""
    (df
        .write
        .format("delta")
        .option("txnVersion", batch_id)      # Idempotency key
        .option("txnAppId", "my_stream_job")  # Job identifier
        .mode("append")
        .saveAsTable("target")
    )
```

### Caching Strategy

```python
def process_with_cache(df, batch_id):
    # Cache if multiple actions
    df.cache()
    
    # Action 1: Write to Delta
    df.write.format("delta").saveAsTable("target1")
    
    # Action 2: Write to JDBC
    df.write.jdbc(url, "table2")
    
    # Action 3: Aggregate and log
    metrics = df.groupBy("status").count().collect()
    log_metrics(metrics)
    
    # Release memory
    df.unpersist()
```

### Error Handling

```python
from pyspark.sql.utils import StreamingQueryException

def robust_process(df, batch_id):
    try:
        # Main processing
        write_to_target(df)
    except Exception as e:
        # Log error details
        log.error(f"Batch {batch_id} failed: {e}")
        # Re-raise to fail the stream
        raise
```

## FAQ

**Q: When should I use forEachBatch vs native streaming sinks?**
A: Use forEachBatch when: (1) Target not natively supported, (2) Need custom logic, (3) Migrating batch code.

**Q: Can I use SQL inside forEachBatch?**
A: Yes. Create temp view and use `df.sparkSession.sql("...")`.

**Q: What's the performance impact?**
A: Minimal overhead. Main cost is your processing logic. Optimize like batch jobs.

**Q: Can I access SparkSession inside forEachBatch?**
A: Yes, via `df.sparkSession`.

**Q: How do I handle very small microbatches?**
A: Use `trigger(processingTime="5 minutes")` or `maxOffsetsPerTrigger` to control size.