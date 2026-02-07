---
name: streaming-multi-table-writes
description: Write a single Spark stream to multiple Delta tables using foreachBatch. Use when fanning out streaming data to multiple sinks, implementing CDC patterns, or creating materialized views from a single stream.
---

# Streaming Multi-Table Writes

## Overview

A common streaming pattern requires writing incoming data to multiple destinations - perhaps raw events to a bronze table, filtered alerts to a monitoring table, and aggregates to a summary table. The `foreachBatch` pattern enables this multi-sink architecture.

## Basic Multi-Table Pattern

```python
from pyspark.sql.functions import col, when, count, window

def write_to_multiple_tables(df, batch_id):
    """
    Write a single microbatch to multiple Delta tables.
    All writes participate in the same transaction epoch.
    """
    # Table 1: Raw events (all data)
    (df.write
        .format("delta")
        .option("txnAppId", "multi_sink_job")
        .option("txnVersion", batch_id)
        .mode("append")
        .saveAsTable("bronze.raw_events"))
    
    # Table 2: Error events only
    error_df = df.filter(col("level") == "ERROR")
    (error_df.write
        .format("delta")
        .option("txnAppId", "multi_sink_job_errors")
        .option("txnVersion", batch_id)
        .mode("append")
        .saveAsTable("silver.error_events"))
    
    # Table 3: High-value transactions
    high_value_df = df.filter(col("amount") > 10000)
    (high_value_df.write
        .format("delta")
        .option("txnAppId", "multi_sink_job_highvalue")
        .option("txnVersion", batch_id)
        .mode("append")
        .saveAsTable("silver.high_value_txns"))

# Start the stream
(spark
    .readStream
    .format("kafka")
    .option("subscribe", "events")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .writeStream
    .foreachBatch(write_to_multiple_tables)
    .option("checkpointLocation", "/checkpoints/multi_table")
    .start()
)
```

## Transactional Guarantees

Each foreachBatch call represents one epoch. All writes within the batch:
- See the same input data
- Share the same batch_id
- Are idempotent if using txnVersion

```python
def transactional_multi_write(df, batch_id):
    """
    All writes use the same batch_id for exactly-once semantics.
    If the batch fails halfway, Spark will retry with the same batch_id.
    Delta's idempotent writes ensure no duplicates.
    """
    # All three writes use their own txnAppId but same batch_id
    # This allows independent retries while maintaining exactly-once
    
    df.write.format("delta").option("txnVersion", batch_id).save("/delta/table1")
    df.filter(...).write.format("delta").option("txnVersion", batch_id).save("/delta/table2")
    df.filter(...).write.format("delta").option("txnVersion", batch_id).save("/delta/table3")
```

## Conditional Routing

```python
def route_by_event_type(df, batch_id):
    """Route events to different tables based on type."""
    
    event_types = df.select("event_type").distinct().collect()
    
    for row in event_types:
        event_type = row.event_type
        filtered_df = df.filter(col("event_type") == event_type)
        
        # Route to type-specific table
        (filtered_df.write
            .format("delta")
            .option("txnAppId", f"router_{event_type}")
            .option("txnVersion", batch_id)
            .mode("append")
            .save(f"/delta/events/{event_type}"))
```

## CDC Pattern: Apply Changes to Multiple Targets

```python
def cdc_multi_target_apply(df, batch_id):
    """
    Apply CDC changes to both a current state table and an audit log.
    """
    from delta.tables import DeltaTable
    
    # Split by operation type
    deletes = df.filter(col("_op") == "DELETE")
    upserts = df.filter(col("_op").isin(["INSERT", "UPDATE"]))
    
    # Target 1: Current state table with MERGE
    target_table = DeltaTable.forName(spark, "silver.customers")
    
    (target_table.alias("target")
        .merge(upserts.alias("source"), "target.customer_id = source.customer_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    
    # Actually delete (soft-delete pattern could also be used)
    (target_table.alias("target")
        .merge(deletes.alias("source"), "target.customer_id = source.customer_id")
        .whenMatchedDelete()
        .execute())
    
    # Target 2: Audit log (append all changes)
    audit_df = (df
        .withColumn("_processed_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id)))
    
    (audit_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("audit.customer_changes"))
```

## Parallel Writes Optimization

```python
from concurrent.futures import ThreadPoolExecutor

def parallel_write_to_tables(df, batch_id):
    """
    Write to independent tables in parallel.
    Use when tables have no dependencies.
    """
    def write_table(args):
        table_name, filter_expr = args
        filtered_df = df.filter(filter_expr) if filter_expr else df
        (filtered_df.write
            .format("delta")
            .option("txnAppId", f"parallel_{table_name}")
            .option("txnVersion", batch_id)
            .mode("append")
            .saveAsTable(table_name))
        return f"Wrote {table_name}"
    
    tables = [
        ("bronze.all_events", None),
        ("silver.errors", col("level") == "ERROR"),
        ("silver.warnings", col("level") == "WARN"),
        ("gold.metrics", col("type") == "metric")
    ]
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(write_table, tables))
    
    return results
```

## Materialized Views Pattern

```python
def create_materialized_views(df, batch_id):
    """
    Create multiple derived views from the same stream.
    """
    # Base: All events
    df.write.format("delta").option("txnVersion", batch_id).save("/delta/views/raw")
    
    # View 1: Hourly aggregations
    hourly = (df
        .withWatermark("event_time", "1 hour")
        .groupBy(window(col("event_time"), "1 hour"), col("category"))
        .agg(count("*").alias("event_count"), sum("value").alias("total_value")))
    
    hourly.write.format("delta").option("txnVersion", batch_id).save("/delta/views/hourly")
    
    # View 2: User sessions (last 15 min window)
    sessions = (df
        .withWatermark("event_time", "15 minutes")
        .groupBy(window(col("event_time"), "15 minutes"), col("user_id"))
        .agg(count("*").alias("actions")))
    
    sessions.write.format("delta").option("txnVersion", batch_id).save("/delta/views/sessions")
```

## Error Handling and Dead Letter Queue

```python
def write_with_dlq(df, batch_id):
    """
    Write valid records to target, invalid to dead letter queue.
    """
    # Validation
    valid = df.filter(col("required_field").isNotNull() & col("timestamp").isNotNull())
    invalid = df.filter(col("required_field").isNull() | col("timestamp").isNull())
    
    # Write valid data
    (valid.write
        .format("delta")
        .option("txnVersion", batch_id)
        .mode("append")
        .saveAsTable("silver.valid_events"))
    
    # Write invalid to DLQ with metadata
    if invalid.count() > 0:
        dlq_df = (invalid
            .withColumn("_error_reason", 
                when(col("required_field").isNull(), "missing_required_field")
                .otherwise("missing_timestamp"))
            .withColumn("_batch_id", lit(batch_id))
            .withColumn("_processed_at", current_timestamp()))
        
        (dlq_df.write
            .format("delta")
            .mode("append")
            .saveAsTable("errors.dead_letter_queue"))
```

## Best Practices

### 1. Idempotency is Critical

Always use `txnVersion` with `batch_id` to ensure exactly-once writes:

```python
.write.option("txnAppId", "my_job").option("txnVersion", batch_id)
```

### 2. Keep Batch Processing Fast

```python
# Avoid expensive operations in foreachBatch
def efficient_write(df, batch_id):
    # GOOD: Simple filters and writes
    df.filter(...).write.save("/delta/table1")
    df.filter(...).write.save("/delta/table2")
    
    # BAD: Expensive aggregations that should be in the stream definition
    df.groupBy(...).agg(...).write.save("/delta/table3")  # Move to stream!
```

### 3. Monitor Each Sink

```python
def monitored_multi_write(df, batch_id):
    start = time.time()
    
    # Write 1
    t1_start = time.time()
    df.write.save("/delta/table1")
    print(f"Table1: {time.time() - t1_start}s, rows: {df.count()}")
    
    # Write 2
    t2_start = time.time()
    df2 = df.filter(...)
    df2.write.save("/delta/table2")
    print(f"Table2: {time.time() - t2_start}s, rows: {df2.count()}")
```

### 4. Handle Schema Evolution

```python
def schema_aware_write(df, batch_id):
    """Handle schemas that may evolve over time."""
    try:
        (df.write
            .format("delta")
            .option("mergeSchema", "true")
            .option("txnVersion", batch_id)
            .mode("append")
            .save("/delta/table"))
    except AnalysisException as e:
        # Log schema mismatch for investigation
        print(f"Schema error in batch {batch_id}: {e}")
        # Write to fallback location
        df.write.json(f"/fallback/batch_{batch_id}")
```

## Common Patterns Summary

| Pattern | Use Case | txnAppId Strategy |
|---------|----------|-------------------|
| Fan-out | Same data to multiple tables | Unique per table |
| Routing | Different data to different tables | Include route key |
| CDC | Apply changes to targets | Same for all targets |
| Parallel | Independent writes | Unique per table |
| DLQ | Separate invalid records | Dedicated DLQ id |

## Checkpoint Considerations

```python
# Use a descriptive checkpoint location
checkpoint_path = "/checkpoints/multi_table/enrichment_pipeline_v1"

# Never share checkpoints between different multi-table queries
# Each query has its own checkpoint
```
