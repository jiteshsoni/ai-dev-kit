---
name: table-operations
description: Table operations for streaming - MERGE performance, writing to multiple tables, and batch processing patterns.
tags: ["streaming", "merge", "delta", "multi-sink", "performance"]
---

# Table Operations

Operations for writing streaming data to Delta tables: MERGE performance, multi-table writes, and optimization patterns.

## Quick Decision Matrix

| Pattern | When to Use | Approach |
|---------|-------------|----------|
| [Single Table Write](#single-table-write) | Standard ingestion | Direct writeStream |
| [MERGE (Upsert)](#merge-upsert) | Update existing records | forEachBatch with MERGE |
| [Multi-Table Write](#multi-table-write) | Fan-out to multiple sinks | forEachBatch or multiple streams |
| [Parallel Writes](#parallel-merges) | High-throughput scenarios | Parallel foreachBatch |

---

## Single Table Write

### Basic Append

```python
(spark
    .readStream
    .format("kafka")
    .option("subscribe", "events")
    .load()
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/bronze")
    .trigger(processingTime="30 seconds")
    .start("/delta/bronze_events")
)
```

### With Idempotency

```python
def idempotent_write(batch_df, batch_id):
    """Exactly-once write with transaction metadata"""
    batch_df.write \
        .format("delta") \
        .option("txnVersion", batch_id) \
        .option("txnAppId", "stream_job_v1") \
        .mode("append") \
        .saveAsTable("bronze_events")

stream.writeStream \
    .foreachBatch(idempotent_write) \
    .option("checkpointLocation", "/checkpoints/bronze") \
    .start()
```

---

## MERGE (Upsert)

Update existing records or insert new ones using Delta MERGE.

### Basic MERGE Pattern

```python
def upsert_to_delta(batch_df, batch_id):
    """Merge streaming data into Delta table"""
    
    # Create temp view
    batch_df.createOrReplaceTempView("updates")
    
    # MERGE statement
    spark.sql("""
        MERGE INTO silver_events target
        USING updates source
        ON source.event_id = target.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

stream.writeStream \
    .foreachBatch(upsert_to_delta) \
    .option("checkpointLocation", "/checkpoints/silver") \
    .start()
```

### Conditional MERGE

```python
def conditional_upsert(batch_df, batch_id):
    """Merge with conditions"""
    
    batch_df.createOrReplaceTempView("updates")
    
    spark.sql("""
        MERGE INTO silver_events target
        USING updates source
        ON source.event_id = target.event_id
        
        -- Update only if source is newer
        WHEN MATCHED AND source.updated_at > target.updated_at THEN
            UPDATE SET *
        
        -- Insert new records
        WHEN NOT MATCHED THEN
            INSERT *
    """)
```

### SCD Type 2 MERGE

```python
def scd_type2_merge(batch_df, batch_id):
    """Slowly Changing Dimension Type 2"""
    
    batch_df.createOrReplaceTempView("updates")
    
    spark.sql("""
        MERGE INTO dim_customers target
        USING (
            SELECT updates.*, current_date() as effective_date
            FROM updates
        ) source
        ON source.customer_id = target.customer_id AND target.is_current = true
        
        -- Close existing record
        WHEN MATCHED THEN
            UPDATE SET is_current = false, end_date = current_date()
        
        -- Insert new record
        WHEN NOT MATCHED THEN
            INSERT (customer_id, name, email, effective_date, is_current)
            VALUES (source.customer_id, source.name, source.email, source.effective_date, true)
    """)
```

---

## Multi-Table Write

Write to multiple Delta tables from a single stream.

### Pattern 1: forEachBatch Multi-Write

```python
def write_to_multiple_tables(batch_df, batch_id):
    """Write to bronze, silver, and gold from single batch"""
    
    # Bronze: raw data
    (batch_df
        .write
        .mode("append")
        .format("delta")
        .saveAsTable("bronze_events"))
    
    # Silver: transformed
    silver_df = (batch_df
        .filter(col("event_type").isNotNull())
        .withColumn("processed_at", current_timestamp())
    )
    
    (silver_df
        .write
        .mode("append")
        .format("delta")
        .saveAsTable("silver_events"))
    
    # Gold: aggregated
    gold_df = (silver_df
        .groupBy("event_type")
        .agg(count("*").alias("event_count"))
    )
    
    (gold_df
        .write
        .mode("append")
        .format("delta")
        .saveAsTable("gold_metrics"))

stream.writeStream \
    .foreachBatch(write_to_multiple_tables) \
    .option("checkpointLocation", "/checkpoints/multi_write") \
    .start()
```

### Pattern 2: Conditional Multi-Write

```python
def route_and_write(batch_df, batch_id):
    """Route events to different tables based on criteria"""
    
    # High priority events
    high_priority = batch_df.filter(col("priority") == "high")
    if high_priority.count() > 0:
        (high_priority
            .write
            .mode("append")
            .format("delta")
            .saveAsTable("urgent_events"))
    
    # Standard events
    standard = batch_df.filter(col("priority") == "standard")
    if standard.count() > 0:
        (standard
            .write
            .mode("append")
            .format("delta")
            .saveAsTable("standard_events"))
    
    # All events to archive
    (batch_df
        .write
        .mode("append")
        .format("delta")
        .saveAsTable("all_events_archive"))
```

### Pattern 3: Fan-Out to Different Systems

```python
def multi_system_write(batch_df, batch_id):
    """Write to Delta, Kafka, and external database"""
    
    # Delta Lake
    (batch_df
        .write
        .mode("append")
        .format("delta")
        .saveAsTable("events"))
    
    # Kafka
    (batch_df
        .select(col("event_id").alias("key"), to_json(struct("*")).alias("value"))
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("topic", "processed-events")
        .save())
    
    # JDBC (for external system)
    (batch_df
        .write
        .mode("append")
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "events")
        .save())
```

---

## Parallel Merges

Process multiple tables in parallel for high-throughput scenarios.

### Parallel Table Processing

```python
from concurrent.futures import ThreadPoolExecutor
import functools

def process_single_table(args):
    """Process one table"""
    table_name, batch_df = args
    
    (batch_df
        .write
        .mode("append")
        .format("delta")
        .saveAsTable(table_name))
    
    return f"Processed {table_name}"

def parallel_multi_write(batch_df, batch_id):
    """Process multiple tables in parallel"""
    
    tables = ["events", "metrics", "alerts"]
    
    # Prepare data for each table
    table_data = [
        ("events", batch_df),
        ("metrics", batch_df.select("timestamp", "metric_value")),
        ("alerts", batch_df.filter(col("severity") == "high"))
    ]
    
    # Process in parallel
    with ThreadPoolExecutor(max_workers=3) as executor:
        results = list(executor.map(process_single_table, table_data))
    
    print(f"Batch {batch_id}: {results}")

stream.writeStream \
    .foreachBatch(parallel_multi_write) \
    .option("checkpointLocation", "/checkpoints/parallel") \
    .start()
```

### Parallel MERGE Operations

```python
def parallel_merge(batch_df, batch_id):
    """Run multiple MERGEs in parallel"""
    
    # Split batch by partition key
    partitions = batch_df.select("region").distinct().collect()
    
    def merge_region(region_row):
        region = region_row.region
        region_df = batch_df.filter(col("region") == region)
        
        region_df.createOrReplaceTempView(f"updates_{region}")
        
        spark.sql(f"""
            MERGE INTO silver_events target
            USING updates_{region} source
            ON source.event_id = target.event_id AND target.region = '{region}'
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        return f"Merged region {region}"
    
    # Process regions in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(merge_region, partitions))
```

---

## Merge Performance Optimization

### Optimize MERGE Performance

```python
# 1. Partition target table on merge key
spark.sql("""
    CREATE TABLE silver_events (
        event_id STRING,
        event_type STRING,
        timestamp TIMESTAMP
    ) USING DELTA
    PARTITIONED BY (event_type)
""")

# 2. Z-ORDER on merge key for non-partitioned columns
spark.sql("OPTIMIZE silver_events ZORDER BY (event_id)")

# 3. Use partition pruning in MERGE
spark.sql("""
    MERGE INTO silver_events target
    USING updates source
    ON source.event_id = target.event_id 
       AND source.event_type = target.event_type  -- Enables pruning
    WHEN NOT MATCHED THEN INSERT *
""")
```

### Batch Size Control

```python
# Control microbatch size for optimal MERGE performance
(spark
    .readStream
    .format("kafka")
    .option("subscribe", "events")
    .option("maxOffsetsPerTrigger", 10000)  # Limit batch size
    .load()
    .writeStream
    .foreachBatch(upsert_to_delta)
    .trigger(processingTime="30 seconds")
    .start()
)
```

### Liquid Clustering for MERGE

```python
# Enable Liquid Clustering on target table
spark.sql("""
    CREATE TABLE silver_events (
        event_id STRING,
        event_type STRING,
        timestamp TIMESTAMP
    ) USING DELTA
    CLUSTER BY (event_id)
""")

# Automatic clustering optimizes MERGE performance
```

---

## Best Practices

### Write Optimization
- Use `foreachBatch` for complex operations (MERGE, multi-write)
- Control batch size with `maxOffsetsPerTrigger`
- Partition target tables on frequently-filtered columns
- Enable Liquid Clustering for high-cardinality merge keys

### MERGE Optimization
- Include partition columns in MERGE condition for pruning
- Z-ORDER on high-cardinality columns
- Use idempotent writes with `txnVersion`/`txnAppId`
- Monitor and optimize file sizes with `OPTIMIZE`

### Multi-Write Patterns
- Use ThreadPoolExecutor for parallel independent writes
- Handle errors gracefully to avoid stream failure
- Consider separate checkpoints for independent sinks
- Monitor throughput and adjust parallelism

---

## Troubleshooting

| Issue | Symptom | Solution |
|-------|---------|----------|
| Slow MERGE | High latency | Add partition pruning, Z-ORDER, Liquid Clustering |
| Small files | Many tiny files | Adjust trigger interval, use `maxOffsetsPerTrigger` |
| Write conflicts | Concurrent modification errors | Ensure idempotent writes, check transaction isolation |
| Memory issues | OOM during multi-write | Reduce batch size, serialize operations |
| Skewed writes | Uneven processing | Repartition by key before write |
