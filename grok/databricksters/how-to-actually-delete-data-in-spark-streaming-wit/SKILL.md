---
name: "spark-streaming-deletes-gdpr"
description: "Complete guide to handling deletes in Spark Structured Streaming for GDPR compliance, including deletion vectors, REORG, and Change Data Feed patterns."
---

# Spark Streaming Deletes: GDPR-Compliant Data Management

## Overview

This skill covers implementing delete operations in Spark Structured Streaming pipelines for GDPR and compliance requirements. Learn about Delta Lake's DELETE capabilities, deletion vectors, REORG operations, Change Data Feed patterns, and how to propagate deletes across Bronze, Silver, and Gold layers while maintaining streaming pipeline integrity.

## Quick Start

### Enable Change Data Feed for Delete Tracking
Track deletes through your streaming pipeline:

```sql
-- Enable Change Data Feed on source table
ALTER TABLE bronze.user_data SET TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
);

-- Create streaming table that handles deletes
CREATE STREAMING TABLE silver.user_data_clean
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
AS SELECT * FROM STREAM(bronze.user_data);
```

### Handle Deletes in Streaming Pipeline
Process deletes using Change Data Feed:

```python
def create_delete_aware_streaming_pipeline():
    """Create streaming pipeline that handles deletes"""
    
    # Read with Change Data Feed enabled
    change_feed_df = spark.readStream \
        .format("delta") \
        .option("readChangeFeed", "true") \
        .table("bronze.user_data")
    
    # Filter for different change types
    inserts = change_feed_df.filter("_change_type = 'insert'")
    updates = change_feed_df.filter("_change_type = 'update_postimage'")
    deletes = change_feed_df.filter("_change_type = 'delete'")
    
    # Process deletes separately
    def process_changes(batch_df, batch_id):
        # Handle inserts/updates
        inserts_updates = batch_df.filter(
            col("_change_type").isin(["insert", "update_postimage"])
        ).drop("_change_type")
        
        if inserts_updates.count() > 0:
            inserts_updates.write.format("delta").mode("append").saveAsTable("silver.user_data_clean")
        
        # Handle deletes
        deletes_batch = batch_df.filter("_change_type = 'delete'")
        if deletes_batch.count() > 0:
            # Delete from target table
            delete_ids = [row['user_id'] for row in deletes_batch.select("user_id").collect()]
            
            for user_id in delete_ids:
                spark.sql(f"""
                    DELETE FROM silver.user_data_clean
                    WHERE user_id = '{user_id}'
                """)
    
    # Write streaming changes
    query = change_feed_df.writeStream \
        .foreachBatch(process_changes) \
        .option("checkpointLocation", "/tmp/checkpoint/deletes") \
        .start()
    
    return query
```

## Common Patterns

### Pattern 1: GDPR Right-to-Be-Forgotten Implementation
Complete GDPR deletion workflow:

```python
def gdpr_delete_workflow(user_email):
    """Complete GDPR deletion workflow"""
    
    # Step 1: Delete from Bronze layer
    spark.sql(f"""
        DELETE FROM bronze.user_data
        WHERE email = '{user_email}'
    """)
    
    # Step 2: Delete from Silver layer
    spark.sql(f"""
        DELETE FROM silver.user_data_enriched
        WHERE email = '{user_email}'
    """)
    
    # Step 3: Delete from Gold layer
    spark.sql(f"""
        DELETE FROM gold.user_analytics
        WHERE email = '{user_email}'
    """)
    
    # Step 4: Physically purge deleted rows (REORG)
    for table in ["bronze.user_data", "silver.user_data_enriched", "gold.user_analytics"]:
        spark.sql(f"""
            REORG TABLE {table} APPLY (PURGE)
        """)
    
    # Step 5: Vacuum old files (after retention period)
    spark.sql("""
        VACUUM bronze.user_data RETAIN 168 HOURS
    """)
    
    print(f"GDPR deletion completed for {user_email}")

# Usage
gdpr_delete_workflow("user@example.com")
```

### Pattern 2: Append-Only Tables with Skip Change Commits
Handle deletes by ignoring them in append-only pipelines:

```python
def append_only_streaming_with_skip_changes():
    """Streaming pipeline that ignores deletes/updates"""
    
    # Create append-only table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.events_append_only (
            event_id STRING,
            user_id STRING,
            event_type STRING,
            timestamp TIMESTAMP
        ) USING DELTA
        TBLPROPERTIES (
            'delta.appendOnly' = 'true'
        )
    """)
    
    # Read stream ignoring DML operations
    append_only_stream = spark.readStream \
        .format("delta") \
        .option("skipChangeCommits", "true") \
        .table("bronze.events_append_only")
    
    # Process only inserts
    query = append_only_stream.writeStream \
        .format("delta") \
        .option("checkpointLocation", "/tmp/checkpoint/append_only") \
        .table("silver.events_processed")
    
    return query

# Note: If delta.appendOnly = true, DELETE operations will fail
# Use skipChangeCommits to ignore DML in downstream processing
```

### Pattern 3: Streaming Delete Propagation
Propagate deletes through multiple streaming layers:

```python
def propagate_deletes_through_layers():
    """Propagate deletes from Bronze to Silver to Gold"""
    
    # Bronze to Silver propagation
    def bronze_to_silver(batch_df, batch_id):
        # Check for deletes
        if "_change_type" in batch_df.columns:
            deletes = batch_df.filter("_change_type = 'delete'")
            
            if deletes.count() > 0:
                # Extract keys to delete
                delete_keys = deletes.select("user_id", "event_id").collect()
                
                # Delete from Silver
                for row in delete_keys:
                    spark.sql(f"""
                        DELETE FROM silver.events_enriched
                        WHERE user_id = '{row['user_id']}'
                          AND event_id = '{row['event_id']}'
                    """)
            
            # Process inserts/updates
            inserts_updates = batch_df.filter(
                col("_change_type").isin(["insert", "update_postimage"])
            ).drop("_change_type")
            
            if inserts_updates.count() > 0:
                inserts_updates.write.format("delta").mode("append").saveAsTable("silver.events_enriched")
    
    # Silver to Gold propagation
    def silver_to_gold(batch_df, batch_id):
        if "_change_type" in batch_df.columns:
            deletes = batch_df.filter("_change_type = 'delete'")
            
            if deletes.count() > 0:
                delete_keys = deletes.select("user_id").distinct().collect()
                
                for row in delete_keys:
                    spark.sql(f"""
                        DELETE FROM gold.user_summary
                        WHERE user_id = '{row['user_id']}'
                    """)
    
    # Set up streaming queries
    bronze_stream = spark.readStream \
        .format("delta") \
        .option("readChangeFeed", "true") \
        .table("bronze.events") \
        .writeStream \
        .foreachBatch(bronze_to_silver) \
        .option("checkpointLocation", "/tmp/checkpoint/bronze_silver") \
        .start()
    
    silver_stream = spark.readStream \
        .format("delta") \
        .option("readChangeFeed", "true") \
        .table("silver.events_enriched") \
        .writeStream \
        .foreachBatch(silver_to_gold) \
        .option("checkpointLocation", "/tmp/checkpoint/silver_gold") \
        .start()
    
    return bronze_stream, silver_stream
```

## Reference Files

- [Delta Lake VACUUM](https://docs.databricks.com/en/delta/vacuum.html) - File cleanup operations
- [Delta Lake REORG](https://docs.databricks.com/en/delta/reorg-table.html) - Physical deletion application
- [Deletion Vectors](https://docs.databricks.com/en/delta/deletion-vectors.html) - Efficient delete tracking
- [Change Data Feed](https://docs.databricks.com/en/delta/delta-change-data-feed.html) - Change tracking
- [Row-Level Concurrency](https://docs.databricks.com/en/optimizations/isolation-level.html) - Concurrent operations

## Common Issues

| Issue | Solution |
|-------|----------|
| **DELETE fails on append-only table** | Remove `delta.appendOnly = true` or use `skipChangeCommits` in downstream |
| **Deletes not propagating** | Enable Change Data Feed and use `readChangeFeed = true` |
| **Storage costs growing** | Run REORG APPLY (PURGE) and VACUUM regularly |
| **Concurrent delete conflicts** | Enable row-level concurrency for safe parallel operations |
| **Time travel broken after VACUUM** | Set appropriate retention periods (7-21 days) |

## Key Takeaways

1. **Change Data Feed** - Enables tracking deletes through streaming pipelines
2. **Deletion Vectors** - Logical deletes without immediate file rewrites
3. **REORG APPLY (PURGE)** - Physically removes deleted rows from files
4. **VACUUM** - Removes old files after retention period
5. **Append-Only Tables** - Use `delta.appendOnly = true` to prevent DML operations
6. **Retention Strategy** - Balance compliance (7 days) with recovery needs (21 days)

## GDPR Compliance Checklist

```python
def gdpr_compliance_checklist(table_name):
    """Verify GDPR compliance settings"""
    
    # Check table properties
    properties = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
    props_dict = {row['key']: row['value'] for row in properties}
    
    checklist = {
        'change_data_feed_enabled': props_dict.get('delta.enableChangeDataFeed') == 'true',
        'deletion_vectors_enabled': props_dict.get('delta.enableDeletionVectors') == 'true',
        'row_tracking_enabled': props_dict.get('delta.enableRowTracking') == 'true',
        'vacuum_retention_set': 'delta.deletedFileRetentionDuration' in props_dict,
        'log_retention_set': 'delta.logRetentionDuration' in props_dict
    }
    
    # Verify deletion capability
    try:
        test_delete = spark.sql(f"DELETE FROM {table_name} WHERE 1=0")
        checklist['delete_operations_allowed'] = True
    except Exception as e:
        if 'appendOnly' in str(e):
            checklist['delete_operations_allowed'] = False
        else:
            checklist['delete_operations_allowed'] = 'unknown'
    
    return checklist

# Usage
compliance = gdpr_compliance_checklist("silver.user_data")
print("GDPR Compliance Status:")
for item, status in compliance.items():
    print(f"  {item}: {status}")
```

## Best Practices

### Weekly Maintenance Schedule
```python
def weekly_gdpr_maintenance():
    """Weekly maintenance for GDPR compliance"""
    
    tables = ["bronze.user_data", "silver.user_data_enriched", "gold.user_analytics"]
    
    for table in tables:
        # Step 1: Apply physical deletions
        spark.sql(f"REORG TABLE {table} APPLY (PURGE)")
        
        # Step 2: Vacuum old files (7-day retention)
        spark.sql(f"VACUUM {table} RETAIN 168 HOURS")
        
        print(f"Completed maintenance for {table}")

# Schedule weekly (e.g., Sunday 2 AM)
# This ensures deleted data is physically removed within compliance windows
```

## When to Use This Skill

- Implementing GDPR right-to-be-forgotten workflows
- Building compliance-ready streaming pipelines
- Handling deletes in Change Data Capture (CDC) scenarios
- Managing data retention and deletion policies
- Optimizing storage costs while maintaining compliance

## Related Skills

- delta-lake-change-data-feed
- gdpr-compliance-patterns
- data-retention-management
- streaming-delete-propagation