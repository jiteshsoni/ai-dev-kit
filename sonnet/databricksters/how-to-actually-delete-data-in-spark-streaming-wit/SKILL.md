---
name: "GDPR-Compliant Deletes in Spark Streaming"
description: "Handle DELETE operations in streaming pipelines for GDPR compliance using Delta Lake Change Data Feed, deletion vectors, REORG, and VACUUM."
author: "Geethu"
url: "https://www.databricksters.com/p/how-to-actually-delete-data-in-spark"
date: "2025-11-28"
tags: ["streaming", "gdpr", "delete", "delta-lake", "change-data-feed", "compliance", "databricks"]
---

# GDPR-Compliant Deletes in Spark Streaming

## Overview

Streaming pipelines are traditionally append-only, but GDPR and compliance requirements demand DELETE propagation across Bronze, Silver, and Gold layers. Delta Lake enables DELETE, UPDATE, and MERGE on streaming tables through Change Data Feed and deletion vectors. This skill covers patterns for handling deletes without breaking streaming pipelines.

**Use this skill when:** Implementing GDPR/CCPA compliance, handling deletes in streaming, or migrating from append-only to mutable streaming tables.

## Quick Start

Basic pattern for propagating deletes through streaming layers:

```python
# Bronze table: Allow deletes but continue streaming
spark.sql("""
    CREATE TABLE bronze_events (
        user_id STRING,
        email STRING,
        event_data STRING,
        event_time TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true'
    )
""")

# Option 1: Ignore deletes in downstream (append-only semantics)
ignore_deletes_df = (spark.readStream
    .format("delta")
    .option("skipChangeCommits", "true")  # Only see new appends
    .table("bronze_events")
)

# Option 2: Process deletes explicitly (CDC semantics)
change_feed_df = (spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")  # See all changes
    .table("bronze_events")
)

# Filter by change type
inserts = change_feed_df.filter("_change_type = 'insert'")
updates = change_feed_df.filter("_change_type = 'update_postimage'")
deletes = change_feed_df.filter("_change_type = 'delete'")
```

## Common Patterns

### Pattern 1: skipChangeCommits vs readChangeFeed

Choose the right option based on downstream requirements:

```python
# Pattern A: Ignore deletes (continue append-only streaming)
# Use when: Downstream doesn't need to process deletes

only_appends_df = (spark.readStream
    .format("delta")
    .option("skipChangeCommits", "true")
    .table("bronze_events")
)

# Stream continues without interruption
only_appends_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/silver") \
    .table("silver_events")

# Pattern B: Process all changes explicitly
# Use when: Need to propagate deletes downstream

all_changes_df = (spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .table("bronze_events")
)

# Handle each change type appropriately
def process_changes(batch_df, batch_id):
    inserts = batch_df.filter("_change_type = 'insert'")
    updates = batch_df.filter("_change_type = 'update_postimage'")
    deletes = batch_df.filter("_change_type = 'delete'")
    
    # Process inserts/updates normally
    if inserts.count() > 0:
        inserts.write.format("delta").mode("append").saveAsTable("silver_events")
    
    # Propagate deletes
    if deletes.count() > 0:
        delete_ids = [row.user_id for row in deletes.collect()]
        spark.sql(f"""
            DELETE FROM silver_events 
            WHERE user_id IN ({','.join([f"'{id}'" for id in delete_ids])})
        """)

all_changes_df.writeStream \
    .foreachBatch(process_changes) \
    .option("checkpointLocation", "/checkpoints/silver_cdc") \
    .start()
```

**Decision matrix:**
- **skipChangeCommits**: Downstream doesn't care about deletes (reports, archives)
- **readChangeFeed**: Downstream must reflect deletes (user profiles, active records)

### Pattern 2: GDPR Delete Workflow

Complete workflow for GDPR compliance:

```python
# Step 1: Receive deletion request
deletion_request = "user123@example.com"

# Step 2: Delete from all layers (Bronze → Silver → Gold)
for table in ["bronze_events", "silver_user_profiles", "gold_analytics"]:
    spark.sql(f"""
        DELETE FROM {table} 
        WHERE email = '{deletion_request}'
    """)

# Step 3: Physically purge using deletion vectors
for table in ["bronze_events", "silver_user_profiles", "gold_analytics"]:
    # REORG physically removes rows marked in deletion vectors
    spark.sql(f"REORG TABLE {table} APPLY (PURGE)")

# Step 4: VACUUM to remove old files (after retention period)
for table in ["bronze_events", "silver_user_profiles", "gold_analytics"]:
    spark.sql(f"VACUUM {table} RETAIN 72 HOURS")

print(f"User {deletion_request} deleted and purged across all layers")
```

**Timing recommendations:**
- **DELETE**: Immediate upon request
- **REORG**: Weekly (physically removes DV-marked rows)
- **VACUUM**: Weekly after REORG (removes old files)

### Pattern 3: Deletion Vectors Explained

Understanding how deletion vectors work:

```python
# Without Deletion Vectors:
# - DELETE requires rewriting entire Parquet file
# - Expensive for small deletes in large files
# - Blocks concurrent operations

# With Deletion Vectors:
# - DELETE creates small metadata file marking rows deleted
# - No rewrite needed immediately
# - Fast operation, concurrent-safe

# Enable deletion vectors
spark.sql("""
    ALTER TABLE bronze_events 
    SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
""")

# After deletes, check DV usage
details = spark.sql("DESCRIBE DETAIL bronze_events").collect()[0]
print(f"Deletion vector count: {details.deletionVectorStats}")

# Visual representation:
"""
DELETE Operation Timeline:

1. DELETE command
   ├─ Marks rows in deletion vector (milliseconds)
   └─ Returns immediately

2. Reads (SELECT, MERGE)
   ├─ Read Parquet files
   └─ Skip rows marked in DV

3. REORG (periodic)
   ├─ Rewrites files without deleted rows
   └─ Removes deletion vectors

4. VACUUM (periodic)
   └─ Physically deletes old files
"""
```

### Pattern 4: Append-Only Lock for Source Tables

Prevent accidental deletes on source tables:

```python
# Lock bronze table to append-only
spark.sql("""
    CREATE TABLE bronze_raw_events (
        event_id STRING,
        payload STRING,
        ingested_at TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES (
        'delta.appendOnly' = 'true'
    )
""")

# Attempts to DELETE will fail
try:
    spark.sql("DELETE FROM bronze_raw_events WHERE event_id = '123'")
except Exception as e:
    print(f"Error: {e}")  # "This table is append-only"

# Downstream can still perform deletes on transformed tables
spark.sql("""
    CREATE TABLE silver_events (
        event_id STRING,
        user_id STRING,
        processed_payload STRING
    ) USING DELTA
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true'
    )
""")

# Silver table accepts deletes
spark.sql("DELETE FROM silver_events WHERE user_id = 'user123'")
```

**Use case:** Protect immutable source tables while allowing deletes on processed data.

### Pattern 5: Row-Level Concurrency with Deletes

Enable concurrent deletes and streaming writes:

```python
# Enable row-level concurrency
spark.sql("""
    ALTER TABLE silver_events 
    SET TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.enableRowLevelConcurrency' = 'true'
    )
""")

# Now these can run concurrently without conflicts:
# 1. Streaming appends
stream = (spark.readStream
    .table("bronze_events")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/")
    .table("silver_events")
)

# 2. GDPR deletes (different partition/rows)
spark.sql("DELETE FROM silver_events WHERE user_id = 'user456'")

# 3. Updates (different rows)
spark.sql("UPDATE silver_events SET status = 'active' WHERE user_id = 'user789'")

# Without RLC: Operations conflict and fail
# With RLC: Operations succeed if touching different rows
```

## Reference Files

- [Delta Lake VACUUM](https://docs.databricks.com/sql/language-manual/delta-vacuum.html)
- [Delta Lake REORG](https://docs.databricks.com/sql/language-manual/delta-reorg-table.html)
- [Deletion Vectors](https://docs.databricks.com/delta/deletion-vectors.html)
- [Row-Level Concurrency](https://docs.databricks.com/optimizations/isolation-level.html)
- [Change Data Feed](https://docs.databricks.com/delta/delta-change-data-feed.html)
- [GDPR with DLT](https://www.databricks.com/blog/handling-right-be-forgotten-gdpr-and-ccpa-using-delta-live-tables-dlt)

## Common Issues

| Issue | Solution |
|-------|----------|
| **Stream fails after DELETE** | Use `skipChangeCommits` or `readChangeFeed`. Don't read without options. |
| **Deletes not visible downstream** | Use `readChangeFeed` and explicitly process `_change_type = 'delete'`. |
| **Storage growing after deletes** | Run REORG to purge, then VACUUM to remove old files. |
| **Concurrent DELETE conflicts** | Enable `delta.enableRowLevelConcurrency` and `enableDeletionVectors`. |
| **VACUUM deleting too soon** | Set appropriate retention: `VACUUM table RETAIN 168 HOURS` (7 days). |
| **Can't DELETE on append-only table** | Remove `delta.appendOnly = 'true'` property if deletes needed. |

## Advanced Tips

### Automated GDPR Deletion Pipeline

```python
# Complete automated deletion pipeline
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

# Create deletion job
deletion_job = w.jobs.create(
    name="gdpr-deletion-pipeline",
    tasks=[
        {
            "task_key": "delete_from_tables",
            "notebook_task": {
                "notebook_path": "/Shared/gdpr_delete",
                "base_parameters": {
                    "email": "{{job.parameters.email}}"
                }
            },
            "existing_cluster_id": "cluster-id"
        },
        {
            "task_key": "reorg_tables",
            "depends_on": [{"task_key": "delete_from_tables"}],
            "notebook_task": {
                "notebook_path": "/Shared/reorg_purge"
            },
            "existing_cluster_id": "cluster-id"
        },
        {
            "task_key": "vacuum_tables",
            "depends_on": [{"task_key": "reorg_tables"}],
            "notebook_task": {
                "notebook_path": "/Shared/vacuum_old_files"
            },
            "existing_cluster_id": "cluster-id"
        }
    ],
    parameters=[
        {"name": "email", "default": ""}
    ]
)

# Trigger deletion for specific user
run = w.jobs.run_now(
    job_id=deletion_job.job_id,
    job_parameters={"email": "user@example.com"}
)
```

### Monitor Deletion Vector Growth

```sql
-- Check DV statistics per table
DESCRIBE DETAIL bronze_events;

-- Look for:
-- - deletionVectorStats.numDeletionVectors
-- - deletionVectorStats.numDeletedRows

-- Schedule REORG if DV count is high
-- Rule of thumb: REORG if >10% of rows marked deleted
SELECT 
    numDeletedRows / numRows as delete_percentage
FROM (DESCRIBE DETAIL bronze_events);
```

### Retention Policy Best Practices

```python
# Default: 7 days (good for most use cases)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql("VACUUM bronze_events RETAIN 168 HOURS")

# Longer retention for critical tables (21 days = 3 weeks vacation)
spark.sql("VACUUM silver_critical RETAIN 504 HOURS")

# Shorter retention for high-volume tables (3 days)
spark.sql("VACUUM bronze_high_volume RETAIN 72 HOURS")

# Remember: Once vacuumed, time travel to before that point is impossible
```

## FAQ

**Q: Do I need to pause streaming jobs during DELETE?**  
A: No! With `skipChangeCommits` or `readChangeFeed`, streaming continues during deletes.

**Q: Does Predictive Optimization run REORG automatically?**  
A: No. You must explicitly run `REORG TABLE ... APPLY (PURGE)` to purge deletion vectors.

**Q: How often should I run DELETE, REORG, and VACUUM?**  
A: Weekly is cost-effective. Sequence: DELETE → REORG → VACUUM (in that order).

**Q: What if VACUUM retention is too long?**  
A: You'll pay for storing logically deleted data. Balance retention with storage costs.

**Q: Can two concurrent DELETEs conflict?**  
A: With row-level concurrency enabled, they only conflict if deleting the same rows.

**Q: How do I know if deletion vectors are working?**  
A: Run `DESCRIBE DETAIL table` and check `deletionVectorStats.numDeletionVectors > 0`.
