---
name: "Delta Lake VACUUM Optimization Strategies"
description: "Reduce storage costs using VACUUM FULL, VACUUM LITE, and hybrid strategies to clean up old Delta table files efficiently."
author: "Canadian Data Guy"
url: "https://www.databricksters.com/p/your-storage-bill-is-too-high-here"
date: "2025-12-02"
tags: ["vacuum", "storage", "cost-optimization", "delta-lake", "maintenance", "databricks"]
---

# Delta Lake VACUUM Optimization

## Overview

Delta Lake files accumulate without automatic cleanup, causing storage bloat. Three VACUUM modes address this: VACUUM FULL (comprehensive but slow), VACUUM LITE (fast daily cleanup, DBR 16.1+), and VACUUM USING INVENTORY (extreme scale only). Hybrid approach recommended: daily LITE + weekly FULL for optimal cost/performance.

**Use this skill when:** Storage costs growing unexpectedly, optimizing Delta table maintenance, or implementing efficient cleanup schedules.

## Quick Start

```sql
-- Basic VACUUM (FULL mode)
VACUUM events RETAIN 168 HOURS;  -- 7 days retention

-- Fast VACUUM LITE (DBR 16.1+, requires prior FULL run)
VACUUM events LITE RETAIN 168 HOURS;

-- Check retention settings
SHOW TBLPROPERTIES events;

-- Modify retention duration
ALTER TABLE events 
SET TBLPROPERTIES (
  'delta.logRetentionDuration' = '7 days',
  'delta.deletedFileRetentionDuration' = '7 days'
);
```

## Common Patterns

### Pattern 1: Hybrid Strategy (Recommended)

```sql
-- Daily: Fast LITE cleanup (5 minutes)
VACUUM high_churn_table LITE RETAIN 168 HOURS;

-- Weekly: Comprehensive FULL cleanup (30-60 minutes)
-- Scheduled for Sunday morning
VACUUM high_churn_table RETAIN 168 HOURS;
```

**Benefits:**
- Daily LITE removes committed file deletions quickly
- Weekly FULL catches straggler files from aborted writes
- Optimal balance of cost, performance, and thoroughness

### Pattern 2: VACUUM FULL Process

Three phases of VACUUM FULL execution:

```python
# Phase 1: Recursive file listing (slowest)
# - Lists all files in table directory
# - Generates cloud storage API calls (S3 LIST, Azure List Blobs)
# - Time: 20-40 minutes for petabyte tables

# Phase 2: Delta log comparison
# - Reads transaction log
# - Identifies files not referenced + older than retention
# - Finds straggler files never committed

# Phase 3: Deletion (driver-only)
# - Driver issues delete commands
# - AWS: Single-threaded DeleteObjects
# - Azure/GCP: Parallel with parallelDelete.enabled

# Enable parallel delete for Azure/GCP
spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")

# Run VACUUM FULL
spark.sql("VACUUM events RETAIN 168 HOURS")
```

**Use cases:**
- First VACUUM run (establishes baseline)
- After failed/messy ingestion jobs
- Periodic deep cleaning (weekly/monthly)
- Compliance-critical scenarios

### Pattern 3: VACUUM LITE Fast Cleanup

```sql
-- Requires: DBR 16.1+ and prior VACUUM FULL baseline

-- VACUUM LITE workflow:
-- 1. Read Delta log (fast)
-- 2. Find files marked "removed" + older than retention
-- 3. Delete them (no directory traversal!)

VACUUM events LITE RETAIN 168 HOURS;

-- Error if no FULL baseline:
-- DELTA_CANNOT_VACUUM_LITE: Please run VACUUM FULL first

-- Run FULL to establish baseline, then use LITE
VACUUM events RETAIN 168 HOURS;  -- Baseline
VACUUM events LITE RETAIN 168 HOURS;  -- Future runs
```

**Performance:**
- FULL: 30-60 minutes for large tables
- LITE: 2-5 minutes for same tables
- 10-20x speedup

**Limitation:** Doesn't catch straggler files (uncommitted files from aborted writes)

### Pattern 4: Scheduled VACUUM Jobs

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

# Create daily LITE job
daily_job = w.jobs.create(
    name="daily-vacuum-lite",
    tasks=[{
        "task_key": "vacuum_lite",
        "notebook_task": {
            "notebook_path": "/Shared/maintenance/vacuum_lite",
            "source": "WORKSPACE"
        },
        "existing_cluster_id": "cluster-id"
    }],
    schedule={
        "quartz_cron_expression": "0 0 2 * * ?",  # 2 AM daily
        "timezone_id": "UTC"
    }
)

# Create weekly FULL job
weekly_job = w.jobs.create(
    name="weekly-vacuum-full",
    tasks=[{
        "task_key": "vacuum_full",
        "notebook_task": {
            "notebook_path": "/Shared/maintenance/vacuum_full",
            "source": "WORKSPACE"
        },
        "new_cluster": {
            "spark_version": "16.1.x-scala2.12",
            "node_type_id": "c5.4xlarge",  # Compute-optimized
            "num_workers": 0,  # Single-node for cost efficiency
            "driver_node_type_id": "c5.9xlarge"  # Large driver
        }
    }],
    schedule={
        "quartz_cron_expression": "0 0 3 ? * SUN",  # Sunday 3 AM
        "timezone_id": "UTC"
    }
)
```

### Pattern 5: Cost-Optimized Cluster Config

```python
# Standard config: Auto-scaling 1-4 workers
# Cost-optimized: Single powerful driver (no workers)

# For VACUUM LITE (fast anyway):
single_node_config = {
    "spark_version": "16.1.x-scala2.12",
    "node_type_id": "c5.4xlarge",  # 16 cores, compute-optimized
    "num_workers": 0,
    "spark_conf": {
        "spark.master": "local[*, 4]",
        "spark.databricks.cluster.profile": "singleNode"
    },
    "custom_tags": {"ResourceClass": "SingleNode"}
}

# For VACUUM FULL (benefits from parallelism):
# - File listing phase: Parallelized across directories
# - But bottlenecked by S3 API rate limits
# - Single large driver often sufficient and cheaper

# Why it works:
# 1. File listing bottlenecked by API, not CPU
# 2. File deletion is driver-only operation
# 3. Single large driver cheaper than multi-node cluster

# Cost comparison (30-minute job):
# Multi-node (1 large driver + 4 workers): ~$15
# Single-node (1 xlarge driver): ~$5
# Savings: 67%
```

## Reference Files

- [Delta Lake VACUUM Documentation](https://docs.databricks.com/sql/language-manual/delta-vacuum.html)
- [Delta Lake 3.3.0 Release (LITE)](https://github.com/delta-io/delta/releases/tag/v3.3.0)
- [Delta Lake 3.2.0 Release (INVENTORY)](https://github.com/delta-io/delta/releases/tag/v3.2.0)

## Common Issues

| Issue | Solution |
|-------|----------|
| **VACUUM takes too long** | Use VACUUM LITE for daily runs. Requires DBR 16.1+. |
| **DELTA_CANNOT_VACUUM_LITE error** | Run VACUUM FULL once to establish baseline for LITE. |
| **Storage still growing** | Check VACUUM is actually running. Verify retention settings. |
| **Out of memory during VACUUM** | Reduce batch size or increase driver memory. |
| **Straggler files not removed** | LITE doesn't remove stragglers. Run FULL periodically. |
| **High S3 API costs from VACUUM** | Use LITE (no directory traversal) or schedule FULL less frequently. |

## Advanced Tips

### Calculate Storage Savings

```sql
-- Check current storage
DESCRIBE DETAIL events;
-- Note: sizeInBytes

-- Run VACUUM
VACUUM events RETAIN 168 HOURS;

-- Check after VACUUM
DESCRIBE DETAIL events;
-- Calculate savings: before_size - after_size

-- Track over time
CREATE TABLE vacuum_metrics (
    table_name STRING,
    vacuum_date DATE,
    size_before_gb DECIMAL(10,2),
    size_after_gb DECIMAL(10,2),
    savings_gb DECIMAL(10,2),
    savings_pct DECIMAL(5,2)
);

INSERT INTO vacuum_metrics VALUES (
    'events',
    CURRENT_DATE(),
    before_size / 1024 / 1024 / 1024,
    after_size / 1024 / 1024 / 1024,
    (before_size - after_size) / 1024 / 1024 / 1024,
    ((before_size - after_size) / before_size) * 100
);
```

### Retention Policy Considerations

```python
# Default: 7 days (168 hours)
# Conservative: 21 days (accounts for vacation)
# Aggressive: 3 days (high-volume tables)

# Set based on:
# 1. Time travel requirements
# 2. Incident recovery SLAs
# 3. Compliance requirements

# Example: Different policies per table
tables_config = {
    "bronze_raw": 3,      # Source data, can reload
    "silver_processed": 7,  # Standard
    "gold_critical": 21    # Business-critical, longer recovery window
}

for table, days in tables_config.items():
    spark.sql(f"VACUUM {table} RETAIN {days * 24} HOURS")
```

### Monitoring VACUUM Performance

```python
# Track VACUUM execution
vacuum_start = time.time()

spark.sql("VACUUM events LITE RETAIN 168 HOURS")

duration = time.time() - vacuum_start
print(f"VACUUM completed in {duration:.2f} seconds")

# Log to monitoring table
spark.sql(f"""
    INSERT INTO vacuum_performance VALUES (
        'events',
        'LITE',
        CURRENT_TIMESTAMP(),
        {duration}
    )
""")

# Analyze trends
spark.sql("""
    SELECT 
        table_name,
        vacuum_mode,
        AVG(duration_seconds) as avg_duration,
        MAX(duration_seconds) as max_duration
    FROM vacuum_performance
    WHERE vacuum_date >= CURRENT_DATE() - INTERVAL 30 DAYS
    GROUP BY table_name, vacuum_mode
""").show()
```

### VACUUM USING INVENTORY (Advanced/Not Recommended)

```sql
-- Only for extreme scale (petabytes) with dedicated platform team
-- Requires setup: S3 Inventory, manifest tables, complex monitoring

-- Setup inventory (one-time)
CREATE TABLE inventory_manifest 
USING delta
AS SELECT * FROM parquet.`s3://bucket/inventory/manifest.parquet`;

-- VACUUM with inventory
VACUUM events 
USING INVENTORY inventory_manifest
RETAIN 168 HOURS;

-- Why not recommended:
-- 1. Complex setup and maintenance
-- 2. Compliance risk if inventory stale
-- 3. LITE is simpler and almost as fast
-- 4. No team I've worked with uses it (400+ companies)
```

## FAQ

**Q: How often should I run VACUUM?**  
A: LITE daily, FULL weekly for most tables. Adjust based on modification frequency.

**Q: Will VACUUM delete data I need?**  
A: No, retention period protects recent versions. Only deletes files older than retention + not in current state.

**Q: Can I run VACUUM on streaming tables?**  
A: Yes! VACUUM is compatible with streaming. Schedule during low-traffic periods.

**Q: Does Predictive Optimization run VACUUM?**  
A: Yes, PO can run VACUUM automatically on UC-managed tables.

**Q: What if I accidentally VACUUM with 0 hours retention?**  
A: Requires disabling safety check. Don't do this unless you understand the risk.

**Q: LITE vs FULL - which should I use?**  
A: Use LITE for daily cleanup (fast), FULL for weekly deep clean (catches stragglers).

**Q: Why is my storage still high after VACUUM?**  
A: Check: (1) VACUUM actually ran, (2) retention not too long, (3) deletion vectors not purged (run REORG).
