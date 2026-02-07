---
name: "Delta Lake Data Archival for Cost Optimization"
description: "Archive infrequently accessed Delta table data to cold/archive storage tiers using cloud lifecycle policies and delta.timeUntilArchived property."
author: "Yogesh Gowda"
url: "https://www.databricksters.com/p/archiving-data-in-databricks-lakehouse"
date: "2025-02-25"
tags: ["archival", "cost-optimization", "storage-tiers", "delta-lake", "lifecycle-management", "databricks"]
---

# Delta Lake Data Archival for Cost Optimization

## Overview

Reduce storage costs by moving older Delta table data to cold/archive tiers using cloud lifecycle policies (Azure Cool/Archive, AWS Glacier). Configure `delta.timeUntilArchived` property and use views with predicates to restrict access to archived data while maintaining table functionality. Effective strategy can reduce storage costs by 70%+ for large historical datasets.

**Use this skill when:** Managing large historical datasets, optimizing storage costs, or implementing compliance retention policies.

## Quick Start

Basic archival setup:

```sql
-- Step 1: Partition table by date for easy archival
CREATE TABLE sales_data 
USING delta
PARTITIONED BY (ingestion_date)
AS SELECT * FROM raw_sales;

-- Step 2: Set archival retention period
ALTER TABLE sales_data 
SET TBLPROPERTIES ('delta.timeUntilArchived' = '365 days');

-- Step 3: Create view restricting to active (non-archived) data
CREATE VIEW active_sales AS 
SELECT * FROM sales_data
WHERE ingestion_date > CURRENT_DATE() - INTERVAL 30 DAYS;

-- Step 4: Configure cloud lifecycle policy
-- Azure Blob Storage: Set lifecycle rule via portal/CLI
-- Move to Cool tier after 30 days
-- Move to Archive tier after 365 days
```

## Common Patterns

### Pattern 1: Azure Blob Storage Lifecycle Policy

```json
{
  "rules": [
    {
      "name": "ArchiveOldData",
      "type": "Lifecycle",
      "enabled": true,
      "definition": {
        "filters": {
          "blobTypes": ["blockBlob"],
          "prefixMatch": ["gold/sales_data/"]
        },
        "actions": {
          "baseBlob": {
            "tierToCool": {
              "daysAfterCreationGreaterThan": 30
            },
            "tierToArchive": {
              "daysAfterCreationGreaterThan": 365
            }
          }
        }
      }
    }
  ]
}
```

**Storage tier characteristics:**
- **Hot tier**: Instant access, higher storage cost
- **Cool tier**: Instant access, lower storage cost, higher access fees
- **Archive tier**: Requires rehydration, lowest storage cost

### Pattern 2: BI Reporting with Tiered Data

```sql
-- Daily reports: Hot tier only (last 30 days)
CREATE VIEW daily_reports AS
SELECT * FROM sales_data
WHERE ingestion_date >= CURRENT_DATE() - INTERVAL 30 DAYS;

-- Quarterly reports: Hot + Cool tiers (last 365 days)
CREATE VIEW quarterly_reports AS
SELECT * FROM sales_data
WHERE ingestion_date >= CURRENT_DATE() - INTERVAL 365 DAYS;

-- Historical analysis: Access via base table with predicates
-- Users aware of potential rehydration costs

-- Configure retention
ALTER TABLE sales_data 
SET TBLPROPERTIES (
  'delta.timeUntilArchived' = '365 days',
  'delta.logRetentionDuration' = '365 days'
);
```

### Pattern 3: Optimize with Predicates to Avoid Rewriting Old Files

```sql
-- ❌ BAD: Rewrites ALL files including archived
OPTIMIZE sales_data;

-- ✅ GOOD: Only optimize recent data
OPTIMIZE sales_data 
WHERE ingestion_date >= CURRENT_DATE() - INTERVAL 30 DAYS
ZORDER BY (customer_id);

-- This prevents:
-- 1. Resetting file creation dates (delays archival)
-- 2. Rehydration costs for archived files
-- 3. Unnecessary optimization of cold data
```

### Pattern 4: Handling DML Operations

```sql
-- Problem: UPDATE/DELETE/MERGE reset file creation dates

-- Solution 1: Partition strategy
-- Only run DML on recent partitions
DELETE FROM sales_data 
WHERE ingestion_date >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND customer_id = '12345';

-- Solution 2: Accept delayed archival for updated files
-- Files will be archived based on NEW creation date

-- Solution 3: Use deletion vectors (no file rewrites)
ALTER TABLE sales_data 
SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

DELETE FROM sales_data WHERE customer_id = '12345';
-- No file rewrite, original creation date preserved
```

### Pattern 5: Streaming Tables as Archival Candidates

```python
# Streaming telemetry: Perfect for archival
# Objects read once, then archived

# Enable archival on streaming target
spark.sql("""
  ALTER TABLE iot_telemetry 
  SET TBLPROPERTIES ('delta.timeUntilArchived' = '90 days')
""")

# Streaming job writes data
(spark.readStream
  .table("bronze_iot")
  .writeStream
  .format("delta")
  .option("checkpointLocation", "/checkpoints/")
  .partitionBy("event_date")  # Enable date-based archival
  .table("iot_telemetry")
)

# After 90 days, files automatically tier to cold storage
# Stream continues operating normally
```

## Reference Files

- [Delta Lake Archival Documentation](https://docs.databricks.com/en/optimizations/archive-delta.html)
- [Azure Blob Lifecycle Management](https://docs.microsoft.com/en-us/azure/storage/blobs/lifecycle-management-overview)
- [AWS S3 Lifecycle Policies](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html)

## Common Issues

| Issue | Solution |
|-------|----------|
| **Files not archiving as expected** | Check cloud lifecycle policy filters match table path. Verify file creation dates. |
| **DML operations delay archival** | Use deletion vectors, or accept delayed archival for updated partitions. |
| **Query fails on archived data** | Archived (offline) files require rehydration. Use views to restrict access. |
| **High access costs** | Frequent access to Cool/Archive tiers is expensive. Keep active data in Hot tier. |
| **OPTIMIZE rewrites old files** | Use predicates: `OPTIMIZE table WHERE date >= recent_date`. |
| **_delta_log archived** | NEVER apply lifecycle policies to `_delta_log/` directory! |

## Advanced Tips

### Calculate Cost Savings

```python
# Example: 10TB table, 80% data > 1 year old
# Hot tier: $0.0184/GB/month = $184/TB/month
# Cool tier: $0.0100/GB/month = $100/TB/month
# Archive tier: $0.00099/GB/month = $0.99/TB/month

# Before archival: 10TB * $184 = $1,840/month
# After archival:
#   - 2TB Hot (recent): 2TB * $184 = $368
#   - 0TB Cool (transition)
#   - 8TB Archive (old): 8TB * $0.99 = $7.92
# Total: $375.92/month
# Savings: $1,464/month (80% reduction)
```

### Monitor Archive Status

```sql
-- Check file distribution across partitions
DESCRIBE DETAIL sales_data;

-- Identify candidate tables for archival
SELECT 
  table_name,
  size_in_bytes / 1024 / 1024 / 1024 as size_gb,
  num_files
FROM (DESCRIBE EXTENDED sales_data)
WHERE size_in_bytes > 1099511627776; -- > 1TB

-- Track data age distribution
SELECT 
  YEAR(ingestion_date) as year,
  COUNT(*) as file_count,
  SUM(size_in_bytes) / 1024 / 1024 / 1024 as total_gb
FROM delta.`/path/to/sales_data`
GROUP BY YEAR(ingestion_date)
ORDER BY year DESC;
```

### Best Practices Checklist

```markdown
✅ **DO:**
- Partition tables by date for archival
- Set delta.timeUntilArchived to match cloud policy
- Use views with predicates for interactive queries
- Apply OPTIMIZE only to recent partitions
- Enable deletion vectors for tables with DML
- Choose streaming tables for archival

❌ **DON'T:**
- Archive _delta_log directory
- Run OPTIMIZE without predicates
- Query archived data frequently
- Apply lifecycle policies to all storage
- Forget to document archival strategy for users
```

## FAQ

**Q: Can I query archived data?**  
A: Online tiers (Cool): Yes, with higher access costs. Offline tiers (Archive): Requires manual rehydration first.

**Q: What happens to streaming jobs when data is archived?**  
A: Streaming continues normally. Archived files aren't re-read by streaming sources.

**Q: How do lifecycle policies interact with VACUUM?**  
A: Independent operations. VACUUM removes files from Delta log. Lifecycle policies tier files in storage.

**Q: Does Predictive Optimization conflict with archival?**  
A: PO may rewrite files, resetting creation dates. Use manual OPTIMIZE with predicates instead.

**Q: What about Unity Catalog managed tables?**  
A: Same approach works. Configure lifecycle policies on UC external locations.

**Q: Can I restore archived data to Hot tier?**  
A: Yes, rehydrate from Archive to Hot/Cool tier. May take hours depending on volume.
