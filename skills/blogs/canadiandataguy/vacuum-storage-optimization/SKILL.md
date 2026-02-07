---
name: "Your Storage Bill Is Too High. Here Are 3 Levels of VACUUM to Fix It"
description: "Use Delta Lake VACUUM to manage storage costs with Full, Light, and Inventory-based cleanup strategies."
author: "Canadian Data Guy"
url: "https://www.youtube.com/watch?v=Ijde5wDzX5A"
date: "2025-12-02"
tags: ["delta", "vacuum", "storage", "cost-optimization", "cleanup"]
---

# Delta VACUUM Strategies

## Overview

Delta Lake's time travel retains old files, increasing storage costs. VACUUM removes unneeded files while preserving the ability to time travel within retention period.

**Three Levels**:
1. **VACUUM FULL**: Scans entire directory
2. **VACUUM LIGHT**: Uses transaction log (faster)
3. **VACUUM INVENTORY**: For petabyte scale

## Quick Start

### Basic VACUUM

```sql
-- Default: Remove files older than 7 days
VACUUM delta_table;

-- Retain 30 days of history
VACUUM delta_table RETAIN 720 HOURS;

-- Dry run: See what would be deleted
VACUUM delta_table DRY RUN;
```

### VACUUM LIGHT

```sql
-- Faster, uses transaction log
-- Requirement: Recent successful FULL VACUUM
VACUUM delta_table LIGHT;

-- Best practice: Alternate FULL and LIGHT
-- FULL: Weekly
-- LIGHT: Daily
```

## Common Patterns

### Pattern 1: Storage Cost Management

```sql
-- Aggressive cleanup for non-critical tables
VACUUM bronze_data RETAIN 24 HOURS;

-- Conservative for compliance tables
VACUUM audit_logs RETAIN 2555 HOURS;  -- 365 days

-- Immediate cleanup for temp tables
VACUUM temp_table RETAIN 0 HOURS;
```

### Pattern 2: Automated VACUUM Schedule

```sql
-- Weekly deep clean
-- Daily light maintenance

-- Monday: FULL VACUUM
VACUUM prod_table;

-- Tuesday-Sunday: LIGHT VACUUM
VACUUM prod_table LIGHT;
```

### Pattern 3: Safe VACUUM Validation

```sql
-- Always dry run first
VACUUM prod_table RETAIN 168 HOURS DRY RUN;

-- Review output:
-- - Number of files to delete
-- - Total size to recover
-- - Any warnings

-- Then execute
VACUUM prod_table RETAIN 168 HOURS;
```

## Reference Files

### VACUUM Comparison

| Type | Speed | Use Case | Requirement |
|------|-------|----------|-------------|
| **FULL** | Slower | Deep clean | None |
| **LIGHT** | Fast | Daily maintenance | Recent FULL |
| **INVENTORY** | Scalable | Petabyte scale | Custom setup |

### Two-Step Process

```
VACUUM Execution:
1. Workers FIND files to delete
   - FULL: Scan directory
   - LIGHT: Read transaction log
   
2. Driver DELETES files
   - Coordinated cleanup
   - Progress tracking
```

### Cluster Configuration

```
Optimal VACUUM Cluster:
- Autoscaling: Yes (for large tables)
- Driver: Large (coordinates deletes)
- Workers: Compute-optimized (find files)
```

## Common Issues

| Issue | Solution |
|-------|----------|
| **VACUUM LIGHT fails** | Run FULL VACUUM first |
| **Still high storage** | Check retention period; may need shorter retention |
| **Long VACUUM times** | Use LIGHT for regular; FULL for weekly |
| **Accidental data loss** | Use DRY RUN; verify before execute |
| **Time travel broken** | VACUUM respects retention; check RETAIN hours |

## Advanced Tips

### Retention Period Strategy

```sql
-- Bronze layer: Short retention (raw data recreatable)
VACUUM bronze RETAIN 24 HOURS;

-- Silver layer: Medium retention
VACUUM silver RETAIN 168 HOURS;  -- 7 days

-- Gold layer: Longer retention
VACUUM gold RETAIN 720 HOURS;  -- 30 days

-- Compliance: As required
VACUUM audit RETAIN 2555 HOURS;  -- 365 days
```

### Storage Monitoring

```sql
-- Check table size and file count
DESCRIBE DETAIL delta_table;

-- Output:
-- - sizeInBytes
-- - numFiles
-- - properties (vacuum settings)

-- Monitor vacuum effectiveness
-- Track storage costs over time
```

### VACUUM Performance

```sql
-- For large tables:
-- 1. Use autoscaling cluster
-- 2. Run during low-traffic hours
-- 3. Consider partitioning strategy

-- Partitioned tables vacuum faster
-- Only affected partitions scanned
```

## FAQ

**Q: How often should I run VACUUM?**
A: LIGHT daily, FULL weekly. Adjust based on update frequency.

**Q: Can I recover files after VACUUM?**
A: No. VACUUM permanently deletes files. Use time travel before VACUUM if needed.

**Q: Why is my storage still high after VACUUM?**
A: Check retention period. Files newer than RETAIN are preserved.

**Q: Does VACUUM affect queries?**
A: No, VACUUM runs in background. May impact performance on small clusters.

**Q: What's the minimum retention?**
A: 0 hours (immediate deletion), but not recommended for production.