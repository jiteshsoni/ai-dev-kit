---
name: storage-optimization
description: "File compaction, layout optimization, and storage cost management."
tags: ["storage", "optimization", "compaction", "vacuum"]
---

# Storage Optimization

## Overview

Optimize storage layout and reduce costs through file compaction and VACUUM strategies.

## Quick Start

### File Compaction

```sql
-- Consolidate small files
OPTIMIZE table_name;

-- With Z-ORDER
OPTIMIZE table_name ZORDER BY (date, customer_id);
```

### Storage Retention

```sql
-- Set retention policies
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.logRetentionDuration' = '7 days',
  'delta.deletedFileRetentionDuration' = '7 days'
);
```

## Common Patterns

### Pattern 1: Auto-Optimize

```sql
-- Enable automatic optimization
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = true,
  'delta.autoOptimize.autoCompact' = true
);
```

### Pattern 2: VACUUM Schedule

```sql
-- Weekly deep clean
VACUUM my_table RETAIN 168 HOURS;  -- 7 days
```
