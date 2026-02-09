---
name: liquid-clustering
description: "Liquid Clustering for automatic data optimization without manual intervention."
tags: ["delta", "liquid-clustering", "optimization", "clustering"]
---

# Liquid Clustering

## Overview

Liquid Clustering provides incremental, automatic data clustering without the need for manual Z-ORDER maintenance.

## Quick Start

```sql
-- Enable Liquid Clustering
ALTER TABLE my_table 
SET TBLPROPERTIES ('delta.liquid.clustering' = true);

-- Cluster by specific columns
ALTER TABLE my_table CLUSTER BY (date, region);
```

## Benefits vs Z-ORDER

| Aspect | Z-ORDER | Liquid Clustering |
|--------|---------|-------------------|
| Maintenance | Manual OPTIMIZE | Automatic/Incremental |
| Re-clustering | May reprocess existing | Only new data |
| Cost | Higher (full rewrites) | Lower (incremental) |
| Predictability | Variable | Consistent |

## Common Patterns

### Pattern 1: Enable on Existing Table

```sql
-- Convert existing table
ALTER TABLE existing_table 
SET TBLPROPERTIES ('delta.liquid.clustering' = true);

-- Re-cluster existing data
OPTIMIZE existing_table;
```

### Pattern 2: Create with Clustering

```sql
CREATE TABLE events (
    id STRING,
    timestamp TIMESTAMP,
    region STRING
) USING DELTA
CLUSTER BY (region, date(timestamp));
```

### Pattern 3: Combine with Other Features

```sql
-- Liquid + DV + RLC for optimal streaming
ALTER TABLE target_table SET TBLPROPERTIES (
  'delta.liquid.clustering' = true,
  'delta.enableDeletionVectors' = true,
  'delta.enableRowLevelConcurrency' = true
);
```
