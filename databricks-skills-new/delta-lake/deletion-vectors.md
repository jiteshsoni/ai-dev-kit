---
name: deletion-vectors
description: "Deletion Vectors for efficient updates and deletes without full file rewrites."
tags: ["delta", "deletion-vectors", "updates", "performance"]
---

# Deletion Vectors

## Overview

Deletion Vectors enable soft deletes, avoiding expensive file rewrites for UPDATE and DELETE operations.

## Quick Start

```sql
-- Enable Deletion Vectors
ALTER TABLE my_table 
SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
```

## How It Works

```
Without DV:
┌─────────────┐     Update      ┌─────────────┐
│ File A      │  ─────────→  │ File A'     │
│ 1000 rows   │   (rewrite)    │ 1000 rows   │
└─────────────┘                └─────────────┘

With DV:
┌─────────────┐     Update      ┌─────────────┐
│ File A      │  ─────────→  │ File A      │
│ 1000 rows   │                │ 1000 rows   │
└─────────────┘                │ + DV file   │
                               │ (10 rows    │
                               │  marked)    │
                               └─────────────┘
```

## Common Patterns

### Pattern 1: High-Update Workloads

```sql
-- Scenario: 10% updates, 90% inserts
CREATE TABLE high_update (
    id STRING,
    value STRING
) USING DELTA
TBLPROPERTIES ('delta.enableDeletionVectors' = true);
```

### Pattern 2: Merge Performance

```python
# MERGE benefits significantly from DV
spark.sql("""
    MERGE INTO target t
    USING source s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

## Limitations

- DV files are small bitmaps (minimal overhead)
- Some operations may still require full rewrites
- Periodically run OPTIMIZE to apply DV changes
