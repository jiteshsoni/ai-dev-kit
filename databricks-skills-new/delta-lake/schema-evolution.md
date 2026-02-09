---
name: schema-evolution
description: "Handle schema changes and evolution in Delta Lake tables."
tags: ["delta", "schema", "evolution", "migration"]
---

# Schema Evolution

## Overview

Delta Lake supports schema evolution for handling changing data structures.

## Quick Start

```python
# Auto-merge schema
df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save("/path/to/table")
```

```sql
-- Allow schema evolution
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name'
);
```

## Schema Evolution Modes

| Mode | Behavior |
|------|----------|
| addNewColumns | Add new columns automatically |
| rescue | Put unexpected data in _rescued_data |
| failOnNewColumns | Fail when schema changes |

## Common Patterns

### Pattern 1: Add Columns

```python
# New data with additional columns
df_with_new_cols.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save("/path/to/table")
```

### Pattern 2: Type Changes

```sql
-- Change column type
ALTER TABLE my_table ALTER COLUMN id TYPE BIGINT;
```
