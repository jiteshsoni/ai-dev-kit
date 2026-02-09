---
name: cdc-patterns
description: "Change Data Capture patterns with Delta Lake for tracking data changes."
tags: ["delta", "cdc", "change-data-capture", "scd"]
---

# CDC Patterns

## Overview

Implement Change Data Capture for tracking inserts, updates, and deletes.

## Quick Start

```python
# CDC with MERGE
spark.sql("""
    MERGE INTO target t
    USING (
        SELECT *,
            CASE WHEN _change_type = 'delete' THEN 1 ELSE 0 END as is_deleted
        FROM cdc_source
    ) s ON t.id = s.id
    WHEN MATCHED AND s.is_deleted = 1 THEN DELETE
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

## Common Patterns

### Pattern 1: SCD Type 2

```sql
-- Track historical changes
CREATE TABLE dim_customer (
    customer_id STRING,
    name STRING,
    email STRING,
    effective_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN
) USING DELTA;

-- Merge with SCD Type 2 logic
MERGE INTO dim_customer t
USING (SELECT * FROM staging) s
ON t.customer_id = s.customer_id AND t.is_current = true
WHEN MATCHED AND t.email != s.email THEN
    UPDATE SET is_current = false, end_date = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, effective_date, end_date, is_current)
    VALUES (s.customer_id, s.name, s.email, current_timestamp(), null, true);
```
