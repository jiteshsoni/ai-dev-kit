---
name: time-travel
description: "Query historical data versions using Delta Lake time travel capabilities."
tags: ["delta", "time-travel", "history", "versioning"]
---

# Time Travel

## Overview

Query previous versions of data using version numbers or timestamps.

## Quick Start

```python
# Query by version
spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("/path/to/table")

# Query by timestamp
spark.read.format("delta") \
    .option("timestampAsOf", "2025-01-01") \
    .load("/path/to/table")
```

## SQL Syntax

```sql
-- Version
SELECT * FROM table@v0;

-- Timestamp
SELECT * FROM table TIMESTAMP AS OF '2025-01-01';

-- Between versions
DESCRIBE HISTORY table;
```

## Common Patterns

### Pattern 1: Audit Changes

```python
# Compare current vs previous
previous = spark.read.format("delta").option("versionAsOf", 0).table("events")
current = spark.table("events")

# Find differences
previous.exceptAll(current).show()
```

### Pattern 2: Restore Table

```sql
-- Restore to previous version
RESTORE TABLE events TO VERSION AS OF 0;
```
