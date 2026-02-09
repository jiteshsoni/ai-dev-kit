---
name: vacuum-strategies
description: "Delta Lake VACUUM strategies for storage cost management: Full, Light, and Inventory-based cleanup."
author: "Canadian Data Guy"
source: "https://www.youtube.com/watch?v=Ijde5wDzX5A"
tags: ["delta", "vacuum", "storage", "cost-optimization"]
---

# VACUUM Strategies

## Overview

VACUUM removes old files to reduce storage costs while preserving time travel capabilities.

## Quick Start

```sql
-- Default: Remove files older than 7 days
VACUUM delta_table;

-- Retain 30 days
VACUUM delta_table RETAIN 720 HOURS;

-- Dry run
VACUUM delta_table DRY RUN;
```

## VACUUM Types

| Type | Speed | Use Case |
|------|-------|----------|
| **FULL** | Slower | Deep clean |
| **LIGHT** | Fast | Daily maintenance |
| **INVENTORY** | Scalable | Petabyte scale |

```sql
-- VACUUM LIGHT (requires recent FULL)
VACUUM delta_table LIGHT;
```

## Common Patterns

### Pattern 1: Retention by Layer

```sql
-- Bronze: Short retention
VACUUM bronze RETAIN 24 HOURS;

-- Silver: Medium retention
VACUUM silver RETAIN 168 HOURS;  -- 7 days

-- Gold: Longer retention
VACUUM gold RETAIN 720 HOURS;  -- 30 days

-- Compliance tables
VACUUM audit RETAIN 2555 HOURS;  -- 365 days
```

### Pattern 2: Automated Schedule

```sql
-- Monday: FULL VACUUM
VACUUM prod_table;

-- Tuesday-Sunday: LIGHT VACUUM
VACUUM prod_table LIGHT;
```

### Pattern 3: Safe Validation

```sql
-- Always dry run first
VACUUM prod_table RETAIN 168 HOURS DRY RUN;

-- Review output, then execute
VACUUM prod_table RETAIN 168 HOURS;
```

## Common Issues

| Issue | Solution |
|-------|----------|
| LIGHT fails | Run FULL first |
| Still high storage | Check retention period |
| Long VACUUM times | Use LIGHT regularly |
| Accidental deletion | Use DRY RUN first |
