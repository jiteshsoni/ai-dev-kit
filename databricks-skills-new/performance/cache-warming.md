---
name: cache-warming
description: "Warm SQL warehouse cache for predictable performance."
tags: ["cache", "performance", "sql", "warehouse"]
---

# Cache Warming

## Overview

Pre-warm SQL warehouse cache for consistent query performance.

## Quick Start

```sql
-- Run representative queries
SELECT * FROM fact_sales WHERE date = CURRENT_DATE() - INTERVAL 1 DAY LIMIT 1;

-- Run during off-hours
-- Cache lasts across sessions
```

## Common Patterns

### Pattern 1: Bootstrapping

```sql
-- Cache all tables at startup
SELECT COUNT(*) FROM dim_customers;
SELECT COUNT(*) FROM fact_orders;
SELECT COUNT(*) FROM dim_products;
```

### Pattern 2: Query Pattern Cache

```sql
-- Cache common query patterns
SELECT * FROM sales WHERE region = 'us-west' AND date >= CURRENT_DATE() - INTERVAL 7 DAYS;
SELECT * FROM sales WHERE region = 'eu' AND date >= CURRENT_DATE() - INTERVAL 7 DAYS;
SELECT * FROM sales WHERE region = 'apac' AND date >= CURRENT_DATE() - INTERVAL 7 DAYS;
```
