---
name: stream-static-joins
description: "Enrich streaming data with Delta dimension tables using stream-static joins."
author: "Canadian Data Guy"
source: "https://www.youtube.com/watch?v=XXXXXXXXXXX"
tags: ["spark-streaming", "joins", "dimension-table", "enrichment"]
---

# Stream-Static Joins

## Overview

Enrich streaming data with slowly-changing dimension tables. The Delta table is automatically refreshed each microbatch.

## Quick Start

```python
# Stream of events
events = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "events")
    .load()
)

# Static dimension (refreshed each microbatch)
dimensions = spark.table("dimension.users")

# Left join for production
enriched = events.join(dimensions, "user_id", "left")
```

## Why Left Join?

| Join Type | Behavior | Use Case |
|-----------|----------|----------|
| Inner | Drops events with no match | When dimension must exist |
| Left | Preserves all events | Production (never lose data) |

## Delta vs Non-Delta

```python
# Delta table: Refreshed each microbatch
dim = spark.table("dimensions")  # ✅ Refreshes automatically

# Non-Delta: Read once at startup
dim = spark.read.parquet("path")  # ❌ Never refreshes
```

## Common Patterns

### Pattern 1: User Enrichment

```python
events = spark.readStream.table("events")
users = spark.table("dimension.users")

enriched = (events
    .join(users, "user_id", "left")
    .select("event_id", "user_id", "user_name", "user_segment", "event_type")
)
```

### Pattern 2: Multi-Dimension Join

```python
events = spark.readStream.table("events")
users = spark.table("dim.users")
products = spark.table("dim.products")

enriched = (events
    .join(users, "user_id", "left")
    .join(products, "product_id", "left")
)
```
