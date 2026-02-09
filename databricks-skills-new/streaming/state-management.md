---
name: state-management
description: "Stateful operations in Spark Streaming: watermarks, aggregations, and state store management."
tags: ["spark-streaming", "state", "watermark", "aggregation"]
---

# State Management

## Overview

Stateful operations maintain data across microbatches for aggregations, joins, and deduplication.

## Watermarking

```python
# Event-time processing with watermarks
windowed = (df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("user_id")
    )
    .agg(sum("amount"))
)
```

**Watermark Purpose:**
- Defines late data threshold
- Triggers state expiration
- Reduces state store size

## State Store Monitoring

```python
# Read state store directly
state_df = spark.read.format("statestore").load("/checkpoint/state")

# Check partition balance
state_df.groupBy("partitionId").count().show()

# State metadata
spark.read.format("state-metadata").load("/checkpoint").show()
```

## Choosing Watermark Duration

```python
# Trade-off: Late data tolerance vs state size
.withWatermark("event_time", "2 hours")    # High tolerance, large state
.withWatermark("event_time", "10 minutes") # Low tolerance, small state

# Rule of thumb: Max expected delay + safety margin
```
