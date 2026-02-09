---
name: stream-stream-joins
description: "Deep dive on Spark Structured Streaming stream-stream joins with watermarks, temporal conditions, and state management."
author: "Canadian Data Guy"
source: "https://www.youtube.com/watch?v=XXXXXXXXXXX"
tags: ["spark-streaming", "joins", "stateful", "watermark", "event-time"]
---

# Stream-Stream Joins

## Overview

Join two unbounded streaming sources with event-time semantics. Requires careful handling of state and time bounds.

## Key Concepts

| Aspect | Stream-Static | Stream-Stream |
|--------|--------------|---------------|
| State | Stateless | Stateful |
| Memory | Current batch | Buffered windows |
| Latency | Immediate | Window-dependent |
| Complexity | Simple | Requires watermarking |

## Quick Start

### Inner Join with Watermarks

```python
from pyspark.sql.functions import expr

# Both streams need watermarks
impressions = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "impressions")
    .load()
    .select("ad_id", "impression_time")
    .withWatermark("impression_time", "10 minutes")
)

clicks = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "clicks")
    .load()
    .select("ad_id", "click_time")
    .withWatermark("click_time", "10 minutes")
)

# Join with time bounds
joined = impressions.join(
    clicks,
    expr("""
        impressions.ad_id = clicks.ad_id AND
        clicks.click_time BETWEEN impressions.impression_time AND
                                impressions.impression_time + interval 1 hour
    """),
    "inner"
)
```

### Left Outer Join

```python
# Left outer results emitted after watermark expires
joined = impressions.join(
    clicks,
    expr("""
        impressions.ad_id = clicks.ad_id AND
        clicks.click_time BETWEEN impressions.impression_time AND
                                impressions.impression_time + interval 30 minutes
    """),
    "leftOuter"
)
```

## Common Patterns

### Pattern 1: Time-Range Join

```python
# Match events within 5 minutes of each other
joined = (stream1
    .join(
        stream2,
        expr("""
            stream1.user_id = stream2.user_id AND
            stream2.ts >= stream1.ts - interval 5 minutes AND
            stream2.ts <= stream1.ts + interval 5 minutes
        """),
        "inner"
    )
)
```

### Pattern 2: Multi-Stream Joins

```python
# Chain joins carefully
step1 = (stream1
    .withWatermark("ts", "10 min")
    .join(stream2.withWatermark("ts", "10 min"), "key")
)

final = step1.join(stream3.withWatermark("ts", "10 min"), "key")
```

## Common Pitfalls

| Pitfall | Solution |
|---------|----------|
| Missing watermarks | Always define `.withWatermark()` on both sides |
| Unbounded time range | Use `BETWEEN` with explicit bounds |
| State growing forever | Tune watermark duration |
| Different time zones | Standardize to UTC |

## Tuning Checklist

- [ ] Watermark duration matches late data SLA
- [ ] Time range bounds are explicit
- [ ] State store size monitored
- [ ] Output mode is "append"
- [ ] Checkpoint location is unique
