---
name: streaming-cost-optimization
description: "Optimize streaming costs by reducing S3 API calls and tuning configurations."
author: "Databricks"
source: "https://www.databricksters.com/p/the-hidden-price-of-streaming-cutting"
tags: ["streaming", "cost", "s3", "optimization"]
---

# Streaming Cost Optimization

## Overview

Streaming pipelines can generate excessive S3 API calls, driving up cloud costs. Optimize by reducing frequency and batch sizes.

## Cost Breakdown

### Default Settings (500ms trigger)
- 100 S3 API calls every 500 ms = 200 calls/sec
- 17,280,000 calls/day = ~$38.71/day
- 10 pipelines = ~$387/month

## Optimization Strategies

### Strategy 1: Increase Trigger Interval

```python
# Default: 500ms
.trigger(processingTime="500 milliseconds")  # High cost

# Optimized: 2 seconds
.trigger(processingTime="2 seconds")  # 4x fewer calls
```

**Impact:** 2x reduction in S3 API cost (from ~$38.71/day to ~$19.35/day)

### Strategy 2: Use V2 Checkpointing

```sql
ALTER TABLE my_table
SET TBLPROPERTIES (
  'delta.feature.v2Checkpoint' = 'supported',
  'delta.checkpointPolicy' = 'v2'
);
```

**Benefits:**
- Stores stats in checkpoint files
- Reduces S3 GET/LIST calls
- Faster streaming reads/writes

### Strategy 3: Reduce Metadata Retention

```sql
ALTER TABLE silver SET TBLPROPERTIES (
  'delta.logRetentionDuration' = '7 days',
  'delta.deletedFileRetentionDuration' = '3 days'
);
```

**Impact:** Up to 85% smaller _delta_log directories

### Strategy 4: Increase Min Batches to Retain

```python
# Default: 100 batches in memory
spark.conf.set("spark.sql.streaming.minBatchesToRetain", "50")
```

**Reduces:** Memory pressure and metadata scans

## Recommendations by Layer

| Layer | Trigger Interval | Retention |
|-------|-----------------|-----------|
| Bronze | 2-30 seconds | 7 days |
| Silver | 1-5 minutes | 30 days |
| Gold | 5-30 minutes | 365 days |

## Common Issues

| Issue | Solution |
|-------|----------|
| High S3 API costs | Increase trigger interval |
| Metadata bloat | Reduce retention periods |
| Many small files | Enable auto-optimize |
| State store growth | Reduce minBatchesToRetain |
