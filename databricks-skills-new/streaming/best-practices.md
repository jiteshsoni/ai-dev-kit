---
name: streaming-best-practices
description: "Production checklist and best practices for Spark Structured Streaming pipelines."
tags: ["spark-streaming", "production", "checklist", "best-practices"]
---

# Streaming Best Practices

## Production Checklist

### Cluster Configuration
- [ ] Fixed-size cluster (no autoscaling for streaming)
- [ ] Sufficient cores for Kafka partitions
- [ ] Checkpoint in persistent storage (S3/ADLS)

### Code Quality
- [ ] Unique checkpoint per stream
- [ ] Target-tied checkpoint organization
- [ ] Watermarks for stateful operations
- [ ] Error handling in ForEachBatch
- [ ] Monitoring hooks

### Monitoring
- [ ] Input Rate vs Processing Rate
- [ ] Max Offsets Behind Latest
- [ ] Batch Duration vs Trigger Interval
- [ ] State Store Size
- [ ] Alerting for lag increase

## Key Guidelines

### Trigger Interval
```python
# Guideline: SLA / 3
# Example: 1 hour SLA → 20 minute trigger
.trigger(processingTime="20 minutes")
```

### Join Recommendations
```python
# Stream-Static: Use left join
stream.join(dim, "key", "left")

# Stream-Stream: Use watermarks
stream1.withWatermark("ts", "10 min").join(stream2.withWatermark("ts", "10 min"))
```

### Checkpoint Location
```python
# Target-tied checkpoint
checkpoint = f"/Volumes/catalog/checkpoints/{target_table}"
```
