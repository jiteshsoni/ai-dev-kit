---
name: error-recovery
description: "Failure handling, recovery procedures, and resilience patterns for streaming pipelines."
tags: ["spark-streaming", "recovery", "resilience", "failure-handling"]
---

# Error Recovery

## Overview

Handle failures gracefully with automatic recovery and manual intervention procedures.

## Automatic Recovery

```python
# Spark automatically recovers:
# 1. Reads latest offset from checkpoint
# 2. Checks if corresponding commit exists
# 3. Reprocesses if commit missing
# 4. Delta handles deduplication
```

## Common Failure Scenarios

### Scenario 1: Transient Failures

```python
# Spark retries automatically
# Configure retry policy:
spark.conf.set("spark.task.maxFailures", "4")
```

### Scenario 2: Schema Mismatch

```python
# Handle evolving schemas
(df
    .writeStream
    .option("mergeSchema", "true")
    .start("/delta/table")
)
```

### Scenario 3: Lost Checkpoint

```python
# Recovery procedure:
# 1. Delete checkpoint folder
# 2. Restart with startingOffsets=earliest
# 3. Reprocess all data (idempotent with Delta)
```

## Monitoring

```python
# Get stream status
for stream in spark.streams.active:
    print(stream.status)
    print(stream.lastProgress)
```
