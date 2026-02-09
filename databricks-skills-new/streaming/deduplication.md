---
name: deduplication
description: "Exactly-once processing and deduplication strategies in Spark Structured Streaming."
tags: ["spark-streaming", "deduplication", "exactly-once", "state"]
---

# Deduplication

## Overview

Remove duplicate events using built-in `dropDuplicates` or manual state management.

## Built-in Deduplication

```python
# Simple deduplication by key
deduped = df.dropDuplicates(["event_id"])

# With watermark (recommended for streaming)
deduped = (df
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["event_id"])
)

# Composite key
deduped = (df
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["user_id", "event_id"])
)
```

## How It Works

1. State stores seen keys with timestamp
2. Keys expire after watermark duration
3. Newer duplicates within window are dropped

## Delta Idempotent Writes

```python
# For exactly-once in forEachBatch:
df.write \
    .format("delta") \
    .option("txnVersion", batch_id) \
    .option("txnAppId", "my_stream_job") \
    .mode("append") \
    .saveAsTable("target")
```
