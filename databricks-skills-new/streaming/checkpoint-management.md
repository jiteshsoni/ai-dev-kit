---
name: checkpoint-management
description: "Checkpoint internals, recovery procedures, and best practices for Spark Structured Streaming."
author: "Canadian Data Guy"
source: "https://www.youtube.com/watch?v=XXXXXXXXXXX"
tags: ["spark-streaming", "checkpoint", "recovery", "exactly-once"]
---

# Checkpoint Management

## Overview

Checkpoints enable exactly-once semantics and fault tolerance in Spark Structured Streaming.

## Checkpoint Structure

```
checkpoint/
├── metadata/      # Stream query ID
├── offsets/       # Intent: what to process
├── commits/       # Confirmation: what completed
├── sources/       # Source metadata
└── state/         # Stateful operations
```

## Offset Semantics

- **Start Offset (inclusive)**: First message to process
- **End Offset (exclusive)**: First message NOT to process
- Example: start=100, end=200 → processes 100-199

## Best Practices

### Target-Tied Organization

```python
def get_checkpoint_location(table_name):
    """Checkpoint tied to TARGET, not source"""
    return f"/Volumes/catalog/checkpoints/{table_name}"
```

**Why?** Checkpoint already contains source information.

### Storage Requirements

| DO | DON'T |
|----|-------|
| Use Unity Catalog volumes | Use DBFS (ephemeral) |
| Use S3/ADLS-backed storage | Share checkpoints between streams |
| Unique checkpoint per stream | Store in temporary locations |

## Recovery Scenarios

### Normal Recovery (Automatic)

```python
# On restart:
# 1. Read latest offset file
# 2. Check if corresponding commit exists
# 3. If no commit: reprocess that batch
# 4. Delta handles deduplication via queryId + epochId
```

### Lost Checkpoint

```python
# 1. Delete checkpoint folder
# 2. Restart with startingOffsets=earliest
# 3. Stream reprocesses from beginning
# 4. Delta sink handles deduplication
```
