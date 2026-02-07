---
name: "How Spark Structured Streaming Recovers After Failures"
description: "Deep dive into Spark Streaming failure recovery mechanisms, exactly-once semantics, and checkpoint-based fault tolerance."
author: "Canadian Data Guy"
url: "https://www.youtube.com/watch?v=Piw5dujnsqU"
date: "2025-11-29"
tags: ["spark-streaming", "fault-tolerance", "recovery", "exactly-once", "checkpoint"]
---

# Spark Streaming Recovery Mechanisms

## Overview

Spark Structured Streaming provides robust fault tolerance through checkpointing and exactly-once processing semantics. Understanding recovery mechanisms is essential for production reliability.

**Key Mechanism**: Offset files (intent) + Commit files (confirmation) + Delta idempotency

## Quick Start

### How Recovery Works

```python
# 1. Stream writes offset file (intent to process batch N)
# 2. Stream processes data
# 3. Stream writes commit file (confirmation)
# 4. Next batch: offset N+1

# If crash occurs between 2 and 3:
# - Offset N exists
# - Commit N missing
# - On restart: reprocess batch N
# - Delta deduplication prevents duplicates
```

### Exactly-Once with Delta

```python
# Delta provides idempotent writes via:
# - Query ID (from checkpoint metadata)
# - Epoch ID (batch ID)

# Example Delta log entry:
# {
#   "queryId": "stream-uuid-from-metadata",
#   "epochId": 223,
#   "addedFiles": [...]
# }

# If same (queryId, epochId) writes again:
# - Delta detects duplicate
# - Skips write operation
```

## Common Patterns

### Pattern 1: Normal Operation Flow

```python
# Visual flow:
# 
# Offset 0  →  Process  →  Commit 0
#    ↓
# Offset 1  →  Process  →  Commit 1
#    ↓
# Offset 2  →  Process  →  Commit 2

# Each offset has matching commit
# Stream progresses normally
```

### Pattern 2: Crash Recovery

```python
# Crash scenario:
# 
# Offset 0  →  Process  →  Commit 0  ✓
#    ↓
# Offset 1  →  Process  →  CRASH  ✗
#    ↓
# [Restart]
#    ↓
# Check: Commit 1 exists? No
# Action: Reprocess Offset 1
#    ↓
# Offset 1  →  Process  →  Commit 1  ✓

# No data loss, no duplicates (Delta handles it)
```

### Pattern 3: Verify Recovery

```python
# Check Delta table history
spark.sql("DESCRIBE HISTORY target_table").show()

# Look for:
# - Operation: STREAMING UPDATE
# - Parameters: queryId, epochId
# - Multiple entries for same epoch = recovery happened
```

## Reference Files

### Checkpoint Recovery State Machine

```
States:
├── STARTUP
│   └── Read latest offset file
├── CHECK_COMMIT
│   ├── Commit exists → PROCEED
│   └── Commit missing → RECOVER
├── RECOVER
│   └── Reprocess batch (Delta deduplicates)
└── PROCEED
    └── Start next batch
```

### Delta Transaction Log

```json
{
  "commitInfo": {
    "operation": "STREAMING UPDATE",
    "operationParameters": {
      "queryId": "e5f6a7b8-c9d0...",
      "epochId": "223",
      "outputMode": "Append"
    },
    "isBlindAppend": true
  }
}
```

## Common Issues

| Issue | Solution |
|-------|----------|
| **Duplicate data in target** | Check if target is Delta; non-Delta sinks may duplicate |
| **Stuck at same offset** | Check for infinite reprocessing; verify data quality |
| **Long recovery time** | Large batches take longer; tune maxOffsetsPerTrigger |
| **Checkpoint corruption** | Delete checkpoint and restart (reprocesses from beginning) |

## Advanced Tips

### Offset Semantics

```python
# Offset file content:
{
  "startOffset": {"topic": {"0": 100}},  # Inclusive
  "endOffset": {"topic": {"0": 200}}     # Exclusive!
}

# Processes: offsets 100, 101, ..., 199
# Does NOT process: offset 200

# Next batch:
# startOffset = 200 (previous endOffset)
```

### Monitoring Recovery

```python
# Signs recovery happened:
# 1. Log message: "Recovering from checkpoint..."
# 2. Same epoch ID appears twice in Delta history
# 3. Input rate spike (reprocessing)

# Good metric: Recovery time
# Time from restart to catching up with latest offset
```

### Idempotency Verification

```python
# Test your stream is idempotent:
# 1. Run stream for 10 batches
# 2. Stop stream
# 3. Delete commit files (not offsets!)
# 4. Restart stream
# 5. Verify: Row count same, no duplicates

# Expected: Exactly same result as first run
```

## FAQ

**Q: Can I lose data with Spark Streaming?**
A: No, if using Delta sink. Checkpoint + Delta provides exactly-once guarantees.

**Q: What if I lose my checkpoint?**
A: Stream restarts from beginning. No duplicates if writing to Delta.

**Q: How long does recovery take?**
A: Depends on batch size and cluster. Usually seconds to minutes.

**Q: Can I manually trigger recovery?**
A: Delete commit file for offset you want to reprocess. Stream will replay.

**Q: What's the difference between at-least-once and exactly-once?**
A: Spark Streaming + Delta = exactly-once. Idempotent sinks achieve effectively-once.