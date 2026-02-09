---
name: checkpoint-optimization
description: "Checkpoint location organization and performance tuning."
tags: ["checkpoint", "streaming", "optimization"]
---

# Checkpoint Optimization

## Overview

Organize checkpoints for better performance and manageability.

## Best Practices

### Target-Tied Organization

```python
def get_checkpoint_location(table_name):
    """Checkpoint tied to target, not source"""
    return f"/Volumes/catalog/checkpoints/{table_name}"
```

**Why?** Checkpoint already contains source information.

### Storage Requirements

| DO | DON'T |
|----|-------|
| Use Unity Catalog volumes | Use DBFS (ephemeral) |
| Use S3/ADLS-backed storage | Share checkpoints between streams |
| Unique checkpoint per stream | Store in temporary locations |

### Monitoring

```python
# Track checkpoint size
# Alert on access failures
# Monitor state store growth
```
