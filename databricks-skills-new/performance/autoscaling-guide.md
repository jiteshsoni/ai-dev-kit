---
name: autoscaling-guide
description: "Cluster autoscaling patterns for different workload types."
tags: ["autoscaling", "clusters", "performance", "cost"]
---

# Autoscaling Guide

## Overview

Configure cluster autoscaling for optimal performance and cost.

## Quick Start

### Streaming Jobs

```python
# Streaming: Fixed size recommended
# No autoscaling for consistent resource availability
```

### Batch Jobs

```python
# Batch: Autoscaling enabled
spark.conf.set("spark.databricks.autoscaling.enabled", "true")
spark.conf.set("spark.databricks.autoscaling.minWorkers", "2")
spark.conf.set("spark.databricks.autoscaling.maxWorkers", "10")
```

## Recommendations

| Workload | Autoscaling | Reason |
|----------|------------|--------|
| Streaming | Disabled | Fixed resources needed |
| Interactive | Enabled | Varying user load |
| ETL Jobs | Enabled | Varying data volumes |
| ML Training | Enabled | Different model sizes |
