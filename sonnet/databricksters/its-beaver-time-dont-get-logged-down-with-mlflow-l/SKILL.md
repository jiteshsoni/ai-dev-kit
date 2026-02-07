---
name: "Efficient MLflow Logging Patterns"
description: "Optimize MLflow logging performance by batching metrics, lazy artifact logging, and avoiding excessive logging calls."
author: "Databricksters"
url: "https://www.databricksters.com/p/its-beaver-time-dont-get-logged-down-with-mlflow-l"
date: "2025"
tags: ["mlflow", "logging", "performance", "best-practices", "databricks"]
---

# Efficient MLflow Logging

## Overview

Excessive MLflow logging slows training: log metrics in batches, log artifacts at end, avoid redundant calls. Smart logging reduces training time by 30-50% for iterative algorithms.

## Quick Start

```python
import mlflow

with mlflow.start_run():
    # ❌ BAD: Log every iteration (slow)
    # for i in range(1000):
    #     mlflow.log_metric("loss", loss, step=i)
    
    # ✅ GOOD: Batch log every N iterations
    metrics = {}
    for i in range(1000):
        loss = train_step()
        
        if i % 10 == 0:  # Log every 10 steps
            metrics[f"loss_step_{i}"] = loss
    
    mlflow.log_metrics(metrics)  # Single API call
```

## Common Patterns

### Pattern 1: Artifact Logging at End

```python
with mlflow.start_run():
    # Train model
    model = train()
    
    # Log artifacts only once at end
    mlflow.log_model(model, "model")
    mlflow.log_dict(config, "config.json")
    mlflow.log_figure(fig, "loss_curve.png")
    
    # Don't log intermediate artifacts
```

### Pattern 2: Selective Logging

```python
# Only log important metrics
mlflow.log_param("model_type", "xgboost")
mlflow.log_metric("final_accuracy", 0.95)

# Skip verbose intermediate values
```

## FAQ

**Q: How often should I log metrics?**  
A: Every 10-100 steps for training loops. Only final metrics for short jobs.

**Q: Does logging affect model training?**  
A: Yes, excessive logging creates API overhead. Batch when possible.
