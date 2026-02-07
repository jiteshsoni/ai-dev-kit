---
name: "Multi-Model Serving Endpoints"
description: "Serve multiple models from single endpoint using routing logic, A/B testing, or multi-stage inference pipelines."
author: "Databricksters"
url: "https://www.databricksters.com/p/juggling-multiple-models-in-a-single-serving-endpo"
date: "2025"
tags: ["model-serving", "endpoints", "ab-testing", "mlflow", "databricks"]
---

# Multi-Model Serving Endpoints

## Overview

Single serving endpoint can host multiple models for A/B testing, staged rollouts, or multi-stage pipelines. Traffic split configurations enable gradual model updates and champion/challenger patterns.

## Quick Start

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Create endpoint with multiple models
endpoint = w.serving_endpoints.create(
    name="multi-model-endpoint",
    config={
        "served_models": [
            {
                "model_name": "catalog.schema.model_v1",
                "model_version": "1",
                "scale_to_zero_enabled": True,
                "workload_size": "Small",
                "traffic_percentage": 80  # Champion
            },
            {
                "model_name": "catalog.schema.model_v2",
                "model_version": "1",
                "scale_to_zero_enabled": True,
                "workload_size": "Small",
                "traffic_percentage": 20  # Challenger
            }
        ],
        "traffic_config": {
            "routes": [
                {"served_model_name": "model_v1-1", "traffic_percentage": 80},
                {"served_model_name": "model_v2-1", "traffic_percentage": 20}
            ]
        }
    }
)
```

## Common Patterns

### Pattern 1: A/B Testing

```python
# Gradually shift traffic to new model
def update_traffic(endpoint_name, v1_pct, v2_pct):
    w.serving_endpoints.update_config(
        name=endpoint_name,
        served_models=[...],
        traffic_config={
            "routes": [
                {"served_model_name": "v1", "traffic_percentage": v1_pct},
                {"served_model_name": "v2", "traffic_percentage": v2_pct}
            ]
        }
    )

# Week 1: 90/10 split
update_traffic("endpoint", 90, 10)

# Week 2: 50/50 split
update_traffic("endpoint", 50, 50)

# Week 3: Full cutover
update_traffic("endpoint", 0, 100)
```

## FAQ

**Q: How to route by request attributes?**  
A: Not directly supported. Use PyFunc wrapper for custom routing logic.
