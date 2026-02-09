---
name: model-serving
description: "Deploy MLflow models and AI agents to real-time and batch serving endpoints."
tags: ["model-serving", "endpoints", "deployment", "inference"]
---

# Model Serving

## Overview

Deploy MLflow models as REST endpoints for real-time inference on Databricks.

## Quick Start

### Deploy via UI

```python
# 1. Register model in Unity Catalog
mlflow.set_registry_uri("databricks-uc")
mlflow.register_model(model_uri, "catalog.schema.model")

# 2. Create serving endpoint (UI or API)
```

### Query Endpoint

```python
import requests

# Query serving endpoint
response = requests.post(
    f"{host}/serving-endpoints/{endpoint_name}/invocations",
    headers={"Authorization": f"Bearer {token}"},
    json={"inputs": [{"feature1": value1, "feature2": value2}]}
)
```

## Common Patterns

### Pattern 1: AI_QUERY from SQL

```sql
-- Real-time inference in SQL
SELECT
    customer_id,
    ai_query(
        endpoint => 'churn_model',
        request => named_struct(
            'account_age', account_age,
            'spend', monthly_spend
        ),
        returnType => 'DOUBLE'
    ) AS churn_score
FROM customers;
```

### Pattern 2: Batch Inference

```sql
-- Batch scoring
CREATE TABLE predictions AS
SELECT *,
    ai_query(
        endpoint => 'model_endpoint',
        request => named_struct('input', features)
    ) AS prediction
FROM input_table;
```

### Pattern 3: PyFunc Custom Model

```python
import mlflow

class CustomModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        self.model = load_model(context.artifacts["model"])
    
    def predict(self, context, model_input):
        return self.model.predict(model_input)

# Log and deploy
mlflow.pyfunc.log_model(
    artifact_path="model",
    python_model=CustomModel(),
    artifacts={"model": model_path}
)
```

## Endpoint Types

| Type | Latency | Use Case |
|------|---------|----------|
| Real-time | <100ms | Interactive apps |
| Batch | Minutes | Large-scale scoring |
