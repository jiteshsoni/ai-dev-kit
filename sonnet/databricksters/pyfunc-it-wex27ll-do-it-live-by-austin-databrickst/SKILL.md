---
name: "Custom PyFunc Models for Production"
description: "Deploy custom Python models to Model Serving using MLflow PyFunc for preprocessing, multi-model inference, and complex logic."
author: "Austin"
url: "https://www.databricksters.com/p/pyfunc-it-wex27ll-do-it-live-by-austin-databrickst"
date: "2025"
tags: ["pyfunc", "mlflow", "model-serving", "deployment", "databricks"]
---

# Custom PyFunc Models

## Overview

MLflow PyFunc enables deploying arbitrary Python code as models: custom preprocessing, ensemble models, multi-step pipelines, or API integrations. Implement `predict()` method and package dependencies for serverless deployment.

**Use this skill when:** Deploying models with custom logic, preprocessing, or multi-model orchestration.

## Quick Start

```python
import mlflow
from mlflow.pyfunc import PythonModel

class CustomModel(PythonModel):
    def load_context(self, context):
        # Load artifacts once at startup
        self.model = mlflow.sklearn.load_model(context.artifacts["model"])
        self.scaler = joblib.load(context.artifacts["scaler"])
    
    def predict(self, context, model_input):
        # Custom preprocessing
        scaled_input = self.scaler.transform(model_input)
        # Model inference
        predictions = self.model.predict(scaled_input)
        # Custom postprocessing
        return {"predictions": predictions.tolist(), "confidence": 0.95}

# Log custom model
with mlflow.start_run():
    mlflow.pyfunc.log_model(
        "model",
        python_model=CustomModel(),
        artifacts={"model": "model.pkl", "scaler": "scaler.pkl"}
    )
```

## Common Patterns

### Pattern 1: Multi-Model Ensemble

```python
class EnsembleModel(PythonModel):
    def load_context(self, context):
        self.model1 = mlflow.sklearn.load_model(context.artifacts["model1"])
        self.model2 = mlflow.keras.load_model(context.artifacts["model2"])
    
    def predict(self, context, model_input):
        pred1 = self.model1.predict(model_input)
        pred2 = self.model2.predict(model_input)
        return (pred1 + pred2) / 2  # Average
```

### Pattern 2: API Integration

```python
class APIAugmentedModel(PythonModel):
    def load_context(self, context):
        self.model = mlflow.sklearn.load_model(context.artifacts["model"])
        self.api_key = context.artifacts["api_key"]
    
    def predict(self, context, model_input):
        # Enrich with external API
        enriched = call_external_api(model_input, self.api_key)
        return self.model.predict(enriched)
```

## FAQ

**Q: What are common use cases?**  
A: Custom preprocessing, ensemble models, feature engineering, API calls, complex business logic.

**Q: How to handle dependencies?**  
A: Specify in `conda_env` or `pip_requirements` when logging model.
