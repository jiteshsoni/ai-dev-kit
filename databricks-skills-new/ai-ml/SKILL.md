---
name: ai-ml
description: "AI/ML patterns on Databricks: Agents, Model Serving, Vector Search, MLflow, and evaluation."
tags: ["ai", "ml", "agents", "model-serving", "rag"]
---

# AI/ML on Databricks

Comprehensive guide to building and deploying AI/ML solutions on Databricks.

## Overview

Databricks provides end-to-end AI/ML capabilities from data preparation to model serving.

**Key Components:**
- **Agent Bricks**: Low-code AI agents
- **Model Serving**: Real-time and batch inference
- **Vector Search**: Semantic search and RAG
- **MLflow**: Experiment tracking and model management
- **Evaluation**: LLM judges and custom scorers

## Quick Start

### Create a Knowledge Assistant

```python
# In Databricks UI: Agent Bricks → Knowledge Assistant
# 1. Select vector index
# 2. Configure content columns
# 3. Deploy with one click
```

### Deploy Model Serving Endpoint

```python
import mlflow

# Deploy model
mlflow.set_registry_uri("databricks-uc")
model_uri = "models:/catalog.schema.model@champion"

# Create endpoint (UI or API)
```

## Available Skills

- [Agent Bricks](./agent-bricks.md) - Low-code agent building
- [Model Serving](./model-serving.md) - Deploy endpoints
- [Vector Search](./vector-search.md) - Semantic search
- [Agent Evaluation](./agent-evaluation.md) - Evaluate with MLflow
- [Custom Scorers](./custom-scorers.md) - Build LLM judges
- [Genie Spaces](./genie-spaces.md) - Natural language queries
- [Synthetic Data](./synthetic-data.md) - Generate test data
- [MLflow Tracing](./mlflow-tracing.md) - Instrument AI apps
- [Batch Inference](./batch-inference.md) - Scale predictions
- [Multi-Agent Systems](./multi-agent-systems.md) - Orchestrate agents
