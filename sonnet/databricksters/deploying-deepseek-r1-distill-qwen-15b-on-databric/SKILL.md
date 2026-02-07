---
name: "Deploy DeepSeek-R1 Models on Databricks"
description: "Deploy DeepSeek-R1 distilled models (Qwen, Llama) on Databricks Model Serving for cost-efficient reasoning tasks."
author: "Databricksters"
url: "https://www.databricksters.com/p/deploying-deepseek-r1-distill-qwen-15b-on-databric"
date: "2025"
tags: ["deepseek", "llm", "model-serving", "deployment", "databricks"]
---

# Deploy DeepSeek-R1 Models

## Overview

DeepSeek-R1 distilled models (Qwen-1.5B/7B, Llama-8B/70B) provide strong reasoning capabilities at lower cost than original R1 model. Deploy to Databricks Model Serving with vLLM for production inference. Ideal for tasks requiring chain-of-thought reasoning: math, coding, logic.

**Use this skill when:** Deploying reasoning-focused LLMs, optimizing inference costs, or replacing larger models with distilled alternatives.

## Quick Start

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ServedEntityInput

w = WorkspaceClient()

# Deploy DeepSeek-R1-Distill-Qwen-1.5B
endpoint = w.serving_endpoints.create(
    name="deepseek-r1-qwen-1.5b",
    config={
        "served_entities": [{
            "entity_name": "system.ai.deepseek_r1_distill_qwen_1_5b",
            "scale_to_zero_enabled": True,
            "workload_size": "Small",
            "workload_type": "GPU_SMALL"
        }]
    }
)

# Query endpoint
response = w.serving_endpoints.query(
    name="deepseek-r1-qwen-1.5b",
    inputs=[{
        "prompt": "Solve: If x + 2 = 5, what is x?"
    }]
)
```

## Common Patterns

### Pattern 1: Deploy from HuggingFace

```python
import mlflow
from transformers import AutoModelForCausalLM, AutoTokenizer

# Load DeepSeek model
model_name = "deepseek-ai/DeepSeek-R1-Distill-Qwen-1.5B"
model = AutoModelForCausalLM.from_pretrained(model_name)
tokenizer = AutoTokenizer.from_pretrained(model_name)

# Log to MLflow
with mlflow.start_run():
    mlflow.transformers.log_model(
        transformers_model={
            "model": model,
            "tokenizer": tokenizer
        },
        artifact_path="model",
        task="text-generation"
    )
    model_uri = mlflow.get_artifact_uri("model")

# Register to Unity Catalog
mlflow.register_model(model_uri, "catalog.schema.deepseek_r1_qwen")

# Deploy
w.serving_endpoints.create(
    name="deepseek-reasoning",
    config={
        "served_models": [{
            "model_name": "catalog.schema.deepseek_r1_qwen",
            "model_version": "1",
            "scale_to_zero_enabled": True,
            "workload_size": "Medium",
            "workload_type": "GPU_MEDIUM"
        }]
    }
)
```

### Pattern 2: Compare Model Sizes

```python
# Model selection guide:
models = {
    "DeepSeek-R1-Distill-Qwen-1.5B": {
        "params": "1.5B",
        "workload": "GPU_SMALL",
        "cost": "$",
        "use_case": "Simple reasoning, high throughput"
    },
    "DeepSeek-R1-Distill-Qwen-7B": {
        "params": "7B",
        "workload": "GPU_MEDIUM",
        "cost": "$$",
        "use_case": "Balanced reasoning and cost"
    },
    "DeepSeek-R1-Distill-Llama-8B": {
        "params": "8B",
        "workload": "GPU_MEDIUM",
        "cost": "$$",
        "use_case": "General reasoning tasks"
    },
    "DeepSeek-R1-Distill-Llama-70B": {
        "params": "70B",
        "workload": "GPU_LARGE_2",
        "cost": "$$$$",
        "use_case": "Complex reasoning, highest quality"
    }
}
```

## FAQ

**Q: Which DeepSeek model should I use?**  
A: 1.5B for simple reasoning + cost. 7B/8B for balanced. 70B for complex tasks.

**Q: How does performance compare to GPT-4?**  
A: R1-70B approaches GPT-4 on reasoning benchmarks. Smaller models lag but cost much less.

**Q: Can I fine-tune DeepSeek models?**  
A: Yes, standard fine-tuning workflows work. But distilled models often work well zero-shot.
