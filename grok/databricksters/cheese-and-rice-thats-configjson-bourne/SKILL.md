---
name: "fine-tuned-llama-provisioned-throughput"
description: "Deploy fine-tuned Llama models to Databricks Provisioned Throughput endpoints: Unsloth fine-tuning, config.json fixes, generation_config.json requirements, and MLflow registration."
---

# Fine-Tuned Llama Models on Provisioned Throughput: config.json Fixes

## Overview

This skill covers deploying fine-tuned Llama models (using Unsloth) to Databricks Provisioned Throughput endpoints. Learn how to fix config.json issues, add required generation_config.json, merge LoRA adapters back to base weights, and properly register models to Unity Catalog for serving. Includes fixes for security check failures and model deployment errors.

## Quick Start

### Fine-Tune Model with Unsloth
Fine-tune Llama model on Serverless GPU Compute:

```python
from unsloth import FastLanguageModel
import torch
from datasets import Dataset
from transformers import TrainingArguments, Trainer

# Load base model
model, tokenizer = FastLanguageModel.from_pretrained(
    model_name="unsloth/llama-3.1-8b",
    max_seq_length=2048,
    dtype=torch.bfloat16,
    load_in_4bit=True,  # Fastest + lowest memory
)

# Prepare data
data = [
    {"text": "### Instruction: Say hello politely.\n### Response: Hello! How may I help you?"},
    {"text": "### Instruction: Explain PEFT.\n### Response: A lightweight way to fine-tune large models."},
]

dataset = Dataset.from_list(data)

# Tokenize
def tokenize(example):
    encoding = tokenizer(
        example["text"],
        truncation=True,
        max_length=1024,
        padding="max_length",
    )
    encoding["labels"] = encoding["input_ids"].copy()
    return encoding

tokenized_dataset = dataset.map(tokenize)

# Add LoRA adapters
model = FastLanguageModel.get_peft_model(
    model,
    r=8,
    lora_alpha=16,
    lora_dropout=0.0,
    target_modules=["q_proj", "k_proj", "v_proj", "o_proj"],
)

# Train
training_args = TrainingArguments(
    output_dir="outputs",
    per_device_train_batch_size=1,
    gradient_accumulation_steps=1,
    warmup_steps=0,
    max_steps=10,
    learning_rate=5e-5,
    logging_steps=5,
    optim="adamw_torch",
    bf16=True,
    remove_unused_columns=False,
)

trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=tokenized_dataset,
)

trainer.train()
```

### Fix config.json and Deploy
Prepare model for Provisioned Throughput:

```python
import json
import os
import shutil
import subprocess
import mlflow

LOCAL_TEMP_PATH = "/tmp/llama_merged_model"

# Step 1: Merge adapter back to base weights (16-bit)
model.save_pretrained_merged(
    LOCAL_TEMP_PATH,
    tokenizer=tokenizer,
    save_method="merged_16bit",
    safe_serialization=True  # Force safetensors
)

# Step 2: Fix config.json _name_or_path (CRITICAL!)
config_path = os.path.join(LOCAL_TEMP_PATH, "config.json")

with open(config_path, "r") as f:
    config = json.load(f)

# Rename to avoid security check failure
config["_name_or_path"] = "unsloth/Meta-Llama-3.1-8B"

with open(config_path, "w") as f:
    json.dump(config, f, indent=2)

print("config.json fixed!")

# Step 3: Add generation_config.json (REQUIRED!)
gen_config = {
    "bos_token_id": 128000,
    "eos_token_id": 128001,
    "pad_token_id": 128004,
    "do_sample": True,
    "temperature": 0.6,
    "max_length": 8192
}

with open(os.path.join(LOCAL_TEMP_PATH, "generation_config.json"), "w") as f:
    json.dump(gen_config, f, indent=2)

print("generation_config.json added!")

# Step 4: Copy to UC Volume
UC_VOLUME_PATH = "/Volumes/catalog/schema/volume_name/merged_weights"

if os.path.exists(UC_VOLUME_PATH):
    subprocess.run(['rm', '-rf', UC_VOLUME_PATH], check=True)

os.makedirs(UC_VOLUME_PATH, exist_ok=True)
subprocess.run(["cp", "-r", f"{LOCAL_TEMP_PATH}/.", UC_VOLUME_PATH], check=True)

# Step 5: Register to MLflow
mlflow.set_registry_uri("databricks-uc")

input_example = {
    "messages": [
        {"role": "user", "content": "Hello!"}
    ]
}

CATALOG = "catalog"
SCHEMA = "schema"
REGISTERED_NAME = f"{CATALOG}.{SCHEMA}.llama_3_1_8b_custom"

with mlflow.start_run(run_name="register_llama_3_1") as run:
    model_info = mlflow.transformers.log_model(
        transformers_model=UC_VOLUME_PATH,
        artifact_path="model",
        task="llm/v1/chat",
        input_example=input_example,
        registered_model_name=REGISTERED_NAME,
        metadata={
            "source": "uc_volume",
            "original_path": UC_VOLUME_PATH
        }
    )

print(f"Model version {model_info.registered_model_version} registered!")
```

## Common Patterns

### Pattern 1: Deploy to Provisioned Throughput
Create serving endpoint:

```python
import requests

API_ROOT = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
API_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

model_name = "catalog.schema.llama_3_1_8b_custom"
model_version = 1
endpoint_name = "llama-31-8b-custom"

payload = {
    "name": endpoint_name,
    "config": {
        "served_entities": [
            {
                "entity_name": model_name,
                "entity_version": str(model_version),
                "min_provisioned_throughput": 19000,
                "max_provisioned_throughput": 19000,
            }
        ]
    }
}

response = requests.post(
    f"{API_ROOT}/api/2.0/serving-endpoints",
    headers=headers,
    json=payload
)

print(json.dumps(response.json(), indent=2))
```

### Pattern 2: Required Dependencies
Install correct versions for MLR 16.4 LTS:

```python
# For ML Runtime (not SGC)
%pip install unsloth[cu124-torch260]==2025.9.6
%pip install threadpoolctl==3.1.0
%pip install accelerate==1.7.0
%pip install unsloth_zoo==2025.9.8
%restart_python

# For Serverless GPU Compute (SGC)
# Pin these in the environments tab instead
```

### Pattern 3: Validate Model Files
Check all required files exist:

```python
def validate_model_files(model_path):
    """Validate all required files for Provisioned Throughput"""
    
    required_files = [
        "config.json",
        "generation_config.json",
        "tokenizer_config.json",
        "tokenizer.json"
    ]
    
    # Check for model files (safetensors or pytorch_model.bin)
    model_files = [
        f for f in os.listdir(model_path)
        if f.endswith(".safetensors") or f == "pytorch_model.bin"
    ]
    
    missing_files = []
    for file in required_files:
        if not os.path.exists(os.path.join(model_path, file)):
            missing_files.append(file)
    
    if not model_files:
        missing_files.append("model weights (.safetensors or pytorch_model.bin)")
    
    if missing_files:
        raise ValueError(f"Missing required files: {missing_files}")
    
    # Validate config.json
    config_path = os.path.join(model_path, "config.json")
    with open(config_path, "r") as f:
        config = json.load(f)
    
    if "_name_or_path" not in config:
        raise ValueError("config.json missing '_name_or_path' field")
    
    if config["_name_or_path"] == "":
        raise ValueError("config.json '_name_or_path' is empty")
    
    print("All required files present and valid!")
    return True

# Usage
validate_model_files(UC_VOLUME_PATH)
```

## Reference Files

- [Unsloth Documentation](https://github.com/unslothai/unsloth) - Fast fine-tuning library
- [MLflow Transformers](https://mlflow.org/docs/latest/python_api/mlflow.transformers.html) - Model logging
- [Provisioned Throughput](https://docs.databricks.com/en/machine-learning/model-serving/provisioned-throughput.html) - Endpoint configuration

## Common Issues

| Issue | Solution |
|-------|----------|
| **Security check failure** | Fix `_name_or_path` in config.json to "unsloth/Meta-Llama-3.1-8B" |
| **Missing generation_config.json** | Add generation_config.json with token IDs and generation params |
| **Model scan failure** | Ensure all required files present, validate config.json |
| **Slow merge on MLR** | Use Serverless GPU Compute for faster merging (~1 min vs ~6 min) |
| **Deployment fails** | Check model name matches registered name, validate all files |

## Key Takeaways

1. **config.json Fix**: Must set `_name_or_path` to "unsloth/Meta-Llama-3.1-8B" to avoid security check failure
2. **generation_config.json**: Required file with token IDs (bos_token_id, eos_token_id, pad_token_id)
3. **Merge to 16-bit**: Merge LoRA adapters back to base weights before deployment
4. **Safe Serialization**: Use `safe_serialization=True` for safetensors format
5. **Serverless GPU Compute**: Faster for merging (~1 min) vs MLR (~6 min)
6. **Model Registration**: Register to Unity Catalog before deploying to Provisioned Throughput

## Complete Workflow

```python
def complete_fine_tune_deploy_workflow():
    """Complete workflow from fine-tuning to deployment"""
    
    # Step 1: Fine-tune (see Quick Start)
    # ... fine-tuning code ...
    
    # Step 2: Merge and fix config
    LOCAL_TEMP_PATH = "/tmp/llama_merged_model"
    
    model.save_pretrained_merged(
        LOCAL_TEMP_PATH,
        tokenizer=tokenizer,
        save_method="merged_16bit",
        safe_serialization=True
    )
    
    # Fix config.json
    config_path = os.path.join(LOCAL_TEMP_PATH, "config.json")
    with open(config_path, "r") as f:
        config = json.load(f)
    config["_name_or_path"] = "unsloth/Meta-Llama-3.1-8B"
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    
    # Add generation_config.json
    gen_config = {
        "bos_token_id": 128000,
        "eos_token_id": 128001,
        "pad_token_id": 128004,
        "do_sample": True,
        "temperature": 0.6,
        "max_length": 8192
    }
    with open(os.path.join(LOCAL_TEMP_PATH, "generation_config.json"), "w") as f:
        json.dump(gen_config, f, indent=2)
    
    # Step 3: Copy to UC Volume
    UC_VOLUME_PATH = "/Volumes/catalog/schema/volume_name/merged_weights"
    if os.path.exists(UC_VOLUME_PATH):
        subprocess.run(['rm', '-rf', UC_VOLUME_PATH], check=True)
    os.makedirs(UC_VOLUME_PATH, exist_ok=True)
    subprocess.run(["cp", "-r", f"{LOCAL_TEMP_PATH}/.", UC_VOLUME_PATH], check=True)
    
    # Step 4: Validate
    validate_model_files(UC_VOLUME_PATH)
    
    # Step 5: Register
    mlflow.set_registry_uri("databricks-uc")
    REGISTERED_NAME = "catalog.schema.llama_3_1_8b_custom"
    
    with mlflow.start_run() as run:
        model_info = mlflow.transformers.log_model(
            transformers_model=UC_VOLUME_PATH,
            artifact_path="model",
            task="llm/v1/chat",
            input_example={"messages": [{"role": "user", "content": "Hello"}]},
            registered_model_name=REGISTERED_NAME
        )
    
    print(f"Model registered: {REGISTERED_NAME} version {model_info.registered_model_version}")
    
    # Step 6: Deploy (see Pattern 1)
    # ... deployment code ...
    
    return model_info

# Usage
model_info = complete_fine_tune_deploy_workflow()
```

## When to Use This Skill

- Fine-tuning Llama models with Unsloth
- Deploying fine-tuned models to Provisioned Throughput
- Fixing config.json security check failures
- Merging LoRA adapters back to base weights
- Registering custom models to Unity Catalog
- Troubleshooting model deployment errors

## Related Skills

- unsloth-fine-tuning
- mlflow-model-registration
- provisioned-throughput-deployment
- lora-adapter-merging