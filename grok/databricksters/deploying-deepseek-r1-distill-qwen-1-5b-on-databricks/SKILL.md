---
name: "deploy-deepseek-r1-distill-qwen"
description: "Deploy DeepSeek R1 Distill Qwen 1.5B on Databricks: custom GPU Model Serving, RoPE scaling configuration, transformers version requirements, and model testing before deployment."
---

# Deploying DeepSeek R1 Distill Qwen 1.5B on Databricks

## Overview

This skill covers deploying DeepSeek R1 Distill Qwen 1.5B model on Databricks using custom GPU Model Serving. Learn how to configure RoPE scaling for extended context, handle Qwen2.5 architecture differences from Llama, set up proper transformers and PyTorch versions, and test models using mlflow.models.predict() before deployment to avoid container build failures.

## Quick Start

### Install Required Dependencies
Set up correct package versions:

```python
# Install required packages
%pip install accelerate
%pip install transformers --upgrade  # Need RoPE support
%pip install torch --upgrade
%pip uninstall torch torchvision -y
%pip install torch torchvision --index-url https://download.pytorch.org/whl/cu124

dbutils.library.restartPython()

# Verify versions
import torch
import transformers
import accelerate

print(f"transformers: {transformers.__version__}")  # Need 4.48.1+
print(f"accelerate: {accelerate.__version__}")       # Need 0.31.0+
print(f"torch: {torch.__version__}")                 # Need 2.6.0+
```

### Load and Configure Model
Set up DeepSeek R1 Distill Qwen:

```python
import mlflow
import mlflow.transformers
from transformers import AutoModelForCausalLM, AutoTokenizer, AutoConfig, pipeline
from mlflow.models.signature import infer_signature
import pandas as pd

mlflow.set_tracking_uri("databricks")

# Model name
model_name = "deepseek-ai/DeepSeek-R1-Distill-Qwen-1.5B"

# Load tokenizer and config
tokenizer = AutoTokenizer.from_pretrained(model_name)
config = AutoConfig.from_pretrained(model_name)

# Configure RoPE scaling for extended context
if "rope_scaling" in config.to_dict():
    config.rope_scaling = {"type": "dynamic", "factor": 8.0}

# Load model (single GPU A10 for 1.5B, multi-GPU for larger)
model = AutoModelForCausalLM.from_pretrained(
    model_name,
    config=config,
    device_map="cuda:0"  # Single GPU
)

# Create pipeline
text_generator = pipeline("text-generation", model=model, tokenizer=tokenizer)
```

### Register Model to MLflow
Log and register model:

```python
# Prepare signature
example_prompt = "Explain quantum mechanics in simple terms."
example_inputs = pd.DataFrame({"inputs": [example_prompt]})
example_outputs = text_generator(example_prompt, max_length=200)
signature = infer_signature(example_inputs, example_outputs)

# Define conda environment
conda_env = {
    "name": "mlflow-env",
    "channels": ["defaults", "conda-forge"],
    "dependencies": [
        "python=3.11",
        "pip",
        {
            "pip": [
                "mlflow",
                "transformers==4.48.1",  # Critical: RoPE support
                "accelerate==0.31.0",
                "torch==2.6.0",
                "torchvision==0.21.0"
            ]
        }
    ]
}

# Log and register
with mlflow.start_run() as run:
    mlflow.transformers.log_model(
        transformers_model=text_generator,
        artifact_path="deepseek_model",
        signature=signature,
        input_example=example_inputs,
        registered_model_name="deepseek_qwen_1_5b",
        conda_env=conda_env
    )
    
    print(f"Model registered: deepseek_qwen_1_5b version {run.info.run_id}")
```

## Common Patterns

### Pattern 1: Test Before Deployment
Avoid container build failures:

```python
def test_model_before_deployment(model_uri: str):
    """Test model in simulated serving environment"""
    
    # Test 1: load_model() - Fast, uses notebook environment
    loaded_model = mlflow.pyfunc.load_model(model_uri)
    input_data = {"inputs": "Explain quantum mechanics."}
    output1 = loaded_model.predict(input_data)
    print("✓ load_model() test passed")
    
    # Test 2: mlflow.models.predict() - Slower, tests dependencies
    # This simulates serving endpoint environment!
    input_df = pd.DataFrame({"inputs": ["Explain quantum mechanics."]})
    output_path = "/tmp/deepseek_predictions.json"
    
    mlflow.models.predict(model_uri, input_df, output_path=output_path)
    
    import json
    with open(output_path, "r") as f:
        output2 = json.load(f)
    
    print("✓ mlflow.models.predict() test passed")
    print("✓ Model ready for deployment!")
    
    return output1, output2

# Usage
model_uri = "models:/deepseek_qwen_1_5b/1"
test_model_before_deployment(model_uri)
```

### Pattern 2: Multi-GPU Configuration
Deploy larger models:

```python
def load_large_model(model_name: str, num_gpus: int = 2):
    """Load model across multiple GPUs"""
    
    config = AutoConfig.from_pretrained(model_name)
    config.rope_scaling = {"type": "dynamic", "factor": 8.0}
    
    # Multi-GPU device map
    if num_gpus > 1:
        device_map = "auto"  # Let accelerate handle distribution
    else:
        device_map = "cuda:0"
    
    model = AutoModelForCausalLM.from_pretrained(
        model_name,
        config=config,
        device_map=device_map,
        torch_dtype=torch.bfloat16  # Use bfloat16 for memory efficiency
    )
    
    return model

# Usage for 14B or 32B models
# Requires multiple GPUs (e.g., 2x A10 or larger)
large_model = load_large_model("deepseek-ai/DeepSeek-R1-Distill-Qwen-14B", num_gpus=2)
```

### Pattern 3: RoPE Scaling Configuration
Configure for extended context:

```python
def configure_rope_scaling(model_name: str, factor: float = 8.0):
    """Configure RoPE scaling for extended context"""
    
    config = AutoConfig.from_pretrained(model_name)
    
    # Check if model supports RoPE scaling
    if "rope_scaling" in config.to_dict():
        config.rope_scaling = {
            "type": "dynamic",
            "factor": factor  # 8.0 = 8x context length
        }
        print(f"RoPE scaling configured: {factor}x")
    else:
        print("Model does not support RoPE scaling")
    
    return config

# Usage
config = configure_rope_scaling("deepseek-ai/DeepSeek-R1-Distill-Qwen-1.5B", factor=8.0)
```

## Reference Files

- [DeepSeek R1 Paper](https://arxiv.org/html/2501.12948v1) - Model architecture
- [Custom GPU Model Serving](https://docs.databricks.com/en/machine-learning/model-serving/custom-gpu-models.html) - GPU deployment
- [Transformers Documentation](https://huggingface.co/docs/transformers/) - Model loading

## Common Issues

| Issue | Solution |
|-------|----------|
| **Container build fails** | Test with mlflow.models.predict() first, check conda_env |
| **RoPE scaling not working** | Upgrade transformers to 4.48.1+, configure rope_scaling |
| **Out of memory** | Use bfloat16, multi-GPU, or smaller model |
| **Version conflicts** | Pin exact versions in conda_env |
| **Qwen vs Llama** | Use custom GPU serving for Qwen (not Provisioned Throughput) |

## Key Takeaways

1. **Custom GPU Serving**: Qwen models require custom GPU serving (not Provisioned Throughput)
2. **RoPE Scaling**: Configure for extended context (factor: 8.0)
3. **Transformers Version**: Need 4.48.1+ for RoPE support
4. **Testing**: Use mlflow.models.predict() to test before deployment
5. **Multi-GPU**: Larger models (14B, 32B) need multiple GPUs
6. **Architecture**: Qwen2.5 differs from Llama - requires custom serving

## Model Variants

```python
DEEPSEEK_MODELS = {
    "1.5B": {
        "name": "deepseek-ai/DeepSeek-R1-Distill-Qwen-1.5B",
        "gpus": 1,
        "instance": "g5.xlarge"  # Single A10
    },
    "7B": {
        "name": "deepseek-ai/DeepSeek-R1-Distill-Qwen-7B",
        "gpus": 1,
        "instance": "g5.2xlarge"  # Single A10
    },
    "14B": {
        "name": "deepseek-ai/DeepSeek-R1-Distill-Qwen-14B",
        "gpus": 2,
        "instance": "g5.4xlarge"  # Multiple GPUs
    },
    "32B": {
        "name": "deepseek-ai/DeepSeek-R1-Distill-Qwen-32B",
        "gpus": 4,
        "instance": "g5.8xlarge"  # Multiple GPUs
    }
}

def get_model_config(model_size: str):
    """Get configuration for model size"""
    return DEEPSEEK_MODELS.get(model_size)

# Usage
config = get_model_config("1.5B")
print(f"Model: {config['name']}")
print(f"GPUs needed: {config['gpus']}")
```

## Complete Deployment Workflow

```python
def complete_deepseek_deployment(model_size: str = "1.5B"):
    """Complete deployment workflow"""
    
    # Step 1: Install dependencies
    # ... pip install commands ...
    
    # Step 2: Get model config
    model_config = DEEPSEEK_MODELS[model_size]
    model_name = model_config["name"]
    
    # Step 3: Load and configure model
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    config = AutoConfig.from_pretrained(model_name)
    config.rope_scaling = {"type": "dynamic", "factor": 8.0}
    
    model = AutoModelForCausalLM.from_pretrained(
        model_name,
        config=config,
        device_map="cuda:0" if model_config["gpus"] == 1 else "auto"
    )
    
    # Step 4: Create pipeline
    text_generator = pipeline("text-generation", model=model, tokenizer=tokenizer)
    
    # Step 5: Register to MLflow
    with mlflow.start_run() as run:
        mlflow.transformers.log_model(
            transformers_model=text_generator,
            artifact_path="model",
            signature=signature,
            registered_model_name=f"deepseek_qwen_{model_size.lower()}",
            conda_env=conda_env
        )
    
    # Step 6: Test before deployment
    model_uri = f"models:/deepseek_qwen_{model_size.lower()}/1"
    test_model_before_deployment(model_uri)
    
    # Step 7: Deploy to Custom GPU Model Serving
    # Via UI or API
    
    print("Deployment complete!")

# Usage
complete_deepseek_deployment("1.5B")
```

## When to Use This Skill

- Deploying DeepSeek R1 Distill models
- Using Qwen2.5-based models
- Needing extended context (RoPE scaling)
- Custom GPU model serving
- Testing models before deployment

## Related Skills

- custom-gpu-model-serving
- transformers-model-deployment
- mlflow-model-registration
- rope-scaling-configuration