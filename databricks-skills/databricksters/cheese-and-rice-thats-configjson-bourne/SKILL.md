---
name: cheese-and-rice-thats-configjson-bourne
description: 
---

# Cheese and Rice, that's config.json Bourne

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/jesus-christ-thats-configjson-bourne

## Tags
performance, databricks, joins, ai

## Full Content

# Cheese and Rice, that's config.json Bourne

**Source:** https://www.databricksters.com/p/jesus-christ-thats-configjson-bourne

**Blog:** Databricks

---



Cheese and Rice, that&#x27;s config.json Bourne - by Austin

Databricksters

Subscribe

Sign in

Cheese and Rice, that's config.json Bourne

Deploying Fine Tuned Models to Provisioned Throughput Endpoints

Austin

Dec 09, 2025

3

Share

One of my customers recently tried fine tuning a Llama 3.1 8B model using Unsloth on Databricks Serverless GPU Compute (SGC), which worked great. Then they tried deploying that model to a Provisioned Throughput endpoint, which didn’t. It took me much longer to diagnose this issue than I care to admit, so instead of talking about the journey, we’re going to skip to the destination this time. If you’re trying to do this with a larger mode, say Llama 3.3 70B, then stay tuned for our next installment of this blog I’m coauthoring with 

Joshua Eason

.

Subscribe

What’s better, this or John Wick?

The code I provide will work with SGC or on demand ML Runtime, but it’s significantly faster in SGC even if you only use a single A10 in both scenarios.

If you use MLR, then start with these pip installs to have all the libraries play nicely with MLR 16.4 LTS, if you’re using SGC you should pin these dependencies in the environments tab:

%pip install unsloth[cu124-torch260]==2025.9.6
%pip install threadpoolctl==3.1.0
%pip install accelerate==1.7.0
%pip install unsloth_zoo==2025.9.8
%restart_python

Next we need to get our base model from 

unsloth

. This works for other tuning frameworks like HuggingFace 

trl

 as well, but we’re using Unsloth for this demo because it’s nice and quick, and also my customer was using it. Using 4-bit quantization doesn’t necessarily make sense for your production use case, since I’m going to merge it back into 16-bit later anyway, but if you want to save time and RAM during a demo then have at it.

from unsloth import FastLanguageModel
import torch

# Some changes needed for larger models
model, tokenizer = FastLanguageModel.from_pretrained(
 model_name = “unsloth/llama-3.1-8b”,
 max_seq_length = 2048,
 dtype = torch.bfloat16,
 load_in_4bit = True, # fastest + lowest memory
)

And some sample data:

# It’s a toy set, replace this with your real data
from datasets import Dataset

data = [
 {”text”: “### Instruction: Say hello politely.\n### Response: Hello! How may I ruin your day?”},
 {”text”: “### Instruction: Explain PEFT.\n### Response: A lightweight way to fine-tune large models on the cheap.”},
 {”text”: “### Instruction: Explain LoRA.\n### Response: LoRA adds small trainable matrices instead of updating full weights.”},
]

dataset = Dataset.from_list(data)

Great, now we can define our tokenizer and model as well as tokenize the dataset above:

# Define our tokenizer and tokenize dataset
def tokenize(example):
 encoding = tokenizer(
 example[”text”],
 truncation=True,
 max_length=1024,
 padding=”max_length”,
 )
 encoding[”labels”] = encoding[”input_ids”].copy()
 return encoding

tokenized_dataset = dataset.map(tokenize)

# Define our model
model = FastLanguageModel.get_peft_model(
 model,
 r = 8,
 lora_alpha = 16,
 lora_dropout = 0.0,
 target_modules = [”q_proj”, “k_proj”, “v_proj”, “o_proj”],
)

Similarly for our training args, the trainer, and then we train the model:

from transformers import TrainingArguments, Trainer

# Define our training arguments, trainer, and train toy model
training_args = TrainingArguments(
 output_dir = “outputs”,
 per_device_train_batch_size = 1,
 gradient_accumulation_steps = 1,
 warmup_steps = 0,
 max_steps = 10,
 learning_rate = 5e-5,
 logging_steps = 5,
 optim = “adamw_torch”,
 bf16 = True,
 remove_unused_columns = False,
)

trainer = Trainer(
 model = model,
 args = training_args,
 train_dataset = tokenized_dataset,
)

trainer.train()

Great, so now still only in our VM we’re going to merge this adapter later back into our base weights as promised, though in a real use case you would probably have used 16-bit all along. Here is where you’re really going to see a huge difference between SGC (which does this in ~1 minute) and MLR (which takes ~6):

import shutil
import os

# If you run multiple within short succession, just rename this
LOCAL_TEMP_PATH = “/tmp/llama_merged_model_6”

# See, I told you we would merge back into 16bit weights
model.save_pretrained_merged(
 LOCAL_TEMP_PATH,
 tokenizer=tokenizer,
 save_method=”merged_16bit”,
 safe_serialization=True # Force safetensors
)

print(”Merged model saved at:”, LOCAL_TEMP_PATH)

Here’s our first stumbling block we’re going to daintily step over. If you don’t do this manually, then your 

_name_or_path

 param in your config.json Bourne file will either be set to your base model or won’t be defined at all. In either case, it’s enough for the Provisioned Throughput (PT) endpoint to reject the entire thing:

import json
import os

config_path = os.path.join(LOCAL_TEMP_PATH, “config.json”)

with open(config_path, “r”) as f:
 config = json.load(f)

# Rename the model name to avoid triggering the security check fail, you MUST do this
config[”_name_or_path”] = “unsloth/Meta-Llama-3.1-8B”

# Save the sanitized config back
with open(config_path, “w”) as f:
 json.dump(config, f, indent=2)

print(f”Sanitized config saved to {config_path}”)

Now we’re finally going to save what we have out to a UC Volume, so pick a path you have read and write permissions on:

import subprocess
import mlflow

# Define your paths
UC_VOLUME_PATH = “/Volumes/&lt;catalog_name>/&lt;schema_name>/&lt;volume_name>/merged_weights_mlr_bf16”

# Define the final model name; this must match what the PT Endpoint expects
CATALOG = “&lt;catalog_name>”
SCHEMA = “&lt;schema_name>”
REGISTERED_NAME = f”{CATALOG}.{SCHEMA}.llama_3_1_8b_custom”

# Copy artifacts to volume
print(f”Copying from {LOCAL_TEMP_PATH} to {UC_VOLUME_PATH}...”)

if os.path.exists(UC_VOLUME_PATH):
 subprocess.run([’rm’, ‘-rf’, UC_VOLUME_PATH], check=True)

os.makedirs(UC_VOLUME_PATH, exist_ok=True)

# Copy files
subprocess.run([”cp”, “-r”, f”{LOCAL_TEMP_PATH}/.”, UC_VOLUME_PATH], check=True)
print(”Model copied to UC Volume.”)

Cool, here’s another gotcha: you also need a 

generation_config.json

 file to avoid failing the model scan. Here’s one that works; you can change as needed:

# Add generation_config.json to the UC Volume BEFORE logging to MLflow
gen_config = {
 “bos_token_id”: 128000,
 “eos_token_id”: 128001,
 “pad_token_id”: 128004,
 “do_sample”: True,
 “temperature”: 0.6,
 “max_length”: 8192
}

with open(os.path.join(UC_VOLUME_PATH, “generation_config.json”), “w”) as f:
 json.dump(gen_config, f, indent=2)

print(”generation_config.json Bourne added to volume.”)

Alright I can see I’ve overdone the Jason Bourne meme. Lucky for you we’re about done here. We only need to log and register the model to MLflow and then we can serve the fine tuned Llama 3.1 8B model using optimized Provisioned Throughput endpoints for greatly increased token throughput:

# Log and register to MLflow directly from UC Volume path
mlflow.set_registry_uri(”databricks-uc”)

# Define input example for signature (sorry, one more for the road!)
input_example = {
 “messages”: [
 {”role”: “user”, “content”: “That’s Jason Bourne.”}
 ]
}

print(f”Registering model: {REGISTERED_NAME}”)

with mlflow.start_run(run_name=”register_llama_3_1”) as run:
 model_info = mlflow.transformers.log_model(
 transformers_model=UC_VOLUME_PATH,
 artifact_path=”model”,
 task=”llm/v1/chat”,
 input_example=input_example,
 registered_model_name=REGISTERED_NAME,
 metadata={
 “source”: “uc_volume”,
 “original_path”: UC_VOLUME_PATH
 }
 )

print(f”Model version {model_info.registered_model_version} registered.”)

From here you can deploy via the UI or if you want to finish this whole thing in the API you can simply run this with your desired throughput bands. I’m setting mine to the smallest since it’s a demo:

import requests

API_ROOT = dbutils.notebook.entry_point.getDbuti



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
