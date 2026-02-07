---
name: deploying-deepseek-r1-distill-qwen-15b-on-databric
description: 
---

# Deploying DeepSeek R1 Distill Qwen 1.5B on Databricks

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/deploying-deepseek-r1-distill-qwen

## Tags
mlops, ai, data-engineering, performance, databricks

## Full Content

# Deploying DeepSeek R1 Distill Qwen 1.5B on Databricks

**Source:** https://www.databricksters.com/p/deploying-deepseek-r1-distill-qwen

---



Deploying DeepSeek R1 Distill Qwen 1.5B on Databricks

Databricksters

Subscribe

Sign in

AI &amp; ML

Deploying DeepSeek R1 Distill Qwen 1.5B on Databricks

Austin

Feb 04, 2025

6

Share

“The results are promising: DeepSeek-R1-Distill-Qwen-1.5B outperforms GPT-4o and Claude-3.5-Sonnet on math benchmarks with 28.9% on AIME and 83.9% on MATH. Other dense models also achieve impressive results, significantly outperforming other instruction-tuned models based on the same underlying checkpoints.” — DeepSeek-AI

Not Potato Bolshevik’s Tweet

New LLMs usually make waves in the AI spheres, but DeepSeek R1 was more of a tsunami this week as the US stock market lost about a trillion dollars in valuation inside of 30 minutes. Regardless of your opinions on DeepSeek and its implications for the global (or national) economy, it’s an open source model whose performance is very competitive with the largest proprietary models currently available. That performance is due both to architecture it shares in common with large proprietary reasoning models, as well as some very interesting departures from the current training paradigm of ChatGPT, Claude, and Gemini. (

At least until very recently

).

For starters, 

DeepSeek-R1

 comes in a few flavors, and it's worth understanding the significance of each:

Thanks for reading The Databricksters! Subscribe for free to receive new posts and support my work.

Subscribe

DeepSeek-R1-Zero

Applies RL directly to the base model without any Supervised Fine Tuning (SFT) data at all

DeepSeek-R1

Applies RL starting from a checkpoint fine-tuned with minimal high quality Chain of Thought (CoT) examples

DeepSeek-R1-Distill-X

A half dozen smaller, dense models distilled from DeepSeek-R1 that preserve the reasoning patterns learned from its much larger parent model

The 

DeepSeek Paper

 explains how using 800k high quality fine-tuning samples curated from DeepSeek-R1 is enough data to significantly improve the reasoning abilities of smaller models such as Qwen2.5, to the point where even just the 1.5B variant can outperform a model with orders of magnitude more parameters on math tasks. Notably, this distillation process is purely SFT based, and includes no RL at all.

So how can we host one of these overpowered mini models ourselves? Databricks recently published 

a blog post

 showing how to deploy DeepSeek R1 Distill Llama 8B and 70B using Provisioned Throughput directly on the Databricks platform, which makes sense given that Llama 3.x architecture is natively supported, even for fine tuned variants. However, what if we want to serve one of the four Qwen2.5 based distilled models? The architectural differences between Llama 3 and Qwen2.5 are so small you can actually 

convert one to the other

, but if you don’t want to convert all the weights to Llama, you’re going to need to use custom GPU Model Serving for Qwen.

And that brings us to the code portion of this blog:

%pip install accelerate
%pip install transformers --upgrade ## need RoPE for this to work, and that's only included in newer versions
%pip install torch --upgrade ## torch version also matters here
%pip uninstall torch torchvision -y
%pip install torch torchvision --index-url https://download.pytorch.org/whl/cu124

dbutils.library.restartPython()

import torch
import torchvision
import accelerate
import transformers

## Feel free to compare to the conda_env specified below to double check
print(transformers.__version__)
print(accelerate.__version__)
print(torch.__version__)
print(torchvision.__version__)

import pandas as pd
import mlflow
import mlflow.transformers
from mlflow.models.signature import infer_signature
from transformers import AutoModelForCausalLM, AutoTokenizer, AutoConfig, pipeline

# Enable MLflow Autologging
mlflow.set_tracking_uri(&quot;databricks&quot;)

# Specify the model from HuggingFace transformers
model_name = &quot;deepseek-ai/DeepSeek-R1-Distill-Qwen-1.5B&quot;

# Load tokenizer and model
tokenizer = AutoTokenizer.from_pretrained(model_name)
config = AutoConfig.from_pretrained(model_name)

# Adjust rope_scaling
if &quot;rope_scaling&quot; in config.to_dict():
 config.rope_scaling = {&quot;type&quot;: &quot;dynamic&quot;, &quot;factor&quot;: 8.0}

# Loading this on single node, single GPU A10 cluster, for 14B or 32B models you will need multiple GPUs of this size
model = AutoModelForCausalLM.from_pretrained(
 model_name, 
 config=config, 
 device_map=&quot;cuda:0&quot;
)

text_generator = pipeline(&quot;text-generation&quot;, model=model, tokenizer=tokenizer)

# We need a signature for UC registered models
example_prompt = &quot;Explain quantum mechanics in simple terms.&quot;
example_inputs = pd.DataFrame({&quot;inputs&quot;: [example_prompt]})
example_outputs = text_generator(example_prompt, max_length=200)
signature = infer_signature(example_inputs, example_outputs)

# Define the Conda environment with correct package versions
conda_env = {
 &quot;name&quot;: &quot;mlflow-env&quot;,
 &quot;channels&quot;: [&quot;defaults&quot;, &quot;conda-forge&quot;],
 &quot;dependencies&quot;: [
 &quot;python=3.11&quot;,
 &quot;pip&quot;,
 {
 &quot;pip&quot;: [
 &quot;mlflow&quot;,
 &quot;transformers==4.48.1&quot;,
 &quot;accelerate==0.31.0&quot;,
 &quot;torch==2.6.0&quot;,
 &quot;torchvision==0.21.0&quot;
 ]
 }
 ]
}

# Log model with MLflow
with mlflow.start_run() as run:
 mlflow.transformers.log_model(
 transformers_model=text_generator,
 artifact_path=&quot;deepseek_model&quot;,
 signature=signature,
 input_example=example_inputs,
 registered_model_name=&quot;deepseek_qwen_1_5b&quot;,
 conda_env=conda_env
 )

The above is all you need log and register the model to MLflow! From here I like to perform two tests before I kick off a serving endpoint, because I want to be reasonably certain I won’t get a container build failure before I wait 30 minutes for a GPU model serving endpoint to fully spin up:

I test the model using 

load_model()

This catches immediate errors with my class’s logic and is quite fast, but doesn’t test the dependencies for compatibility issues because it’s loading it in the same notebook environment we kicked if off from

I test the model again using 

mlflow.models.predict()

This catches the dependency issues in my 

conda_env

 because it spins up a lightweight virtual env that mimics the serving endpoint

If both pass, then I go to the experiment in MLflow and deploy the model to a live endpoint.

# Load the model locally for test 1
model_uri = &quot;models:/deepseek_qwen_1_5b/4&quot;
loaded_model = mlflow.pyfunc.load_model(model_uri)

input_data = {&quot;inputs&quot;: &quot;Explain quantum mechanics in simple terms.&quot;}
output = loaded_model.predict(input_data)
print(output)

## Call model in virtual env as specified by conda_env for test 2
# Define the model URI
model_uri = &quot;models:/deepseek_qwen_1_5b/4&quot;

# Define input data in the required format
input_data = pd.DataFrame({&quot;inputs&quot;: [&quot;Explain quantum mechanics in simple terms.&quot;]})

# Call the MLflow model predict API since that's better than load_model()
output = mlflow.models.predict(model_uri, input_data)
print(output)

Now we grab some tea and wait for DeepSeek-R1-Distill-Qwen-1.5B to deploy!

About Me:

I come from a DS/ML background, which I did for about 6 years before starting at Databricks as a Specialist Solutions Architect in GenAI and MLOps. I like to write about things I find interesting and that I think other people might benefit from. 

Thanks for reading The Databricksters! Subscribe for free to receive new posts and support my work.

Subscribe

6

Share

Next

Discussion about this post

Comments

Restacks

Top

Latest

Discussions

No posts

Ready for more?

Subscribe

© 2026 Soni

 · 

Privacy

 ∙ 

Terms

 ∙ 

Collection notice

 Start your Substack

Get the app



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
