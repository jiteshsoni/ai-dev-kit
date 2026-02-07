---
name: "LLM Token Cost Reduction Strategies"
description: "Reduce LLM API costs through prompt optimization, caching, smaller models, and output length limits."
author: "Databricksters"
url: "https://www.databricksters.com/p/getting-medieval-on-token-costs"
date: "2025"
tags: ["cost-optimization", "tokens", "llm", "prompts", "databricks"]
---

# LLM Token Cost Reduction

## Overview

LLM costs = (input_tokens + output_tokens) × price_per_token. Reduce costs: optimize prompts, limit output length, cache responses, use smaller models when possible, and batch requests.

## Quick Start

```python
# Cost reduction techniques
from databricks_langchain import ChatDatabricks

# 1. Limit output tokens
llm = ChatDatabricks(
    endpoint="databricks-meta-llama-3-1-70b-instruct",
    max_tokens=100  # Prevent verbose responses
)

# 2. Optimize prompt (fewer input tokens)
# ❌ Verbose: "I would like you to please analyze this text..."
# ✅ Concise: "Analyze: {text}"

# 3. Cache common responses
from functools import lru_cache

@lru_cache(maxsize=1000)
def cached_llm(prompt):
    return llm.invoke(prompt).content
```

## Common Patterns

### Pattern 1: Prompt Compression

```python
# Before: 500 tokens
verbose_prompt = """
I need you to carefully analyze the following customer feedback 
and provide a detailed sentiment analysis. Please consider...
"""

# After: 50 tokens
concise_prompt = "Classify sentiment (positive/negative/neutral): {feedback}"

# Savings: 90% reduction in input tokens
```

### Pattern 2: Use Smaller Models

```python
# Expensive: 70B model for simple tasks
expensive = ChatDatabricks(endpoint="llama-70b")

# Cheaper: 8B model for classification
cheap = ChatDatabricks(endpoint="llama-8b")

# Cost difference: 10x cheaper for same task
```

## FAQ

**Q: How much can I save?**  
A: 50-80% through prompt optimization, caching, and right-sizing models.
