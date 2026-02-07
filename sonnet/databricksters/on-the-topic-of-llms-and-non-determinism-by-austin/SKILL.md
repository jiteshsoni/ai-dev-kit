---
name: "Managing LLM Non-Determinism"
description: "Handle LLM output variability through temperature control, seed setting, caching, and structured output enforcement."
author: "Austin"
url: "https://www.databricksters.com/p/on-the-topic-of-llms-and-non-determinism-by-austin"
date: "2025"
tags: ["llm", "determinism", "temperature", "reliability", "databricks"]
---

# Managing LLM Non-Determinism

## Overview

LLMs are inherently probabilistic - same input can produce different outputs. Reduce variability through temperature=0, seed setting, structured outputs (JSON schema), and response caching. Complete determinism impossible, but consistency achievable for production use.

**Use this skill when:** Building production LLM applications requiring consistent outputs or debugging LLM behavior.

## Quick Start

```python
from databricks_langchain import ChatDatabricks

# Maximum determinism settings
llm = ChatDatabricks(
    endpoint="databricks-meta-llama-3-1-70b-instruct",
    temperature=0.0,  # Greedy decoding
    seed=42,  # Reproducible randomness
    max_tokens=100
)

# Test consistency
results = [llm.invoke("What is 2+2?") for _ in range(5)]
assert len(set(results)) == 1  # All identical
```

## Common Patterns

### Pattern 1: Structured Output Enforcement

```python
from pydantic import BaseModel

class ExtractionResult(BaseModel):
    customer_name: str
    order_id: str
    amount: float

# Force JSON schema
llm_with_structure = llm.with_structured_output(ExtractionResult)
result = llm_with_structure.invoke("Extract: John ordered #12345 for $99.99")
# result is ExtractionResult object, not free text
```

### Pattern 2: Response Caching

```python
from functools import lru_cache

@lru_cache(maxsize=10000)
def cached_llm_call(prompt: str) -> str:
    return llm.invoke(prompt).content

# Identical prompts return cached results
response1 = cached_llm_call("What is Python?")
response2 = cached_llm_call("What is Python?")  # Cached, instant
```

## FAQ

**Q: Does temperature=0 guarantee identical outputs?**  
A: No, but significantly reduces variability. Implementation details and hardware can still cause differences.

**Q: When to use higher temperature?**  
A: Creative tasks (writing, brainstorming). Use low temp (0-0.3) for factual, consistent tasks.
