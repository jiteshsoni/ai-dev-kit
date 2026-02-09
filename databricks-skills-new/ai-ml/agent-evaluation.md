---
name: agent-evaluation
description: "Evaluate AI agents using MLflow LLM judges and custom scorers."
tags: ["agent-evaluation", "mlflow", "llm-judges", "metrics"]
---

# Agent Evaluation

## Overview

Evaluate AI agent quality using MLflow's built-in judges and custom scorers.

## Quick Start

```python
import mlflow

# Evaluate agent
results = mlflow.evaluate(
    model=agent_uri,
    data=eval_dataset,
    judges=["relevance", "groundedness", "coherence"]
)
```

## Built-in Judges

| Judge | Measures |
|-------|----------|
| **Relevance** | Answer matches question |
| **Groundedness** | Answer supported by context |
| **Coherence** | Well-structured response |
| **Safety** | Harmful content |

## Common Patterns

### Pattern 1: Custom Scorer

```python
from mlflow.metrics import make_metric

@make_metric()
def brevity_score(predictions, targets):
    """Penalize long responses"""
    scores = []
    for pred in predictions:
        word_count = len(pred.split())
        if word_count > 100:
            scores.append(0.5)
        else:
            scores.append(1.0)
    return scores

# Use in evaluation
results = mlflow.evaluate(
    model=agent_uri,
    data=eval_dataset,
    extra_metrics=[brevity_score]
)
```

### Pattern 2: Trace Analysis

```python
# Analyze tool usage
for trace in traces:
    tool_calls = trace.search_spans(span_type="TOOL")
    for call in tool_calls:
        print(f"Tool: {call.name}, Success: {call.status}")
```
