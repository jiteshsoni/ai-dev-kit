---
name: "LLM-as-Judge Evaluation Patterns"
description: "Use LLMs to evaluate other LLM outputs with techniques to avoid bias, consistency issues, and hallucination in judge models."
author: "Veena"
url: "https://www.databricksters.com/p/braving-through-the-pitfalls-of-llm-judges-by-veen"
date: "2025"
tags: ["llm", "evaluation", "quality", "mlflow", "databricks"]
---

# LLM-as-Judge Evaluation

## Overview

LLM-as-judge uses one LLM to evaluate another's outputs. Pitfalls include position bias, verbosity bias, self-enhancement bias, and inconsistency. Mitigate with multi-judge ensembles, rubric-based scoring, and reference-guided evaluation.

**Use this skill when:** Evaluating LLM outputs at scale, automating quality checks, or building evaluation pipelines.

## Quick Start

```python
import mlflow

# Define judge prompt
judge_prompt = """Rate the helpfulness of this response on 1-5 scale.
Query: {query}
Response: {response}
Provide score and reasoning."""

# Use LLM as judge
def llm_judge(query, response):
    judge_response = llm.invoke(judge_prompt.format(query=query, response=response))
    return extract_score(judge_response)

# Evaluate with MLflow
with mlflow.start_run():
    results = mlflow.evaluate(
        model,
        data,
        model_type="text",
        evaluators=[llm_judge]
    )
```

## Common Patterns

### Pattern 1: Multi-Judge Ensemble

```python
def ensemble_judge(query, response):
    judges = [
        ChatDatabricks(endpoint="llama-70b"),
        ChatDatabricks(endpoint="mixtral-8x7b"),
        ChatDatabricks(endpoint="gpt-4")
    ]
    
    scores = [judge.evaluate(query, response) for judge in judges]
    return statistics.mean(scores)
```

### Pattern 2: Rubric-Based Scoring

```python
rubric = """
Score 5: Complete, accurate, well-structured
Score 4: Good but minor issues
Score 3: Adequate with some problems
Score 2: Significant issues
Score 1: Poor or incorrect
"""

def rubric_judge(query, response):
    prompt = f"{rubric}\n\nQuery: {query}\nResponse: {response}\nScore:"
    return llm.invoke(prompt)
```

## FAQ

**Q: How accurate are LLM judges?**  
A: 70-85% agreement with human judges when using proper techniques.

**Q: How to reduce bias?**  
A: Use multiple judges, rubrics, reference answers, and position-swapped evaluation.
