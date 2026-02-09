---
name: mlflow-tracing
description: "Instrument AI applications with MLflow tracing for debugging and monitoring."
tags: ["mlflow", "tracing", "observability", "debugging"]
---

# MLflow Tracing

## Overview

Trace AI application execution for debugging, monitoring, and evaluation.

## Quick Start

```python
import mlflow

# Auto-instrument LangChain
mlflow.langchain.autolog()

# Or manual tracing
@mlflow.trace
def retrieve_documents(query):
    return vector_search.search(query)

@mlflow.trace
def generate_response(query, context):
    return llm.predict(f"Context: {context}\nQuestion: {query}")
```

## Common Patterns

### Pattern 1: Trace Spans

```python
# Nested tracing
@mlflow.trace(span_type="CHAIN")
def process_request(query):
    docs = retrieve_documents(query)  # Nested span
    response = generate_response(query, docs)  # Nested span
    return response
```

### Pattern 2: Custom Attributes

```python
@mlflow.trace
def predict(input):
    result = model.predict(input)
    mlflow.trace.set_attribute("confidence", result.confidence)
    mlflow.trace.set_attribute("latency_ms", result.latency)
    return result
```
