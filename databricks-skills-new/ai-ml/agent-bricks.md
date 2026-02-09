---
name: agent-bricks
description: "Build production-grade AI agents with Agent Bricks low-code platform."
author: "Canadian Data Guy"
source: "https://www.youtube.com/watch?v=Ixi7jvIziNw"
tags: ["ai-agents", "agent-bricks", "rag", "low-code"]
---

# Agent Bricks

## Overview

Agent Bricks is Databricks' low-code platform for building production-grade AI agents with built-in evaluation and monitoring.

## Quick Start

### Creating Your First Agent

```python
# In Databricks UI: Agent Bricks → Knowledge Assistant
# 1. Select vector index (UC-based)
# 2. Configure content columns
# 3. Deploy with one click
```

### Available Agent Types

| Agent Type | Use Case | Key Feature |
|------------|----------|-------------|
| **Knowledge Assistant** | RAG over documents | Automatic citations |
| **Information Extraction** | Batch entity extraction | Process at scale |
| **Multi-Agent Supervisor** | Orchestrate agents | Route to specialists |
| **Custom LLM Agent** | Content generation | Flexible prompting |

## Common Patterns

### Pattern 1: Internal Documentation Agent

```python
# Use case: Search internal docs
# 1. Sync content to UC volume
# 2. Create vector index with URL column
# 3. Agent provides cited answers

# Example question flow:
# Q: "What's our data retention policy?"
# A: "According to [Wiki](link), retention is 7 years..."
```

### Pattern 2: Iterative Improvement

```python
# Evaluation workflow:
# 1. Create gold-standard questions (~200)
# 2. Run MLflow evaluation with LLM judges
# 3. Review traces and identify failures
# 4. Adjust configuration
# 5. Re-evaluate to prove improvement

import mlflow
results = mlflow.evaluate(
    model=agent_uri,
    data=eval_dataset,
    judges=["relevance", "groundedness", "coherence"]
)
```

### Pattern 3: Human-in-the-Loop

```python
# Create review session:
# 1. Flag low-confidence responses
# 2. Route to labeling session
# 3. Expert feedback improves agent
```

## Production Checklist

- [ ] Evaluation dataset created
- [ ] LLM judges configured
- [ ] Human review process established
- [ ] Monitoring dashboard set up
- [ ] Alerting for quality degradation
- [ ] Rollback procedure defined
