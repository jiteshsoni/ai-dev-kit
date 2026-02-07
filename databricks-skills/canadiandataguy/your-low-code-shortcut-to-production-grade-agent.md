---
name: your-low-code-shortcut-to-production-grade-agent
description: Build production-ready AI agents using Agent Bricks on Databricks with built-in evaluation, monitoring, and iterative improvement. Use when building RAG applications, creating knowledge assistants, implementing internal documentation search, or deploying production AI agents with evaluation frameworks.
---

# Your Low-Code Shortcut to Production-Grade Agent on Databricks

## Overview

Agent Bricks is Databricks' low-code platform for building production-grade AI agents. It enables rapid agent development with built-in evaluation, monitoring, and iterative improvement capabilities.

**Key Value Proposition**: Go from idea to production agent in minutes, with full observability and evaluation frameworks built-in.

## Quick Start

### Creating Your First Agent

```python
# In Databricks UI: Agent Bricks → Knowledge Assistant
# 1. Select your vector index (UC-based)
# 2. Configure content columns (text, URL, metadata)
# 3. Deploy with one click
```

### Available Agent Types

| Agent Type | Use Case | Key Feature |
|------------|----------|-------------|
| **Knowledge Assistant** | RAG over documents | Automatic citations |
| **Information Extraction** | Batch entity extraction | Process documents at scale |
| **Multi-Agent Supervisor** | Orchestrate multiple agents | Route to specialized agents |
| **Custom LLM Agent** | Content generation | Flexible prompt engineering |

### Configuration Example

```python
# Knowledge Assistant setup
vector_index = "catalog.schema.embedded_content"
content_column = "text"
url_column = "url"  # Enables clickable citations

# The agent automatically:
# - Embeds queries using same model as index
# - Retrieves semantically similar documents
# - Generates responses with source citations
```

## Common Patterns

### Pattern 1: Internal Documentation Agent

```python
# Use case: Search internal Confluence, SharePoint, Notion
# 1. Sync content to UC volume
# 2. Create vector index with URL column
# 3. Agent provides cited answers from internal docs

# Example question flow:
# Q: "What's our data retention policy?"
# A: "According to [Internal Wiki - Data Policies](link), 
#      retention is 7 years for financial data..."
```

### Pattern 2: Iterative Agent Improvement

```python
# Evaluation workflow:
# 1. Create gold-standard question set (~200 questions)
# 2. Run MLflow evaluation with LLM judges
# 3. Review traces and identify failure patterns
# 4. Adjust agent configuration (prompt, index, model)
# 5. Re-evaluate to prove improvement (not regression)

import mlflow

# Evaluate agent responses
results = mlflow.evaluate(
    model=agent_uri,
    data=eval_dataset,
    judges=["relevance", "groundedness", "coherence"]
)
```

### Pattern 3: Human-in-the-Loop Review

```python
# Create review session for domain experts
# 1. Flag low-confidence responses
# 2. Route to labeling session
# 3. Expert feedback improves both agent and judges

# In Databricks UI:
# - Go to MLflow experiment
# - Select traces for review
# - Create labeling session
# - Share with team members
```

## Reference Files

### Built-in LLM Judges

Databricks provides pre-built judges for:
- **Relevance**: Is the answer relevant to the question?
- **Groundedness**: Is the answer supported by retrieved context?
- **Coherence**: Is the answer well-structured and clear?
- **Brevity**: Custom metric for concise responses

### Trace Logging

Every interaction is logged to MLflow:
```json
{
  "request": "user question",
  "response": "agent answer",
  "retrieved_documents": [...],
  "execution_time_ms": 450,
  "token_count": 1250,
  "model": "gpt-4"
}
```

## Common Issues

| Issue | Solution |
|-------|----------|
| **Poor retrieval quality** | Tune vector index; check embedding model alignment |
| **Hallucinations** | Enable citations; use groundedness judge |
| **Long responses** | Add custom brevity scorer |
| **Can't compare models** | Use Playground for A/B testing GPT-4, Claude, Llama |
| **Evaluating improvement** | Create consistent eval set; track metrics across versions |

## Advanced Tips

### Custom Scorers

```python
# Example: Customer support brevity scorer
@mlflow.trace
def brevity_score(response):
    """Penalize overly long responses"""
    word_count = len(response.split())
    if word_count > 100:
        return 0.5  # Too long
    return 1.0  # Good length
```

### Model Comparison Strategy

1. Build baseline agent with Agent Bricks
2. Test same questions against GPT-4, Claude, Llama in Playground
3. Use LLM judges for quantitative comparison
4. Human review for qualitative assessment

### Production Checklist

- [ ] Evaluation dataset created
- [ ] LLM judges configured and validated
- [ ] Human review process established
- [ ] Monitoring dashboard set up
- [ ] Alerting for quality degradation
- [ ] Rollback procedure defined

## FAQ

**Q: Who should use Agent Bricks?**
A: Anyone familiar with Databricks can build their first agent. Ideal for rapid prototyping and production deployment.

**Q: What's the difference between Agent Bricks and custom RAG?**
A: Agent Bricks applies optimizations learned from 1.5+ years of customer deployments. You can achieve same results custom, but it takes longer.

**Q: Can I use my own LLM?**
A: Yes. Databricks provides GPT-4, Claude, Llama, and OSS models. You can also bring your own.

**Q: How do I prove my agent is improving?**
A: Use consistent evaluation sets and track metrics across versions. LLM judges + human review = aligned improvement metrics.