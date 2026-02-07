---
name: "Low-Code Production Agents"
description: "Build production-ready agents using Databricks Agent Framework UI with minimal code for common patterns like RAG and tool calling."
author: "Databricksters"
url: "https://www.databricksters.com/p/your-low-code-shortcut-to-production-grade-agent-o"
date: "2025"
tags: ["low-code", "agents", "agent-framework", "rag", "databricks"]
---

# Low-Code Production Agents

## Overview

Databricks Agent Framework UI enables building production agents through point-and-click: connect Vector Search indexes, add tools, configure LLM, and deploy to Model Serving. Generates Python code automatically for customization.

## Quick Start

```python
# UI workflow creates this automatically:
from databricks import agents

# Agent with Vector Search RAG
agent = agents.create_agent(
    name="customer_support_agent",
    llm_endpoint="databricks-meta-llama-3-1-70b-instruct",
    vector_search_endpoint="vs_endpoint",
    vector_search_index="catalog.schema.docs_index",
    instructions="You are a helpful customer support agent."
)

# Deploy to serving
agents.deploy(agent, endpoint_name="support-agent")
```

## Common Patterns

### Pattern 1: RAG Agent via UI

```
1. Navigate to Machine Learning → Agents
2. Click "Create Agent"
3. Select "RAG Agent" template
4. Configure:
   - LLM: databricks-meta-llama-3-1-70b-instruct
   - Vector Search Index: main.default.docs_index
   - System prompt: "You are..."
5. Test in playground
6. Click "Deploy"
```

### Pattern 2: Export and Customize

```python
# UI generates code → export → customize
agent_code = agents.export_agent("my_agent")

# Add custom tools
@tool
def get_weather(location: str) -> str:
    return f"Weather in {location}: Sunny"

agent_code.add_tool(get_weather)
agents.redeploy(agent_code)
```

## FAQ

**Q: When to use UI vs code?**  
A: UI for standard patterns, code for complex custom logic.
