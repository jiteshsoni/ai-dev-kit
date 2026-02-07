---
name: "Building AI Agents with Agent Bricks"
description: "Use Agent Bricks framework for modular AI agent development with pre-built components for tool integration, memory, and RAG."
author: "Databricksters"
url: "https://www.databricksters.com/p/building-useful-ai-agents-with-agent-bricks"
date: "2025"
tags: ["agent-bricks", "agents", "llm", "rag", "databricks"]
---

# Building AI Agents with Agent Bricks

## Overview

Agent Bricks provides pre-built components (bricks) for rapid agent development: knowledge retrieval, tool calling, memory management, and guardrails. Compose bricks to build production agents without reinventing infrastructure.

**Use this skill when:** Building agents quickly, standardizing agent patterns, or leveraging pre-built agent components.

## Quick Start

```python
from databricks_langchain import ChatDatabricks
from agent_bricks import KnowledgeBrick, ToolBrick, AgentBuilder

# Foundation
llm = ChatDatabricks(endpoint="databricks-meta-llama-3-1-70b-instruct")

# Add knowledge brick (RAG)
knowledge = KnowledgeBrick(
    vector_search_endpoint="my_endpoint",
    index="main.default.docs_index"
)

# Add tool brick
@tool
def get_order_status(order_id: str) -> str:
    return spark.sql(f"SELECT status FROM orders WHERE id = '{order_id}'").first()[0]

tools = ToolBrick(tools=[get_order_status])

# Build agent
agent = AgentBuilder(llm).add_brick(knowledge).add_brick(tools).build()

# Use agent
result = agent.invoke("What's the status of order 12345?")
```

## Common Patterns

### Pattern 1: RAG with Vector Search

```python
knowledge_brick = KnowledgeBrick(
    vector_search_endpoint="vs_endpoint",
    index="catalog.schema.docs",
    columns=["id", "content", "metadata"],
    num_results=5
)

agent = AgentBuilder(llm).add_brick(knowledge_brick).build()
```

### Pattern 2: Multi-Tool Agent

```python
from agent_bricks import ToolBrick

@tool
def query_database(sql: str) -> list:
    return spark.sql(sql).collect()

@tool
def send_email(to: str, subject: str, body: str) -> bool:
    # Email logic
    return True

tools = ToolBrick(tools=[query_database, send_email])
agent = AgentBuilder(llm).add_brick(tools).build()
```

## FAQ

**Q: What are bricks?**  
A: Pre-built, composable components for common agent patterns (RAG, tools, memory, guardrails).

**Q: Can I create custom bricks?**  
A: Yes, extend base Brick class to create reusable components.
