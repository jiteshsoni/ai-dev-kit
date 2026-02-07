---
name: "Multi-Layer Agent Architecture Patterns"
description: "Build production AI agents using layered architecture with specialized layers for tool calling, memory management, orchestration, and observability."
author: "Databricksters"
url: "https://www.databricksters.com/p/agents-are-like-onions-they-have-layers"
date: "2025"
tags: ["agents", "architecture", "llm", "agent-bricks", "genai", "databricks"]
---

# Multi-Layer Agent Architecture

## Overview

Production AI agents require layered architecture similar to onions - each layer handles specific responsibilities. Key layers: Foundation (LLM), Tool Calling (function execution), Memory (context/state), Orchestration (workflow logic), and Observability (logging/tracing). This separation enables maintainability, testability, and scalability.

**Use this skill when:** Designing production agent systems, refactoring monolithic agents, or scaling agent deployments.

## Quick Start

```python
from databricks_langchain import ChatDatabricks
from langgraph.prebuilt import create_react_agent

# Layer 1: Foundation LLM
llm = ChatDatabricks(endpoint="databricks-meta-llama-3-1-70b-instruct")

# Layer 2: Tools
from langchain.tools import tool

@tool
def get_customer_data(customer_id: str) -> dict:
    """Retrieve customer information from database"""
    return spark.sql(f"SELECT * FROM customers WHERE id = '{customer_id}'").first().asDict()

tools = [get_customer_data]

# Layer 3: Agent orchestration
agent = create_react_agent(llm, tools)

# Layer 4: Execution
result = agent.invoke({"messages": [{"role": "user", "content": "Get data for customer 123"}]})
```

## Common Patterns

### Pattern 1: Layer Separation

```python
# Foundation Layer: LLM selection
class FoundationLayer:
    def __init__(self, endpoint_name):
        self.llm = ChatDatabricks(endpoint=endpoint_name)
    
    def call(self, messages):
        return self.llm.invoke(messages)

# Tool Layer: Function registry
class ToolLayer:
    def __init__(self):
        self.tools = {}
    
    def register(self, tool_fn):
        self.tools[tool_fn.name] = tool_fn
    
    def execute(self, tool_name, **kwargs):
        return self.tools[tool_name](**kwargs)

# Memory Layer: Context management
class MemoryLayer:
    def __init__(self):
        self.history = []
    
    def add(self, message):
        self.history.append(message)
    
    def get_context(self, window=10):
        return self.history[-window:]

# Orchestration Layer: Workflow logic
class AgentOrchestrator:
    def __init__(self, foundation, tools, memory):
        self.foundation = foundation
        self.tools = tools
        self.memory = memory
    
    def execute(self, user_query):
        # Add query to memory
        self.memory.add({"role": "user", "content": user_query})
        
        # Get context
        context = self.memory.get_context()
        
        # Call LLM with tools
        response = self.foundation.call(context)
        
        # Execute tools if needed
        if response.tool_calls:
            for tool_call in response.tool_calls:
                result = self.tools.execute(tool_call.name, **tool_call.args)
                self.memory.add({"role": "tool", "content": result})
        
        return response
```

### Pattern 2: Observability Layer

```python
from mlflow import log_param, log_metric, start_run

class ObservabilityLayer:
    def __init__(self, agent):
        self.agent = agent
    
    def execute_with_tracking(self, query):
        with start_run() as run:
            # Log input
            log_param("query", query)
            
            # Execute
            start_time = time.time()
            result = self.agent.execute(query)
            duration = time.time() - start_time
            
            # Log metrics
            log_metric("latency_ms", duration * 1000)
            log_metric("num_tool_calls", len(result.tool_calls))
            
            # Log trace
            mlflow.log_dict(result.dict(), "trace.json")
            
            return result
```

## Reference Files

- [Agent Framework Documentation](https://docs.databricks.com/generative-ai/agent-framework/)
- [LangGraph for Databricks](https://docs.databricks.com/generative-ai/tutorials/agent-framework-langgraph.html)

## Common Issues

| Issue | Solution |
|-------|----------|
| **Monolithic agent hard to debug** | Separate into layers with clear interfaces. |
| **Tool calls unreliable** | Add validation layer between LLM and tool execution. |
| **Memory bloat** | Implement sliding window or summarization in memory layer. |
| **No observability** | Add logging/tracing layer wrapping all operations. |

## FAQ

**Q: Why layer architecture?**  
A: Separation of concerns, easier testing, independent scaling of components.

**Q: What's minimum viable layering?**  
A: Foundation (LLM) + Tools + Basic orchestration. Add memory/observability as needed.
