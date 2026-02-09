---
name: multi-agent-systems
description: "Orchestrate multiple AI agents for complex workflows."
tags: ["multi-agent", "orchestration", "agent-supervisor", "workflows"]
---

# Multi-Agent Systems

## Overview

Build systems with multiple specialized agents coordinated by a supervisor.

## Quick Start

```python
# Define specialized agents
def research_agent(query):
    """Research agent for information gathering"""
    return search_docs(query)

def analysis_agent(data):
    """Analysis agent for data processing"""
    return analyze(data)

def writing_agent(content):
    """Writing agent for content generation"""
    return generate_text(content)

# Supervisor routes to appropriate agent
@mlflow.trace
def supervisor(query):
    intent = classify_intent(query)
    
    if intent == "research":
        return research_agent(query)
    elif intent == "analysis":
        data = research_agent(query)
        return analysis_agent(data)
    else:
        return writing_agent(query)
```

## Common Patterns

### Pattern 1: Agent Router

```python
def route_to_agent(query):
    """Route query to best agent"""
    agents = {
        "technical": technical_agent,
        "billing": billing_agent,
        "general": general_agent
    }
    
    agent_type = classify(query)
    return agents[agent_type](query)
```

### Pattern 2: Sequential Pipeline

```python
def sequential_pipeline(input):
    """Chain agents sequentially"""
    step1 = agent1.process(input)
    step2 = agent2.process(step1)
    step3 = agent3.process(step2)
    return step3
```
