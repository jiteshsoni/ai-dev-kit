---
name: custom-scorers
description: "Build custom LLM judges for agent evaluation with MLflow."
author: "Databricks"
source: "https://www.databricksters.com/p/agents-are-like-onions-they-have"
tags: ["custom-scorers", "llm-judges", "evaluation", "agent-tooling"]
---

# Custom Scorers

## Overview

Build custom scorers to evaluate specific aspects of agent behavior beyond built-in judges.

## Quick Start

```python
from mlflow.metrics import make_metric, AssessmentSource

class ToolUsageScorer:
    """Evaluate if agent uses correct tools"""
    
    def __call__(self, inputs, outputs, trace):
        # Extract tool calls from trace
        tool_spans = trace.search_spans(span_type="TOOL")
        used_tools = [s.name for s in tool_spans]
        
        # Determine required tools
        required = self.determine_required_tools(inputs["query"])
        
        # Compare
        score = len(set(used_tools) & set(required)) / len(required)
        
        return Feedback(
            value=score > 0.8,
            rationale=f"Used {used_tools}, expected {required}",
            source=AssessmentSource(
                source_type="LLM_JUDGE",
                source_id="tool_usage"
            )
        )
```

## Common Patterns

### Pattern 1: Tool Selection Scorer

```python
def score_tool_selection(trace, expected_tools):
    """Check if agent uses expected tools"""
    actual = [s.name for s in trace.search_spans(span_type="TOOL")]
    return len(set(actual) & set(expected_tools)) / len(expected_tools)
```

### Pattern 2: Latency Scorer

```python
def score_latency(trace, max_ms=1000):
    """Score based on response time"""
    duration = trace.info.execution_time_ms
    return 1.0 if duration < max_ms else 0.5
```
