---
name: "MLflow Trace Notifications to Slack"
description: "Send MLflow trace events and agent execution logs to Slack channels for real-time monitoring and debugging."
author: "Databricksters"
url: "https://www.databricksters.com/p/trace-your-steps-back-to-slack"
date: "2025"
tags: ["tracing", "slack", "monitoring", "mlflow", "observability", "databricks"]
---

# MLflow Trace to Slack

## Overview

Send MLflow traces to Slack for real-time monitoring: log agent tool calls, LLM responses, errors, and performance metrics. Enables team visibility into agent behavior without checking MLflow UI.

## Quick Start

```python
import mlflow
from slack_sdk import WebClient

slack_client = WebClient(token="xoxb-...")

def log_to_slack(trace_id, message):
    slack_client.chat_postMessage(
        channel="#agent-monitoring",
        text=f"Trace {trace_id}: {message}"
    )

# Wrap agent execution with Slack logging
with mlflow.start_run():
    mlflow.set_tag("slack_channel", "#agent-monitoring")
    
    result = agent.invoke(query)
    
    # Log trace to Slack
    log_to_slack(
        mlflow.active_run().info.run_id,
        f"Query: {query}\nResult: {result}"
    )
```

## Common Patterns

### Pattern 1: Error Notifications

```python
with mlflow.start_run():
    try:
        result = agent.invoke(query)
    except Exception as e:
        # Alert team in Slack
        slack_client.chat_postMessage(
            channel="#agent-errors",
            text=f"⚠️ Agent failed\nQuery: {query}\nError: {e}"
        )
        raise
```

## FAQ

**Q: How to reduce notification noise?**  
A: Filter by severity: only errors to #alerts, all traces to #monitoring.
