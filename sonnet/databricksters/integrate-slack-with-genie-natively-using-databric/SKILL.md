---
name: "Native Slack-Genie Integration with Apps"
description: "Build custom Slack integrations using Databricks Apps for tailored Genie experiences with custom branding and workflows."
author: "Databricksters"
url: "https://www.databricksters.com/p/integrate-slack-with-genie-natively-using-databric"
date: "2025"
tags: ["slack", "genie", "databricks-apps", "integration", "databricks"]
---

# Native Slack-Genie with Databricks Apps

## Overview

Databricks Apps enable custom Slack integrations beyond basic Genie: custom slash commands, interactive buttons, workflow automation, and branded experiences. Deploy Flask/FastAPI apps handling Slack webhooks and Genie API calls.

## Quick Start

```python
# app.py - Databricks App
from flask import Flask, request, jsonify
from databricks.sdk import WorkspaceClient

app = Flask(__name__)
w = WorkspaceClient()

@app.route("/slack/command", methods=["POST"])
def handle_slack_command():
    # Parse Slack command
    text = request.form.get("text")
    
    # Query via Genie
    response = w.genie.query(
        space_id="space-id",
        query=text
    )
    
    # Format for Slack
    return jsonify({
        "response_type": "in_channel",
        "text": f"Results: {response.results}"
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
```

## Common Patterns

### Pattern 1: Slash Command

```python
# Slack: /data-query show sales today

@app.route("/slack/data-query", methods=["POST"])
def data_query():
    query_text = request.form.get("text")
    user_id = request.form.get("user_id")
    
    # Execute via Genie with user context
    result = w.genie.query(
        space_id="space-id",
        query=query_text,
        user_id=user_id
    )
    
    return jsonify({"text": format_results(result)})
```

## FAQ

**Q: Difference from built-in Genie Slack integration?**  
A: Apps allow custom UX, workflows, and branding beyond standard @Genie mentions.
