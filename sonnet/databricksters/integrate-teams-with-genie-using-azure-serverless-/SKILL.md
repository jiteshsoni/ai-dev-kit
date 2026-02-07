---
name: "Microsoft Teams-Genie Integration"
description: "Integrate Databricks Genie with Microsoft Teams using Azure Functions for conversational data queries."
author: "Databricksters"
url: "https://www.databricksters.com/p/integrate-teams-with-genie-using-azure-serverless-"
date: "2025"
tags: ["teams", "genie", "azure-functions", "integration", "databricks"]
---

# Teams-Genie Integration

## Overview

Connect Microsoft Teams to Databricks Genie using Azure Functions as middleware. Teams users send messages to bot, Azure Function calls Genie API, returns formatted results.

## Quick Start

```python
# Azure Function (Python)
import azure.functions as func
from databricks.sdk import WorkspaceClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    # Parse Teams message
    body = req.get_json()
    user_query = body.get("text")
    
    # Query Genie
    w = WorkspaceClient()
    response = w.genie.query(
        space_id="space-id",
        query=user_query
    )
    
    # Format for Teams
    return func.HttpResponse(
        json.dumps({
            "type": "message",
            "text": f"Results: {response.results}"
        }),
        mimetype="application/json"
    )
```

## FAQ

**Q: Can I use Teams bot framework?**  
A: Yes, Azure Functions integrate with Teams Bot Framework SDK.
