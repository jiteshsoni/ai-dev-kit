---
name: "Agent Configuration Management"
description: "Manage agent configurations using config.json for environment-specific settings, model endpoints, and parameter tuning."
author: "Databricksters"
url: "https://www.databricksters.com/p/cheese-and-rice-thats-configjson-bourne"
date: "2025"
tags: ["configuration", "agents", "deployment", "best-practices", "databricks"]
---

# Agent Configuration Management

## Overview

Externalize agent configurations to config.json for environment-specific settings: model endpoints, parameters, system prompts, and tool configurations. Enables dev/staging/prod deployments without code changes.

## Quick Start

```json
{
  "llm_endpoint": "databricks-meta-llama-3-1-70b-instruct",
  "temperature": 0.1,
  "max_tokens": 500,
  "system_prompt": "You are a helpful assistant.",
  "vector_search": {
    "endpoint": "vs_endpoint",
    "index": "catalog.schema.docs"
  },
  "tools": ["search_database", "send_email"]
}
```

```python
import json

# Load config
with open("config.json") as f:
    config = json.load(f)

# Create agent from config
llm = ChatDatabricks(
    endpoint=config["llm_endpoint"],
    temperature=config["temperature"],
    max_tokens=config["max_tokens"]
)
```

## Common Patterns

### Pattern 1: Environment-Specific Configs

```
config.dev.json    → Development settings
config.staging.json → Staging settings
config.prod.json   → Production settings
```

```python
import os

env = os.getenv("ENV", "dev")
config_file = f"config.{env}.json"
config = json.load(open(config_file))
```

## FAQ

**Q: Where to store config files?**  
A: Unity Catalog Volumes for Unity Catalog-managed access control.

**Q: How to handle secrets?**  
A: Use Databricks Secrets, not config files. Reference secret scope/key in config.
