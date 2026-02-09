---
name: genie-spaces
description: "Natural language data querying with Databricks Genie Spaces."
tags: ["genie", "natural-language", "sql", "ai-bi"]
---

# Genie Spaces

## Overview

Genie Spaces enable natural language querying of your data warehouse.

## Quick Start

### Create Genie Space

```python
# In Databricks UI: AI/BI → Genie
# 1. Select tables
# 2. Add business context
# 3. Share with team
```

### Query via API

```python
# Start conversation
conversation = genie.start_conversation(
    space_id="space_id",
    question="What were sales last month?"
)

# Get results
results = genie.get_results(conversation.id)
```

## Common Patterns

### Pattern 1: Slack Integration

```python
# Integrate Genie with Slack
# Users ask questions in Slack, Genie responds
```

### Pattern 2: Teams Integration

```python
# Microsoft Teams integration
# Natural language queries in Teams
```
