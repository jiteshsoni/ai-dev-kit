---
name: "Genie in Slack Integration"
description: "Enable natural language data queries in Slack using Databricks Genie integration for conversational analytics."
author: "Databricksters"
url: "https://www.databricksters.com/p/chat-with-your-data-in-slack-using-databricks-geni"
date: "2025"
tags: ["genie", "slack", "integration", "conversational-ai", "databricks"]
---

# Genie in Slack Integration

## Overview

Databricks Genie integration enables Slack users to query data using natural language. Users ask questions in Slack, Genie generates SQL, executes queries, and returns results as formatted messages. Requires Genie Space and Slack app configuration.

## Quick Start

```python
# 1. Create Genie Space
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

space = w.genie.create_space(
    display_name="Analytics Space",
    description="Query sales and customer data",
    schema_names=["catalog.sales", "catalog.customers"]
)

# 2. Install Slack App (via Databricks UI)
# Workspace Settings → Integrations → Slack
# Authorize Databricks app in Slack workspace

# 3. Link Genie Space to Slack channel
# Genie UI → Space Settings → Add to Slack → Select channel

# 4. Users query in Slack:
# @Genie What were total sales last month?
# @Genie Show top 10 customers by revenue
```

## Common Patterns

### Pattern 1: Restrict Data Access

```sql
-- Create view limiting accessible data
CREATE VIEW sales_team_view AS
SELECT * FROM sales
WHERE region = 'US' AND date >= CURRENT_DATE() - INTERVAL 90 DAYS;

-- Grant access only to view
GRANT SELECT ON VIEW sales_team_view TO sales_team;

-- Configure Genie Space with view
-- Space Settings → Schemas → Add sales_team_view
```

### Pattern 2: Custom Instructions

```
Genie Space → Settings → Instructions:

"When calculating revenue, always use net_revenue column, not gross_revenue.
For customer counts, only include active customers (status = 'active').
Round currency to 2 decimal places."
```

## FAQ

**Q: Does Genie work in private Slack channels?**  
A: Yes, invite @Genie to private channels.

**Q: Can users query any table?**  
A: No, Genie respects Unity Catalog permissions. Users see only their authorized data.

**Q: How to monitor Genie usage?**  
A: Query system.genie.query_history for usage analytics.
