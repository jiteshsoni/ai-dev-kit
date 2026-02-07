---
name: "aibi-dashboard-genie-integration"
description: "Integrate Genie Space into Databricks AIBI Dashboards: natural language queries, contextual insights, on-demand exploration, and conversational analytics."
---

# Databricks AIBI Dashboard 2.0: Genie Integration

## Overview

This skill covers integrating Genie Space into Databricks AIBI Dashboards to enable natural language interaction with dashboards. Learn how to enable Genie integration during dashboard publishing, link existing Genie Spaces, create contextual insights from visualizations, and transform static dashboards into dynamic conversational analytics platforms.

## Quick Start

### Enable Genie Integration on Publish
Link Genie Space to dashboard:

```python
# Via UI:
# 1. Create or open AIBI Dashboard
# 2. Click "Publish"
# 3. Toggle "Enable Genie integration"
# 4. Either:
#    - Link existing Genie Space (paste URL)
#    - Create new Genie Space from scratch
# 5. Click "Publish"

# Genie Space URL format:
# https://<workspace-url>/genie/spaces/<space-id>
```

### Create Genie Space for Dashboard
Set up Genie Space with dashboard datasets:

```python
from databricks.genai import GenieClient
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
genie_client = GenieClient(w)

# Create Genie Space
genie_space = genie_client.create_space(
    name="Sales Dashboard Genie Space",
    description="Natural language queries for sales dashboard",
    tables=[
        "catalog.schema.sales_data",
        "catalog.schema.customer_data",
        "catalog.schema.product_data"
    ]
)

print(f"Genie Space created: {genie_space.space_id}")
print(f"Genie Space URL: {genie_space.url}")

# Use this URL when publishing dashboard
```

## Common Patterns

### Pattern 1: Contextual Insights from Visualizations
Enable "Ask Genie" on charts:

```python
# In AIBI Dashboard:
# 1. Right-click any chart or table
# 2. Select "Ask Genie"
# 3. Genie opens with context from that visualization
# 4. Ask follow-up questions about that specific data

# Example workflow:
# - Dashboard shows "Sales by Region" chart
# - Right-click → "Ask Genie"
# - Ask: "Why is the West region performing better?"
# - Genie analyzes West region data and provides insights
```

### Pattern 2: Comprehensive Dataset Coverage
Include additional datasets in Genie Space:

```python
# Dashboard may only show aggregated views
# Genie Space can include underlying detailed tables

genie_space = genie_client.create_space(
    name="Comprehensive Sales Analysis",
    tables=[
        # Dashboard tables
        "catalog.schema.sales_summary",
        "catalog.schema.region_summary",
        
        # Additional detailed tables for deeper queries
        "catalog.schema.sales_transactions",  # Detailed transaction data
        "catalog.schema.customer_segments",   # Customer segmentation
        "catalog.schema.product_catalog",      # Product details
        "catalog.schema.marketing_campaigns"  # Marketing data
    ]
)

# Users can ask questions beyond dashboard scope
# Example: "Show me all transactions for high-value customers in Q4"
```

### Pattern 3: Flexible Genie UI
Configure Genie display options:

```python
# Genie can be displayed in multiple ways:

# Option 1: Overlay (default)
# - Genie chat overlays dashboard
# - Click outside to close
# - Good for quick queries

# Option 2: Docked to side
# - Genie panel docks to right/left side
# - Dashboard remains visible
# - Good for continuous exploration

# Option 3: Separate tab
# - Opens Genie in new tab
# - Full-screen Genie experience
# - Good for complex analysis

# Users can switch between modes as needed
```

## Reference Files

- [AIBI Dashboards](https://docs.databricks.com/en/dashboards/index.html) - Dashboard documentation
- [Genie Spaces](https://docs.databricks.com/en/generative-ai/genie/index.html) - Genie documentation
- [Dashboard Publishing](https://docs.databricks.com/en/dashboards/publish.html) - Publishing guide

## Common Issues

| Issue | Solution |
|-------|----------|
| **Genie not appearing** | Verify Genie integration enabled during publish |
| **Access denied** | Ensure users have DBSQL Serverless warehouse access |
| **Table access denied** | Grant SELECT permissions on Genie Space tables |
| **Genie Space not found** | Verify Genie Space URL is correct |
| **No contextual insights** | Ensure Genie Space includes dashboard datasets |

## Key Takeaways

1. **Enable on Publish**: Toggle Genie integration when publishing dashboard
2. **Link or Create**: Link existing Genie Space or create new one
3. **Contextual Insights**: Right-click charts to ask Genie questions
4. **Flexible UI**: Overlay, docked, or separate tab modes
5. **Comprehensive Data**: Include additional datasets beyond dashboard scope
6. **Access Requirements**: Users need DBSQL Serverless and table permissions

## Usage Examples

### Example 1: Ask Genie Button
```python
# Top-right corner "Ask Genie" button
# - Click to open Genie chat
# - Ask general questions about dashboard data
# - Example: "What are the key trends in this dashboard?"
```

### Example 2: Contextual Questions
```python
# Right-click visualization → "Ask Genie"
# - Genie receives context from that chart
# - Ask follow-up questions
# - Example: "Why did sales drop in Q3?" (on sales chart)
```

### Example 3: Deep Dive Analysis
```python
# Use Genie for analysis beyond dashboard
# - Dashboard shows summary metrics
# - Genie can query detailed tables
# - Example: "Show me all customers who churned in the last 30 days"
```

## Complete Setup Workflow

```python
def complete_aibi_genie_setup():
    """Complete setup workflow"""
    
    # Step 1: Create Genie Space
    genie_space = genie_client.create_space(
        name="Dashboard Genie Space",
        tables=[
            "catalog.schema.dashboard_table1",
            "catalog.schema.dashboard_table2"
        ]
    )
    
    # Step 2: Grant permissions
    # Ensure users have:
    # - DBSQL Serverless warehouse access
    # - SELECT on Genie Space tables
    
    # Step 3: Create/Edit Dashboard
    # Build dashboard with visualizations
    
    # Step 4: Publish with Genie
    # - Click "Publish"
    # - Enable "Genie integration"
    # - Paste Genie Space URL: genie_space.url
    # - Click "Publish"
    
    # Step 5: Use Genie
    # - Click "Ask Genie" button
    # - Right-click charts for contextual questions
    # - Explore data with natural language
    
    print("AIBI Dashboard with Genie ready!")

# Usage
complete_aibi_genie_setup()
```

## When to Use This Skill

- Adding natural language queries to dashboards
- Enabling on-demand data exploration
- Providing contextual insights from visualizations
- Transforming static dashboards into interactive analytics
- Reducing dashboard update cycles

## Related Skills

- aibi-dashboard-creation
- genie-space-setup
- natural-language-queries
- conversational-analytics