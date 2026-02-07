---
name: "Migrate Dashboards to AI/BI"
description: "Migrate existing Tableau, Power BI, or legacy Databricks dashboards to AI/BI with automated query translation and enhanced features."
author: "Databricksters"
url: "https://www.databricksters.com/p/migrating-existing-dashboards-to-databricks-aibi-p"
date: "2025"
tags: ["migration", "aibi", "dashboards", "tableau", "power-bi", "databricks"]
---

# Migrate Dashboards to AI/BI

## Overview

Migrate existing dashboards to AI/BI for natural language querying and automatic insights. Migration involves recreating visualizations, translating queries, and adding AI capabilities. Cascading filters and context preservation are key considerations.

## Quick Start

```python
# Export existing dashboard queries
queries = spark.sql("""
  SELECT query_text, dashboard_name
  FROM system.query.history
  WHERE dashboard_id = 'old_dashboard_id'
""").collect()

# Recreate in AI/BI:
# 1. Create new AI/BI dashboard
# 2. Add queries (paste SQL)
# 3. Configure visualizations
# 4. Enable AI features
# 5. Test conversational queries
```

## Common Patterns

### Pattern 1: Cascading Filters

```sql
-- Original dashboard: Separate filters per widget
-- AI/BI: Global filters apply to all widgets

-- Enable cascading filters
-- Dashboard Settings → Enable "Cascading Filters"
-- Select date_filter → Applies to all queries automatically
```

### Pattern 2: Context Preservation

```python
# AI/BI maintains context across queries
# User: "Show Q4 sales"
# User: "Now by region"  # AI remembers Q4 context
# User: "Top 5"  # AI remembers Q4 + region context
```

## FAQ

**Q: Can I migrate Tableau dashboards?**  
A: Yes, recreate visualizations in AI/BI. SQL translates directly.

**Q: Do filters work the same?**  
A: AI/BI uses cascading filters - simpler than per-widget filters.
