---
name: "AI/BI Dashboards 2.0 Features"
description: "Build intelligent dashboards with AI/BI 2.0 features: natural language queries, automatic insights, and conversational analytics."
author: "Ambarish"
url: "https://www.databricksters.com/p/databricks-aibi-dashboard-20-by-ambarish"
date: "2025"
tags: ["aibi", "dashboards", "genie", "analytics", "databricks"]
---

# AI/BI Dashboards 2.0

## Overview

AI/BI Dashboards 2.0 adds natural language queries, automatic insights generation, and conversational analytics to traditional BI dashboards. Users ask questions in plain English, AI generates SQL, and results render as visualizations automatically.

## Quick Start

```sql
-- Create AI/BI dashboard
-- UI: SQL Editor → Create → AI/BI Dashboard

-- Natural language query examples:
-- "Show top 10 customers by revenue this quarter"
-- "What's the trend in daily active users?"
-- "Compare sales across regions"

-- AI generates SQL automatically:
SELECT 
  customer_name,
  SUM(revenue) as total_revenue
FROM sales
WHERE quarter = QUARTER(CURRENT_DATE())
GROUP BY customer_name
ORDER BY total_revenue DESC
LIMIT 10;
```

## Common Patterns

### Pattern 1: Conversational Follow-ups

```
User: "Show sales by region"
→ AI generates: SELECT region, SUM(amount) FROM sales GROUP BY region

User: "Now show only regions > $1M"
→ AI understands context: 
SELECT region, SUM(amount) as total 
FROM sales 
GROUP BY region 
HAVING total > 1000000
```

### Pattern 2: Automatic Insight Discovery

```python
# AI/BI automatically identifies:
# - Trends (increasing/decreasing)
# - Anomalies (outliers)
# - Correlations (related metrics)
# - Seasonality (patterns)
```

## FAQ

**Q: Does this replace SQL?**  
A: No, it complements. Power users can still write SQL directly.

**Q: How accurate are generated queries?**  
A: High accuracy for standard patterns. Review generated SQL for complex analytics.
