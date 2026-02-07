---
name: "migrate-dashboards-aibi-filters"
description: "Migrate existing BI dashboards to Databricks AIBI: implement context filters with parameters, cascading filters with field filters and query-based parameters, and filter scope patterns."
---

# Migrating Existing Dashboards to Databricks AIBI: Context and Cascading Filters

## Overview

This skill covers migrating existing BI dashboards to Databricks AIBI Dashboards, focusing on implementing context filters and cascading "Only Relevant Values" filters. Learn how to use parameter filters for context, field filters for simple cascading, and query-based parameters for advanced cascading patterns.

## Quick Start

### Context Filter with Parameters
Implement context filter using dataset parameters:

```sql
-- Dataset: TPCH Sales (Context)
SELECT
  r.r_name              AS region,
  n.n_name              AS nation,
  c.c_custkey           AS customer_id,
  c.c_name              AS customer_name,
  o.o_orderkey          AS order_id,
  o.o_orderdate         AS order_date,
  l.l_extendedprice * (1 - l.l_discount) AS revenue
FROM samples.tpch.region   AS r
JOIN samples.tpch.nation   AS n ON n.n_regionkey = r.r_regionkey
JOIN samples.tpch.customer AS c ON c.c_nationkey = n.n_nationkey
JOIN samples.tpch.orders   AS o ON o.o_custkey   = c.c_custkey
JOIN samples.tpch.lineitem AS l ON l.l_orderkey  = o.o_orderkey
WHERE r.r_name = :region_param  -- Context filter parameter
```

### Helper Dataset for Filter Values
Create helper dataset for filter dropdown:

```sql
-- Dataset: TPCH Regions (Context)
SELECT DISTINCT r_name AS region
FROM samples.tpch.region
ORDER BY region;
```

### Configure Parameter Filter Widget
Set up filter widget:

```python
# Filter Widget Configuration:
# - Title: "Region"
# - Filter type: Single value
# - Fields: TPCH Regions (Context).region
# - Parameters: TPCH Sales (Context).region_param
# - Scope: Page-level or Global
```

## Common Patterns

### Pattern 1: Cascading Filters with Field Filters
Simple cascading using field filters:

```sql
-- Dataset: TPCH Sales (Cascading Pattern 1)
-- No parameters - all filtering via field filters
SELECT
  r.r_name              AS region,
  n.n_name              AS nation,
  c.c_custkey           AS customer_id,
  c.c_name              AS customer_name,
  o.o_orderkey          AS order_id,
  o.o_orderdate         AS order_date,
  l.l_extendedprice * (1 - l.l_discount) AS revenue
FROM samples.tpch.region   AS r
JOIN samples.tpch.nation   AS n ON n.n_regionkey = r.r_regionkey
JOIN samples.tpch.customer AS c ON c.c_nationkey = n.n_nationkey
JOIN samples.tpch.orders   AS o ON o.o_custkey   = c.c_custkey
JOIN samples.tpch.lineitem AS l ON l.l_orderkey  = o.o_orderkey;
```

```python
# Filter Widgets Configuration:
# - Region filter: Field = TPCH Sales (Cascading Pattern 1).region
# - Nation filter: Field = TPCH Sales (Cascading Pattern 1).nation
# - Customer filter: Field = TPCH Sales (Cascading Pattern 1).customer_id
# 
# AIBI automatically recomputes dropdown values based on filtered dataset
# When Region=ASIA selected, Nation dropdown only shows nations in ASIA
```

### Pattern 2: Cascading Filters with Query-Based Parameters
Advanced cascading with parameters:

```sql
-- Main Dataset: TPCH Sales (Cascading Pattern 2)
SELECT
  r.r_name              AS region,
  n.n_name              AS nation,
  c.c_custkey           AS customer_id,
  c.c_name              AS customer_name,
  o.o_orderkey          AS order_id,
  o.o_orderdate         AS order_date,
  l.l_extendedprice * (1 - l.l_discount) AS revenue
FROM samples.tpch.region   AS r
JOIN samples.tpch.nation   AS n ON n.n_regionkey = r.r_regionkey
JOIN samples.tpch.customer AS c ON c.c_nationkey = n.n_nationkey
JOIN samples.tpch.orders   AS o ON o.o_custkey   = c.c_custkey
JOIN samples.tpch.lineitem AS l ON l.l_orderkey  = o.o_orderkey
WHERE (:region_param   = 'All' OR r.r_name = :region_param)
  AND (:nation_param   = 'All' OR n.n_name = :nation_param)
  AND (:customer_param = 0     OR c.c_custkey  = :customer_param);

-- Helper Dataset 1: TPCH Regions (Cascading Pattern 2)
SELECT DISTINCT r_name AS region
FROM samples.tpch.region
ORDER BY region;

-- Helper Dataset 2: TPCH Nations by Region (Cascading Pattern 2)
SELECT DISTINCT n.n_name AS nation
FROM samples.tpch.nation   AS n
JOIN samples.tpch.region   AS r ON n.n_regionkey = r.r_regionkey
WHERE r.r_name = :region_param  -- Parameter from Region filter
ORDER BY nation;

-- Helper Dataset 3: TPCH Customers by Nation (Cascading Pattern 2)
SELECT DISTINCT c.c_custkey AS customer_id
FROM samples.tpch.nation   AS n
JOIN samples.tpch.customer AS c ON c.c_nationkey = n.n_nationkey
WHERE n.n_name = :nation_param  -- Parameter from Nation filter
ORDER BY customer_id;
```

```python
# Filter Widget Configuration:

# Region Filter:
# - Fields: TPCH Regions (Cascading Pattern 2).region
# - Parameters: 
#   * TPCH Sales (Cascading Pattern 2).region_param
#   * TPCH Nations by Region (Cascading Pattern 2).region_param
# - Default: "All"

# Nation Filter:
# - Fields: TPCH Nations by Region (Cascading Pattern 2).nation
# - Parameters:
#   * TPCH Sales (Cascading Pattern 2).nation_param
#   * TPCH Customers by Nation (Cascading Pattern 2).nation_param
# - Default: "All"

# Customer Filter:
# - Fields: TPCH Customers by Nation (Cascading Pattern 2).customer_id
# - Parameters: TPCH Sales (Cascading Pattern 2).customer_param
# - Default: 0
```

## Reference Files

- [AIBI Dashboards Filters](https://docs.databricks.com/en/dashboards/filters.html) - Filter documentation
- [Parameters](https://docs.databricks.com/en/dashboards/parameters.html) - Parameter configuration
- [Companion Dashboard](https://github.com/ArtemChebotko/Migrating-Existing-Dashboards-to-Databricks-AI-BI) - Example dashboard

## Common Issues

| Issue | Solution |
|-------|----------|
| **Context filter not working** | Use parameter filters, not field filters |
| **Cascading not updating** | Ensure filters reference same dataset or parameters are synced |
| **"All" option not working** | Add WHERE clause: `(:param = 'All' OR field = :param)` |
| **Numeric parameters** | Match parameter type to field type (numeric vs string) |
| **Filter scope issues** | Use page-level or global filters as needed |

## Key Takeaways

1. **Context Filters**: Use parameters in dataset SQL + parameter filter widgets
2. **Field Filters**: Operate on query results (client-side for small datasets)
3. **Parameter Filters**: Substitute into SQL (always server-side)
4. **Cascading Pattern 1**: Field filters on single dataset (simplest)
5. **Cascading Pattern 2**: Query-based parameters with helper datasets (more control)
6. **Filter Scope**: Global, page-level, or widget-level

## When to Use Each Pattern

### Use Field Filters (Pattern 1) When:
- Single main dataset per page
- Want simplest authoring experience
- Need "Allow All" option
- Client-side filtering acceptable

### Use Query-Based Parameters (Pattern 2) When:
- Parameters drive multiple datasets
- Need control over dropdown values
- Want custom queries per filter level
- Need parameter reuse across pages

## Complete Migration Workflow

```python
def migrate_dashboard_filters():
    """Complete filter migration workflow"""
    
    # Step 1: Identify context filters
    context_filters = ["Region", "Business Unit", "Date Range"]
    
    # Step 2: Create datasets with parameters
    # - Add WHERE clauses with :param_name
    # - Define parameters in dataset Parameters panel
    
    # Step 3: Create helper datasets for filter values
    # - Simple SELECT DISTINCT queries
    # - Or parameterized queries for cascading
    
    # Step 4: Add filter widgets
    # - Configure as parameter filters for context
    # - Configure as field filters for simple cascading
    # - Configure as query-based parameters for advanced cascading
    
    # Step 5: Set filter scope
    # - Global filters for cross-page filters
    # - Page-level for page-specific filters
    
    print("Filter migration complete!")

# Usage
migrate_dashboard_filters()
```

## Related Skills

- aibi-dashboard-creation
- dashboard-filter-patterns
- parameter-configuration
- bi-dashboard-migration