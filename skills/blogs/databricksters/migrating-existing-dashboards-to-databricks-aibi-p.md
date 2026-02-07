---
name: migrating-existing-dashboards-to-databricks-aibi-p
description: 
---

# Migrating Existing Dashboards to Databricks AI/BI, Part 1: Context and Cascading Filters

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/migrating-existing-dashboards-to

## Tags
joins, ai, data-engineering, performance, databricks

## Full Content

# Migrating Existing Dashboards to Databricks AI/BI, Part 1: Context and Cascading Filters

**Source:** https://www.databricksters.com/p/migrating-existing-dashboards-to

**Blog:** Databricks

---



Migrating Existing Dashboards to Databricks AI/BI, Part 1: Context and Cascading Filters

Databricksters

Subscribe

Sign in

Data Engineering

Migrating Existing Dashboards to Databricks AI/BI, Part 1: Context and Cascading Filters

How to implement context filters and “only relevant values” behavior in Databricks AI/BI Dashboards

Artem Chebotko

Feb 03, 2026

2

Share

As a Specialist Solutions Architect at Databricks, I regularly work with customers who are migrating critical analytics from existing BI tools to Databricks AI/BI Dashboards – and the first questions I usually get are about filters.

Teams want to know

:

Thanks for reading Databricksters! Subscribe for free to receive new posts and support my work.

Subscribe

“What’s the Databricks equivalent of the context filters we use today?”

“Can we still do cascading filters where each dropdown only shows relevant values?”

“Do you support filter actions when I click on a bar or a point?”

“How do we do user-based filtering in AI/BI Dashboards?”

These aren’t cosmetic features. They’re how analysts actually interact with dashboards, and they’re often the reason an existing BI dashboard feels “alive” instead of static.

In this post, I’ll walk through how to implement two familiar filter patterns from existing BI dashboards in Databricks AI/BI Dashboards, using the built-in 

samples.tpch

 dataset:

Context filters

 → implemented as parameters in dataset SQL

“

Only Relevant Values

” or cascading filters

 → implemented with field filters and query-based parameters

Row-level security and user-based filtering deserve their own deep dive, and action-style interactions (cross-filtering and drill-through) could easily fill another post, so I’ll cover those separately.

I’ve also published the 

companion dashboard

, so you can follow along and inspect the configurations yourself.

Quick primer: datasets and filters in Databricks AI/BI Dashboards

Before we map those patterns, it helps to align on a few AI/BI Dashboards concepts:

Datasets

In AI/BI Dashboards, each dashboard has a 

Data

 tab where you define one or more datasets:

A dataset is defined by an SQL query, direct reference to a Unity Catalog table/view, or an uploaded file.

Multiple visualizations can reuse the same dataset.

Datasets are bundled with the dashboard when you share/export/import it.

Practically, a dataset is your “model” for a set of visuals: one query, many charts.

Field filters vs parameter filters

AI/BI Dashboards support two core ways to filter data from a dashboard: 

field filters and parameter filters

. Both are implemented as 

filter widgets

, but they behave differently under the hood.

Field filters

 are applied directly to dataset fields (columns) on top of the dataset query. Processing behaviour is defined by the 

dataset performance thresholds

. Specifically, for small datasets (≤ 100K rows or ≤ 100MB), results are pulled to the browser and visualization-specific filtering and aggregation are applied client-side. For larger datasets, Databricks wraps the dataset query in a 

WITH

 clause and applies the filter predicates and aggregations in Databricks SQL warehouse (DBSQL).

Parameter filters 

are applied to parameters, which are variables that get substituted into your dataset SQL at runtime. When the parameter value changes, the query is always re-run in DBSQL.

In other words, field filters operate on the results of the dataset query, while parameter filters operate inside the dataset SQL itself.

To speed up processing, various 

caching layers

 in AI/BI Dashboards and DBSQL are used.

We’ll use parameter filters to emulate context filters, and field filters + query-based parameters to emulate “

Only Relevant Values

.”

Filter scope: global, page-level, and widget-level

Filters in AI/BI Dashboards also differ by 

scope

:

Global filters

 are interactive filters in the global filters panel that apply across all pages of the dashboard to any visualization that shares the selected datasets.

Page-level filters

 are interactive filter widgets placed on a specific page in the canvas. They apply to all visualizations on that page that share one or more datasets.

Widget-level filters

 are static filters configured directly on a single visualization widget in its configuration panel. Authors set the values, and viewers can’t change them.

With that foundation in place, we can now map these context filters and “

Only Relevant Values

” patterns into AI/BI Dashboards patterns.

Sample dataset: TPCH on Databricks

To keep examples concrete, we’ll use the TPCH sample data that ships with Databricks in the 

samples.tpch

 schema.

For the purposes of this post, you can start by creating a dataset that joins tables 

region

, 

nation

, 

customer

, 

orders

, and 

lineitem

:

SELECT
 r.r_name AS region,
 n.n_name AS nation,
 c.c_custkey AS customer_id,
 c.c_name AS customer_name,
 o.o_orderkey AS order_id,
 o.o_orderdate AS order_date,
 l.l_extendedprice * (1 - l.l_discount) AS revenue
FROM samples.tpch.region AS r
JOIN samples.tpch.nation AS n ON n.n_regionkey = r.r_regionkey
JOIN samples.tpch.customer AS c ON c.c_nationkey = n.n_nationkey
JOIN samples.tpch.orders AS o ON o.o_custkey = c.c_custkey
JOIN samples.tpch.lineitem AS l ON l.l_orderkey = o.o_orderkey;

In AI/BI Dashboards, you define this query as a dataset in the 

Data

 tab and then reuse it across multiple visualizations. Let’s call this dataset 

TPCH Sales

.

We’ll reuse this same dataset or its derivatives throughout the rest of the post to illustrate context filters and cascading filters.

Implementing context filters with parameters in dataset SQL

What a context filter does

A context filter defines a high-level subset of the data:

The context filter is applied first, often materializing a temporary subset.

Other filters and some calculations are then evaluated on top of that subset.

Context filters are used to:

Improve performance by filtering early and shrinking the working set.

Enforce logical order, such as “

always filter by Region first

.”

Make other filters depend on that subset.

How to think about context in AI/BI Dashboards

Given the primer:

Field filters

 operate on the results of the dataset query (Databricks wraps your dataset SQL and applies them on top).

Parameter filters

 substitute values directly into your dataset SQL, so they filter inside the query, before joins and aggregations.

If you want “context” behavior – 

filter first, then apply everything else

 – you should implement that filter as a 

parameter

 in the dataset SQL, driven by a parameter filter widget.

Pattern: treat the context as a base parameter

Let’s add a context filter for 

Region

:

If you’re following along with the 

companion dashboard

, this setup lives on the “Context filter” page.

Step 1

. Define 

TPCH Sales (Context)

 with a 

Region

 parameter

Create a dataset 

TPCH Sales (Context)

:

SELECT
 r.r_name AS region,
 n.n_name AS nation,
 c.c_custkey AS customer_id,
 c.c_name AS customer_name,
 o.o_orderkey AS order_id,
 o.o_orderdate AS order_date,
 l.l_extendedprice * (1 - l.l_discount) AS revenue
FROM samples.tpch.region AS r
JOIN samples.tpch.nation AS n ON n.n_regionkey = r.r_regionkey
JOIN samples.tpch.customer AS c ON c.c_nationkey = n.n_nationkey
JOIN samples.tpch.orders AS o ON o.o_custkey = c.c_custkey
JOIN samples.tpch.lineitem AS l ON l.l_orderkey = o.o_orderkey
WHERE r.r_name = :region_param -- “context” filter

In the dataset’s 

Parameters

 panel:

Define 

region_param

 with type 

String

.

Optionally set a default (for example, 

AMERICA

) so the dataset runs without any dashboard filter.

This makes 

region_param

 the context for all visuals that use 

TPCH Sales (



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
