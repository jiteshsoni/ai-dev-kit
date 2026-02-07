---
name: 10-lessons-from-analyzing-and-tuning-two-dozen-dat
description: 
---

# 10 Lessons from Analyzing and Tuning Two Dozen Databricks SQL Warehouses

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/10-lessons-from-analyzing-and-tuning

## Tags
joins, ai, performance, data-engineering, deltalake, databricks, clustering

## Full Content

# 10 Lessons from Analyzing and Tuning Two Dozen Databricks SQL Warehouses

**Source:** https://www.databricksters.com/p/10-lessons-from-analyzing-and-tuning

**Blog:** Databricks

---



10 Lessons from Analyzing and Tuning Two Dozen Databricks SQL Warehouses

Databricksters

Subscribe

Sign in

Data Engineering

10 Lessons from Analyzing and Tuning Two Dozen Databricks SQL Warehouses

How to cut cost and boost performance

Artem Chebotko

Nov 07, 2025

5

2

Share

As a Specialist Solutions Architect at Databricks, over the past year I’ve led the Databricks SQL Cost &amp; Performance Optimization Assessment initiative, reviewing and tuning two dozen customer warehouses across industries such as ad tech, healthcare, fintech, and energy. Each engagement shared the same goal — improving performance, reducing cost, and uncovering hidden inefficiencies. While every environment is unique, several recurring themes emerged. The following ten lessons highlight recurring optimization patterns and practical takeaways that helped customers achieve measurable cost and performance improvements. Of course, these are generalizations — exceptions may apply depending on workload characteristics, data layout, and other factors.

1. Liquid Clustering effectively saves compute and improves performance

This should be foundational, yet I continue to see multi-terabyte tables without any data-layout optimization, or with Liquid Clustering misused as hierarchical sorting. In some cases, tables are clustered on too many columns, degrading clustering efficiency. In both scenarios, file pruning is limited, and queries end up scanning far more data than necessary.

Thanks for reading Databricksters! Subscribe for free to receive new posts and support my work.

Subscribe

Lesson:

 Use 

Liquid Clustering

 strategically to improve query filtering and data skipping. Follow 

Databricks best practices

 when defining clustering keys.

2. Predictive Optimization delivers ongoing performance and cost gains

Predictive Optimization automatically improves managed Delta tables by compacting files, applying Liquid Clustering, collecting statistics, and deleting old files, reducing both compute and storage costs. It continuously observes workloads and schedules these maintenance operations automatically.

Lesson:

 Enable 

Predictive Optimization

 on Unity Catalog managed tables to benefit from continuous layout tuning and automatic vacuuming — improving performance and lowering storage spend with no manual jobs to manage.

3. Missing statistics are a common performance issue

Across most environments, I found tables with missing or partial column statistics, often due to external table ingestion or infrequent maintenance. Without accurate statistics, the optimizer can’t effectively estimate data volumes or apply efficient file pruning.

Lesson:

 Run 

ANALYZE TABLE

 regularly or rely on 

Predictive Optimization

 to automate stats collection for managed Delta tables. Missing statistics can significantly degrade query performance.

4. Disk spills are silent performance killers

Between 5% and 20% of queries in typical environments spill to disk during shuffles, adding seconds or even minutes to query runtime. These issues often go unnoticed because queries still “succeed,” but at a high cost.

Lesson:

 Identify queries with significant disk spill by analyzing the 

query history system table

. Rewriting a query, adding 

repartitioning or join hints

, splitting large queries into smaller ones that process subsets of data, or increasing warehouse size are all valid strategies to mitigate spills. Reducing disk spills alone can yield a 10–30% performance improvement.

5. Tiny files are the hidden tax of inefficient ingestion

One customer’s ingestion pipeline wrote one file per row — millions of files, each only a few kilobytes. These microfiles result in drastically slow reads, increased metadata overhead, and make optimization more expensive.

Lesson:

 Adjust partitioning or writer settings in your ingestion pipelines to produce fewer, larger files. Regularly compact small files using the 

OPTIMIZE

 command, or rely on 

Predictive Optimization

 to automate compaction for managed Delta tables. Excessive small files can significantly degrade query performance and increase compute and metadata management costs.

6. Large inserts and replaces drive up cost

Many teams refresh large datasets using 

INSERT OVERWRITE

, 

REPLACE WHERE

, or even 

CREATE OR REPLACE TABLE

 — for example, rewriting an entire 30-day time series when only a small subset of rows has changed. This results in billions of rows being rewritten unnecessarily each day.

Lesson:

 Replace full overwrites with incremental 

MERGE

 operations whenever possible. 

Optimize merge performance

 using 

Liquid Clustering

. In one environment, this simple change reduced warehouse runtime by about 50%.

7. Cross-system queries often run slower without Photon acceleration

Federated queries — for example, Databricks reading from Snowflake or other external systems — often fall out of Photon execution mode, forcing execution to fall back to slower row-based processing. This can significantly increase query latency and compute cost.

Lesson:

 Keep frequently joined or high-volume tables within Databricks when possible, or create materialized views over foreign tables to reduce repeated cross-system access. Use 

Lakehouse Federation

 selectively to balance interoperability with performance. When an external system supports 

Delta Sharing

 for Delta Lake tables, it is typically a better option than federated queries because it preserves Photon acceleration.

8. Auto-stop settings are often too conservative

Many warehouses retain the default 10-minute idle timeout, or even increase it, despite intermittent workloads that provide frequent opportunities for the warehouse to scale down to zero. This leads to clusters sitting idle while still incurring DBU costs.

Lesson:

 Reducing the auto-stop setting to 5 minutes via the UI — or even 1 minute via 

REST API

 — can save thousands of dollars monthly, especially for scheduled or bursty workloads. For continuously running environments, consider right-sizing instead of keeping large warehouses idling.

9. BI tools can create inefficiencies by keeping connections alive and not properly cancelling queries

A frequent source of hidden inefficiency comes from BI tools that keep connections alive or fail to cancel submitted queries properly. In the first case, clients send periodic heartbeat queries to keep sessions open, preventing the warehouse from shutting down. In the second, clients stop polling for results from submitted queries, leaving the warehouse in a waiting state until it cancels them with the familiar 

“Query has been timed out due to inactivity”

 message.

Lesson:

 Review how your BI tools handle connection pooling, query cancellation, and session timeouts. Proper client configuration prevents idle workloads from consuming compute and improves overall warehouse efficiency.

10. Consolidating compatible workloads improves efficiency and reduces cost

Most teams maintain multiple warehouses for BI, ETL, ad hoc querying, metadata exploration, and data science. While such separation is necessary for workloads with different latency or concurrency requirements, running too many similar warehouses creates unnecessary startup, scaling, and caching overhead.

Lesson:

 Consolidate workloads with similar characteristics and latency expectations onto shared warehouses with appropriate scaling policies. This approach can deliver 20–40% cost savings, improves cache reuse, and simplifies governance. Use a small dedicated warehouse for lightweight metadata exploration instead of a large production warehouse.

Bonus Insights

Avoid running 

OPTIMIZE

 after every write. In one case, a dbt job was configured to do exactly that — wasting DBUs without providing mean



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
