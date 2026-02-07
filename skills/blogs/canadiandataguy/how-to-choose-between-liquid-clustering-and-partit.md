---
name: how-to-choose-between-liquid-clustering-and-partit
description: 
---

# How to Choose Between Liquid Clustering and Partitioning with Z-Order in Databricks

## Overview


## Source
- **Author:** Canadian Data Guy
- **URL:** https://www.canadiandataguy.com/p/optimizing-delta-lake-tables-liquid

## Tags
streaming, kafka, joins, ai, performance, deltalake, databricks, clustering

## Full Content

# How to Choose Between Liquid Clustering and Partitioning with Z-Order in Databricks

**Source:** https://www.canadiandataguy.com/p/optimizing-delta-lake-tables-liquid

**Blog:** Canadian Data Guy

---



How to Choose Between Liquid Clustering and Partitioning with Z-Order in Databricks

Canadian Data Guy Unfiltered

Subscribe

Sign in

How to Choose Between Liquid Clustering and Partitioning with Z-Order in Databricks

The views expressed in this blog are my own and do not represent official guidance from Databricks

Canadian Data Guy

 and 

Geethu

Jan 15, 2026

9

3

1

Share

This is one of the most-read posts on the website, so we decided to give it a well-deserved 2026 update. Thank you to 

Geethu

 for co-authoring on this revision and for raising the technical bar of the article.

Delta Lake

, an open source storage format, offers two primary methods for organizing data: 

liquid clustering

 and 

partitioning with Z-order

. This blog post will help you navigate the decision-making process between these two approaches. Clustering in Delta Lake enhances query performance by organizing data based on frequently accessed columns, similar to indexing in relational databases. The key difference is that clustering physically sorts the data within the table rather than creating separate index structures.

Understanding the Basics: Liquid Clustering vs. Partitioned Z-Order Tables

Liquid Clustering

Liquid clustering is a newer algorithm for Delta Lake tables, offering several advantages:

Flexibility

: You can change clustering columns at any time.

Optimization for Unpartitioned Tables

: It works well without partitioning.

Efficiency

: It doesn’t re-cluster previously clustered files unless explicitly instructed.

Liquid clustering relies on 

optimistic concurrency control (OCC)

 to handle conflicts when multiple writes occur to the same table.

Partitioned Z-Order Tables

Partitioning combined with Z-ordering is a traditional approach that:

Control

: Allows greater control over data organization.

Parallel Writes

: Supports parallel writes more effectively.

Fine-Grained Optimization

: Enables optimization of specific partitions.

However, data engineers must be aware of querying patterns upfront to choose an appropriate partition column.

Decision Tree

Built in Jan 2026, this decision tree will be continuously updated as technology evolves. As new enhancements emerge, my understanding will grow, and this resource will be refined accordingly. This is a complex topic, but I will do my best to provide at least an intuitive grasp to help you develop a clearer understanding.

Factors to Consider When Choosing

Table Size

Small tables (&lt; 10 TB)

: If you need fast lookups on exactly two columns, Liquid Clustering on those columns typically delivers comparable performance with simpler maintenance. If your workload involves highly selective lookups across three or more columns, Partition + Z-order may perform better, assuming the partition key has low cardinality. That said, Liquid Clustering can still work for multi-column lookups and is often worth benchmarking with tuned clustering keys.

Medium tables (10 TB -500TB)

: For medium-sized tables, the key decision factor is partition cardinality. If partitioning results in fewer than ~5,000 distinct values (for example, ~1,100 partitions for 3 years of daily data), Partition + Z-order can work well when queries include the partition column. If the number of distinct values exceeds ~5,000, Liquid Clustering is generally preferred to avoid over-partitioning. In practice, benchmark both approaches with representative queries to validate performance.

Large tables (> 500 TB)

: You should reach out to your Databricks representative and have a discussion.

Note: Liquid is being actively improved so the guidance could change 

Data Ingestion Pattern

How data is written - batch or streaming - can influence which data organization strategy is most appropriate.

Batch Ingestion : 

For batch workloads, Liquid Clustering remains a strong default choice. Batch writes naturally organize data efficiently. In the latest Databricks Runtime versions, eager clustering can be enabled to make the data well-clustered as it is written, so queries see an optimized view right away.

Streaming Ingestion : 

For streaming workloads, the choice depends on your main priority.

Low Latency:

 If getting data into the table quickly is most important, Liquid Clustering is preferred without eager clustering. This reduces shuffle overhead during ingestion. Data may not be fully optimized immediately, but query performance can improve later using Predictive I/O.

Fast Downstream Lookups:

 If queries need to be fast as soon as data arrives, Liquid Clustering with eager clustering is recommended. This ensures data is well-clustered on write, and follow-up OPTIMIZE can further improve query performance.

Query Patterns

If users consistently include the partition column in their queries, partitioning can be very effective.

Liquid clustering may be more suitable for more flexible query patterns where users may not always include the partition column.

Data Distribution

If you have uneven partition sizes, the liquid will be better.

Date-based data (e.g., clickstream data) often benefits from partitioning.

For data without a clear partitioning strategy, liquid clustering may be better.

Partition Column Selection

When choosing a partition column:

Select immutable columns (e.g., click date, sale date)

Avoid high-cardinality columns like timestamps

For timestamp data, create a derived date column for partitioning

Aim for fewer than 10,000 distinct partition values.

Each partition should contain at least ~1-10 GB of data.

Real-World Example: Amazon Clickstream Data

Let's consider a real-world scenario using Amazon's clickstream data:

The table stores 3 years of data for 10 countries

Partitioning by click date results in approximately 1,000 partitions (365 * 3)

10 countries * 1,000 date partitions = 10,000 total partitions

This setup is within the recommended partition count (&lt; 10,000) and provides good control over the data. Here's how we might structure this table:

Partition by 

click_date, country

Z-order by 

merchant_id

, and 

advertiser_id

Optimizing the Partitioned Table

To maintain optimal performance, you can run a daily optimization job on the newest partition:

OPTIMIZE table_name
WHERE click_date = 'ANY_DATE' and country = 'CANADA'
ZORDER BY ( merchant_id, advertiser_id)

This approach ensures good performance for date-range queries and lookups on Z-ordered columns.

Optimistic Concurrency Control

Delta Lake uses optimistic concurrency control to manage parallel writes. Here's how it works:

Writers check the current version of the Delta table (e.g., version 100).

They attempt to write a new JSON file (e.g., 101.json).

Only one writer can succeed in creating this file.

The &quot;losing&quot; writer checks if there are conflicts with what was previously written.

If no conflicts, it creates the next version (e.g., 102.json).

This approach works well for appends but can be challenging for updates, especially when multiple writers are trying to modify the same files.

Potential Pitfalls and Best Practices

Here are some key considerations and common mistakes to avoid:

Do not add 

Co-related columns

 to liquid: If two columns are highly correlated, you only need to include one of them as a clustering key. Example, if you have click_date, click_timestamp then only cluster by click_timestamps

Skip meaningless keys:

 When it comes to clustering, try to avoid using meaningless keys such as UUIDs, which are inheritable and unsortable strings. If possible, refrain from using them in both liquid and z-order clustering. However, I understand that sometimes customers require quick lookups on these UUID columns. In those cases, you may include them.

Over-Partitioning

: A common mistake 



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
