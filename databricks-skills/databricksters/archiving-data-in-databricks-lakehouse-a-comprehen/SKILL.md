---
name: archiving-data-in-databricks-lakehouse-a-comprehen
description: 
---

# Archiving Data in Databricks Lakehouse: A Comprehensive Guide to Cost Optimization and Best Practices

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/archiving-data-in-databricks-lakehouse

## Tags
streaming, ai, performance, deltalake, databricks, clustering

## Full Content

# Archiving Data in Databricks Lakehouse: A Comprehensive Guide to Cost Optimization and Best Practices

**Source:** https://www.databricksters.com/p/archiving-data-in-databricks-lakehouse

---



Archiving Data in Databricks Lakehouse: A Comprehensive Guide to Cost Optimization and Best Practices

Databricksters

Subscribe

Sign in

Archiving Data in Databricks Lakehouse: A Comprehensive Guide to Cost Optimization and Best Practices

Yogesh Gowda

Feb 25, 2025

18

14

Share

As organizations accumulate vast amounts of data, managing storage costs becomes a critical challenge. A common use case in the business intelligence world is to analyze recent data (e.g., the past three years) while archiving older data for compliance or historical purposes. Delta Lake, combined with cloud storage data lifecycle management policies from cloud providers, offers a robust way to achieve this.

In this blog post, we'll explore how to archive data in Lakehouse tables effectively, leveraging online and offline archival tiers, data lifecycle management policies, and best practices to optimize costs while ensuring smooth operations.

Why Archiving Matters in the Lakehouse Architecture

Data storage costs are influenced by two key factors:

Storage Costs: The cost of storing data in different tiers (hot, cool, cold, or archival).

Access Costs: Transaction costs incurred when accessing or listing objects.

Without proper lifecycle management policies, organizations may end up storing all their data in expensive hot tiers, leading to unnecessary expenses. For example, one organization I worked with stored several gigabytes of telemetry data daily in Azure Data Lake Storage's hot tier. Over time, this resulted in skyrocketing costs that could have been avoided with an effective archival strategy.

Key Concepts: Online vs. Offline Archival Tiers

Before diving into implementation details, it's essential to understand the difference between online archival tiers and offline archival tiers offered by cloud storage providers:

Online Archival Tiers

Examples: Azure Cool/Cold tiers, AWS Glacier Instant Retrieval.

Characteristics: Lower storage costs than hot tiers; higher access costs but provide instant access.

Use Case: Suitable for infrequently accessed data that still needs occasional querying.

Offline Archival Tiers

Examples: Azure Archive tier, AWS Glacier Flexible Retrieval, AWS Glacier Deep Archive.

Characteristics: Lowest storage cost but requires manual retrieval before access.

Use Case: Rarely accessed data retained for compliance or long-term storage.

Challenges with Archiving Delta Tables

1. Lifecycle Management Policies Based on File Creation Time

As of this writing Databricks only supports lifecycle management policies based on file creation time, not last modified or last accessed time. This means:

DML operations like UPDATE, MERGE or DELETE rewrite underlying Parquet files due to immutability, resetting their creation date and delaying archival.

Maintenance operations like OPTIMIZE also rewrite files and may delay archival.

2. Querying Archived Data

Delta tables can continue functioning with files stored in offline archival tiers as long as those files are not queried. However:

Offline Archival Tier: Querying these files throws an exception unless they are manually restored to an online tier.

Online Archival Tier: Queries succeed but may incur high access costs if accessed frequently.

3. Preventing Frequent Access to Archived Data

When users have direct access to Delta tables, enforcing predicates to restrict access to archived data can be challenging. This is especially true for interactive queries by end-users.

Implementing an Archival Strategy for Delta Tables

Here’s how you can implement an effective archival strategy for Delta tables in a Lakehouse architecture:

Step 1: Partition Data by Date

Partitioning Delta tables by ingestion date simplifies identifying older data for archival purposes.

CREATE TABLE sales_data 
USING delta
PARTITIONED BY (ingestion_date)
AS SELECT * FROM raw_sales_data;

Step 2: Define Storage Lifecycle Policies

Set up lifecycle management rules at the cloud storage level (e.g., Azure Blob Storage or AWS S3). For example:

Move files from the hot tier to the cool tier after 30 days.

Move files from the cool tier to the archive tier after 335 days.

Example Policy (Azure Blob Storage)

{
&quot;rules&quot;: [
{
&quot;name&quot;: &quot;ArchiveOldGoldData&quot;,
&quot;type&quot;: &quot;Lifecycle&quot;,
&quot;enabled&quot;: true,
&quot;definition&quot;: {
&quot;filters&quot;: {
&quot;blobTypes&quot;: [&quot;blockBlob&quot;],
&quot;prefixMatch&quot;: [&quot;gold/&quot;]
},
&quot;actions&quot;: {
&quot;baseBlob&quot;: {
&quot;tierToCool&quot;: {
&quot;daysAfterCreationGreaterThan&quot;: 30
},
&quot;tierToArchive&quot;: {
&quot;daysAfterCreationGreaterThan&quot;: 365
} } } } }
]
}

Step 3: Set the Archival Retention Period on Delta Table

To align with your cloud lifecycle policy, configure the delta.timeUntilArchived property on your Delta table. This defines the retention period after which files are marked as archived.

Example:

ALTER TABLE sales_data 
SET TBLPROPERTIES (delta.timeUntilArchived = '365 days');

This property ensures that Databricks correctly identifies files as archived after the specified period. Note that this setting does not create or modify cloud lifecycle policies; it only informs Databricks about the archival interval.

Step 4: Use Views to Restrict Access

For interactive queries by end-users, create views that filter out archived data using predicates.

Example:

CREATE VIEW active_sales_data AS 
SELECT * FROM sales_data
WHERE ingestion_date > CURRENT_DATE() - INTERVAL 30 DAYS;

This ensures users only query recent data(less than 30 days old) while automated reporting jobs can still access online tier archived data(less than 365 days old) through direct table access with predicates if needed.

Step 5: Optimize and Vacuum Regularly

Run maintenance operations like OPTIMIZE and VACUUM regularly. OPTIMIZE to compact small files and VACUUM to remove older deleted files not referenced in delta log.Use predicates while running OPTIMIZE to avoid rewriting older files unnecessarily.

Example:

OPTIMIZE events 
WHERE date >= current_timestamp() - INTERVAL 30 days
ZORDER BY (eventType)

Real-Life Example: BI Reporting Use Case

Imagine a BI team that generates daily reports using the past 30 days of sales data and quarterly reports using the past year's data. Here’s how you can design a lifecycle policy:

Move files older than 

30 days

 from the hot tier to the cool tier.

After another 

335 days

 in the cool tier, move them to the archive tier.

Create views that restrict daily reports and any interactive queries to hot-tier data while allowing quarterly reports to access cool-tier data directly using the underlying table but using predicates to exclude offline archival files .

Set the delta table property 

delta.timeUntilArchived

 on the sales table to 

'365 days'

This ensures cost-effective storage while meeting reporting requirements.

Best Practices for Archiving Delta Tables

Choose Suitable Tables for Archival:

Tables used as streaming sources (e.g., telemetry) are good candidates since objects are read only once.

Tables queried by automated jobs with predicates are ideal for tiering.

Use views with predicates for tables accessed interactively by end-users.

Avoid Lifecycle Policies on 

_delta_log/

:

Never apply lifecycle policies to 

_delta_log/ 

directories as they are critical for Delta table metadata.

Monitor DML Operations:

Be aware that 

UPDATE, DELETE, and MERGE

 operations reset file creation dates, delaying archival.

Enforce Predicates for Maintenance Operations:

Always use predicates with 

OPTIMIZE

 command to limit operations to recent partitions.

Educate Users About Access Costs:

Inform users about the cost implications of querying archived data frequ



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
