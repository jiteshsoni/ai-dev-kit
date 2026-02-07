---
name: write-anywhere-read-everywhere-achieving-true-data
description: 
---

# Write Anywhere, Read Everywhere: Achieving True Data Interoperability Between Databricks and Snowflake

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/write-anywhere-read-everywhere-achieving

## Tags
streaming, ai, spark, data-engineering, deltalake, databricks

## Full Content

# Write Anywhere, Read Everywhere: Achieving True Data Interoperability Between Databricks and Snowflake

**Source:** https://www.databricksters.com/p/write-anywhere-read-everywhere-achieving

---



Write Anywhere, Read Everywhere: Achieving True Data Interoperability Between Databricks and Snowflake 

Databricksters

Subscribe

Sign in

Write Anywhere, Read Everywhere: Achieving True Data Interoperability Between Databricks and Snowflake 

This blog will show you how to eliminate data silos between Databricks and Snowflake using Federation, enabling you to write from anywhere and read from everywhere under unified governance.

Nikhil Mishra

Jul 15, 2025

1

Share

Update, July 23, 2025: 

This Snowflake 

documentation

 explains 

catalog-linked databases

, a new feature that enables Snowflake to directly connect to and interact with external Apache Iceberg™ REST catalogs

Understanding the Foundation: Apache Iceberg and Catalogs

Before diving into the Federation solution, it's essential to understand the technology that enables seamless interoperability: Apache Iceberg.

What is Apache Iceberg?

Apache Iceberg is an open table format designed for large-scale data storage in data lakes. Think of it as a sophisticated &quot;table of contents&quot; for your data files in cloud storage. Unlike traditional file formats, Iceberg provides:

Thanks for reading Databricksters! Subscribe for free to receive new posts and support my work.

Subscribe

ACID transactions for data consistency

Time travel capabilities to query historical data

Schema evolution without breaking existing queries

Efficient query planning through rich metadata

Why Iceberg Needs a Catalog

Here's the imp point: Iceberg tables require a catalog to function. The catalog serves as the &quot;phone book&quot; that:

Tracks table locations in cloud storage

Manages metadata about schemas, partitions, and snapshots

Handles concurrent access from multiple engines

Provides governance and access control

The Iceberg REST Catalog Standard

The Iceberg REST Catalog is a standardized API that allows any Iceberg-compatible engine to interact with table metadata. This is crucial because it means:

Any engine (Databricks, Snowflake, Trino, etc.) can read the same tables

Unified governance through a single catalog interface

No vendor lock-in - your data remains accessible across platforms

Unity Catalog implements this Iceberg REST Catalog standard, making it the perfect bridge between different data platforms.

The Customer Challenge I See Often

We'll now explore how Federation can address another common interoperability challenge I frequently observe with customers. This involves scenarios where customers use both Snowflake and Databricks, desiring to write from both engines while making all data accessible to both.

In essence, the goal is to write from anywhere and read from everywhere.

The Real-World Problem

Organizations using both platforms consistently face these pain points:

Data trapped in silos

: Teams can't easily access data created in the other platform

Complex data movement

: Engineers spend time building pipelines just to move data between systems

Governance fragmentation

: Security policies and compliance become nightmares to manage

Limited flexibility

: Adding new analytics engines requires extensive integration work

The solution?

 Federation in both directions.

The Federation Solution: Unity Catalog as the Bridge

To achieve seamless interoperability, Federation can be employed in both directions:

From Databricks to Snowflake: Lakehouse Federation

Lakehouse Federation

 allows you to register a Snowflake Horizon catalog within Unity Catalog (UC). Once external locations are created for all Iceberg tables in your federated catalog, UC will know to access object storage to read those tables.

From Snowflake to Databricks: Catalog Integration

In the reverse direction, Snowflake offers a 

&quot;catalog integration&quot;

 that operates very similarly to Lakehouse Federation. Ultimately, this involves registering the Unity Catalog within Snowflake as an external catalog, enabling you to query these tables directly within Snowflake.

The Architecture's Key Advantage: Flexibility

A significant advantage of this architecture is its flexibility. For example, if you decide to utilize an open-source Iceberg engine, as previously discussed, UC already supports the Iceberg REST catalog. This allows you to connect, for instance, Trino to the Iceberg REST Catalog API for writing or reading to UC. Furthermore, all tables federated from the Snowflake Horizon Catalog are available in UC, meaning Trino could also read your Snowflake-managed Iceberg tables from Unity Catalog—

all under unified governance

.

The Complete Federation Architecture

The solution leverages 

bidirectional federation

 to create a seamless data ecosystem where any engine can access data from any other:

This architecture enables:

Databricks

 can access Snowflake-managed tables through Lakehouse Federation

Snowflake

 can access Databricks-managed tables through Catalog Integration

Third-party engines

 like Trino can access both through Unity Catalog's Iceberg REST API

Unified governance

 across all platforms and engines

Implementation Guide: Step-by-Step

Prerequisites

Before implementing the solution, ensure you have:

Databricks Unity Catalog

 enabled and configured

Snowflake account

 with appropriate privileges

Cloud storage access

 is configured for both platforms

Authentication credentials

 (service principal recommended)

Step 0: Enable External Data Access on Metastore

Create data assets

In your Databricks workspace, click 

Catalog

Click the gear icon at the top of the 

Catalog

 pane and select 

Metastore

On the 

Details

 tab, enable 

External data access

This setting allows external engines to access data in your metastore through the Unity Catalog REST APIs. It's disabled by default for security.

Step 1: Grant Access in Databricks

First, give Snowflake permission to access your Unity Catalog tables:

-- Grant access to your schema

GRANT EXTERNAL USE SCHEMA ON SCHEMA nmishra_catalog.federation_demo TO 'your-service-principal@company.com';

-- Or Grant access to specific tables

GRANT SELECT ON TABLE nmishra_catalog.federation_demo.orders TO 'your-service-principal@company.com';

GRANT SELECT ON TABLE nmishra_catalog.federation_demo.customers TO 'your-service-principal@company.com';

Step 2: Create Catalog Integration in Snowflake

Connect Snowflake to Unity Catalog:

Step 3: Create External Tables in Snowflake

Map Unity Catalog tables to Snowflake:
-- Create table that points to Unity Catalog

CREATE ICEBERG TABLE orders_from_databricks
CATALOG = nmishra_databricks_integration
CATALOG_TABLE_NAME = 'nmishra_catalog.federation_demo.orders';

-- Create another table
CREATE ICEBERG TABLE customers_from_databricks
CATALOG = nmishra_databricks_integration
CATALOG_TABLE_NAME = 'nmishra_catalog.federation_demo.customers';

💡 Snowflake Private Preview

: For automatic table syncing instead of manual table creation, Snowflake offers 

catalog-linked databases

 in private preview. This feature automatically syncs all Unity Catalog-managed Iceberg table metadata, eliminating the need to manually create individual tables. 

Learn more about catalog-linked databases

 and contact your Snowflake account team for access.

Step 4: Query Across Platforms

Now query Databricks tables directly from Snowflake:

-- Query your Databricks table from Snowflake

SELECT * FROM orders_from_databricks LIMIT 10;

-- Query another table

SELECT * FROM customers_from_databricks LIMIT 10;

Understanding Iceberg Table Metadata

When Snowflake accesses your Unity Catalog tables, it's reading rich Iceberg metadata that enables advanced capabilities:

Key Iceberg Features Your Tables Provide:

Time Travel

: Query historical versions using snapshot IDs

Schema Evolution

: Add/modify colu



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
