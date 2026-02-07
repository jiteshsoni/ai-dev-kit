---
name: "Databricks-Snowflake Federation via Iceberg"
description: "Achieve bidirectional data interoperability between Databricks and Snowflake using Unity Catalog Iceberg REST API for unified governance without data movement."
author: "Nikhil Mishra"
url: "https://www.databricksters.com/p/write-anywhere-read-everywhere-achieving"
date: "2025-07-15"
tags: ["federation", "snowflake", "iceberg", "unity-catalog", "interoperability", "databricks"]
---

# Databricks-Snowflake Federation via Iceberg

## Overview

Enable "write anywhere, read everywhere" between Databricks and Snowflake using Unity Catalog's Iceberg REST Catalog implementation. Eliminates data silos and movement pipelines through bidirectional federation with unified governance. Both platforms access the same Iceberg tables in cloud storage through standardized REST API.

**Use this skill when:** Integrating Databricks and Snowflake environments, eliminating data duplication, or enabling multi-engine analytics on shared data.

## Quick Start

### Enable Snowflake to Read Databricks Tables

```sql
-- Step 1: Enable external access in Databricks
-- UI: Catalog → Metastore → Enable "External data access"

-- Step 2: Grant Snowflake access to Unity Catalog tables
GRANT EXTERNAL USE SCHEMA ON SCHEMA main.analytics 
TO 'snowflake-sp@company.com';

GRANT SELECT ON TABLE main.analytics.orders 
TO 'snowflake-sp@company.com';
```

```sql
-- Step 3: In Snowflake, create catalog integration
CREATE CATALOG INTEGRATION databricks_catalog
  CATALOG_SOURCE = ICEBERG_REST
  CATALOG_URI = 'https://workspace.cloud.databricks.com/api/2.1/unity-catalog/iceberg'
  WAREHOUSE = COMPUTE_WH
  ENABLED = TRUE;

-- Step 4: Create external Iceberg table pointing to Unity Catalog
CREATE ICEBERG TABLE orders_from_databricks
CATALOG = databricks_catalog
CATALOG_TABLE_NAME = 'main.analytics.orders';

-- Step 5: Query Databricks data from Snowflake!
SELECT * FROM orders_from_databricks LIMIT 10;
```

## Common Patterns

### Pattern 1: Reverse Direction - Databricks Reads Snowflake

```python
# In Databricks: Query Snowflake tables via Lakehouse Federation

# Create connection
spark.sql("""
CREATE CONNECTION snowflake_conn
TYPE snowflake
OPTIONS (
  host 'account.snowflakecomputing.com',
  user 'databricks_user',
  password secret('scope', 'sf-password'),
  warehouse 'COMPUTE_WH',
  database 'ANALYTICS'
)
""")

# Query Snowflake data
df = spark.sql("""
SELECT customer_id, total_revenue
FROM snowflake_conn.analytics.customer_summary
WHERE total_revenue > 10000
""")

display(df)

# Join Databricks and Snowflake tables
joined = spark.sql("""
SELECT 
  db.order_id,
  db.order_date,
  sf.customer_tier
FROM main.orders db
JOIN snowflake_conn.analytics.customers sf 
  ON db.customer_id = sf.customer_id
""")
```

### Pattern 2: Third-Party Engine Access (Trino)

```python
# Unity Catalog implements Iceberg REST standard
# Any Iceberg-compatible engine can access via REST API

# Example: Trino configuration
catalog_config = {
    "connector.name": "iceberg",
    "iceberg.catalog.type": "rest",
    "iceberg.rest.uri": "https://workspace.cloud.databricks.com/api/2.1/unity-catalog/iceberg",
    "iceberg.rest.auth.type": "bearer",
    "iceberg.rest.auth.token": "dapi..."
}

# Trino can now query Unity Catalog tables
# AND federated Snowflake tables
# All under unified governance!
```

### Pattern 3: Understanding Iceberg Metadata Flow

```python
# When Snowflake queries UC table, it:
# 1. Calls UC Iceberg REST API for metadata
# 2. Receives table metadata (schema, snapshots, file locations)
# 3. Directly reads Parquet files from cloud storage
# 4. Applies Iceberg features (time travel, schema evolution)

# Example metadata response
iceberg_metadata = {
    "format-version": 2,
    "table-uuid": "abc-123",
    "current-schema-id": 0,
    "schemas": [{
        "fields": [
            {"id": 1, "name": "customer_id", "type": "int"},
            {"id": 2, "name": "customer_name", "type": "string"}
        ]
    }],
    "current-snapshot-id": 1985908845652566551,
    "snapshots": [{
        "operation": "append",
        "total-records": "1000000",
        "manifest-list": "s3://bucket/metadata/snap-123.avro"
    }]
}

# This enables:
# - Time travel: SELECT * FROM table VERSION AS OF snapshot_id
# - Schema evolution: ALTER TABLE ADD COLUMN (no rewrites)
# - ACID transactions: Concurrent reads/writes
```

### Pattern 4: Automated Table Sync (Snowflake Private Preview)

```sql
-- Instead of manually creating each table,
-- use catalog-linked databases (private preview)

-- Automatically syncs ALL UC Iceberg tables
CREATE DATABASE databricks_analytics
  FROM CATALOG databricks_catalog
  AUTO_REFRESH = TRUE;

-- All tables immediately available
SHOW TABLES IN databricks_analytics;
-- orders
-- customers
-- products
-- etc.

-- Contact Snowflake account team for access
```

## Reference Files

- [Unity Catalog Iceberg REST](https://docs.databricks.com/external-access/iceberg.html)
- [Snowflake Catalog Integration](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-rest)
- [Lakehouse Federation](https://docs.databricks.com/query-federation/index.html)
- [Delta Uniform (UniForm)](https://docs.databricks.com/delta/uniform.html)

## Common Issues

| Issue | Solution |
|-------|----------|
| **Cannot access external data** | Enable "External data access" in Metastore settings. |
| **Snowflake auth errors** | Verify service principal has correct grants in UC. |
| **Table not found in Snowflake** | Check catalog integration is correct and table exists in UC. |
| **Performance slow from Snowflake** | Snowflake reads Parquet directly. Ensure files are optimized (OPTIMIZE, Liquid Clustering). |
| **Schema mismatch errors** | Ensure Iceberg table schema is compatible with Snowflake data types. |
| **Time travel not working** | Verify Snowflake is reading Iceberg format (not Delta directly). Use UniForm if needed. |

## Advanced Tips

### Delta Uniform for Broader Compatibility

```sql
-- Enable Delta Uniform to expose Delta tables as Iceberg
ALTER TABLE main.analytics.orders
SET TBLPROPERTIES (
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- Now readable by any Iceberg client (including Snowflake)
-- Without data duplication!
```

### Cost Optimization

```python
# Federation eliminates data movement costs:
# ❌ Before: ETL pipeline Databricks → S3 → Snowflake ($$$)
# ✅ After: Snowflake reads Databricks tables directly ($)

# Only pay for:
# - Storage (once, in cloud storage)
# - Compute (when querying)
# - No transfer fees (same region)
```

### Governance Benefits

```sql
-- Single source of truth for access control
-- Grant in Unity Catalog applies to ALL engines

GRANT SELECT ON TABLE main.sensitive.customer_data 
TO data_analysts;

-- Works for:
-- ✅ Databricks SQL
-- ✅ Snowflake (via federation)
-- ✅ Trino (via REST API)
-- ✅ Any Iceberg-compatible engine
```

## FAQ

**Q: Does this require data duplication?**  
A: No! Both platforms read the same Parquet files in cloud storage.

**Q: Can Snowflake write to Unity Catalog tables?**  
A: Write support is in private preview (catalog-linked databases). Currently read-only via external tables.

**Q: What about Delta-specific features?**  
A: Use Delta Uniform to expose Delta tables as Iceberg for compatibility.

**Q: Does this work with streaming tables?**  
A: Yes! Snowflake sees the latest snapshot after each micro-batch commit.

**Q: How do I handle schema evolution?**  
A: Iceberg supports schema evolution. Add columns in either platform, visible to both.

**Q: What's the performance impact?**  
A: Similar to native reads. Ensure tables are optimized (OPTIMIZE, Liquid Clustering).
