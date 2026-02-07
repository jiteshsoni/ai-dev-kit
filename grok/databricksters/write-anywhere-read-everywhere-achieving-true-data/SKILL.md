---
name: "databricks-snowflake-federation"
description: "Complete guide to bidirectional data federation between Databricks and Snowflake using Unity Catalog and Apache Iceberg for write-anywhere, read-everywhere architectures."
---

# Databricks-Snowflake Federation: Write Anywhere, Read Everywhere

## Overview

This skill provides a complete implementation guide for establishing bidirectional data federation between Databricks and Snowflake using Unity Catalog and Apache Iceberg. Learn how to eliminate data silos, enable cross-platform queries, and maintain unified governance while allowing writes from any platform and reads from everywhere. Includes step-by-step setup, security configuration, and advanced Iceberg capabilities.

## Quick Start

### Enable Unity Catalog External Access
First, enable external access in your Databricks metastore:

```sql
-- In Databricks workspace: Catalog → Metastore → Details → Enable "External data access"
-- This allows external engines to access data through Unity Catalog REST APIs
```

### Basic Federation Setup
Create a service principal and grant access:

```sql
-- Create service principal for Snowflake access
-- Grant schema-level access
GRANT EXTERNAL USE SCHEMA ON SCHEMA catalog_name.schema_name
TO 'service-principal@domain.com';

-- Or grant table-level access
GRANT SELECT ON TABLE catalog_name.schema_name.table_name
TO 'service-principal@domain.com';
```

### Query Across Platforms
Once federation is configured:

```sql
-- Query Databricks tables from Snowflake
SELECT customer_id, order_total, order_date
FROM databricks_orders
WHERE order_date >= '2024-01-01'
LIMIT 100;

-- Query Snowflake tables from Databricks
SELECT *
FROM snowflake_conn.database.schema.customer_summary
WHERE total_purchases > 1000;
```

## Common Patterns

### Pattern 1: Lakehouse Federation (Databricks → Snowflake)
Register Snowflake tables in Unity Catalog:

```sql
-- Create external connection to Snowflake
CREATE CONNECTION snowflake_horizon
TYPE snowflake
OPTIONS (
  'host' = 'account.snowflakecomputing.com',
  'user' = 'service_user',
  'password' = secret('scope', 'snowflake-password'),
  'warehouse' = 'COMPUTE_WH',
  'database' = 'PROD_DB'
);

-- Create federated schema
CREATE SCHEMA catalog.federated_snowflake
FROM CONNECTION snowflake_horizon;

-- Access Snowflake tables as if they were Databricks tables
SELECT * FROM catalog.federated_snowflake.customer_data
WHERE region = 'US-WEST';
```

### Pattern 2: Catalog Integration (Snowflake → Databricks)
Connect Snowflake to Unity Catalog:

```sql
-- In Snowflake: Create catalog integration
CREATE CATALOG INTEGRATION databricks_uc_integration
CATALOG_SOURCE = UNITY
TABLE_FORMAT = ICEBERG
UNITY_CATALOG_URI = 'https://your-workspace.cloud.databricks.com/api/2.1/unity-catalog/iceberg'
OAUTH_CLIENT_ID = 'your-service-principal-client-id'
OAUTH_CLIENT_SECRET = secret('scope', 'databricks-client-secret')
ENABLED = TRUE;

-- Create database from integration
CREATE DATABASE databricks_db
FROM SHARE databricks_uc_integration;

-- Create external table
CREATE ICEBERG TABLE orders_from_databricks
CATALOG = databricks_uc_integration
CATALOG_TABLE_NAME = 'catalog.schema.orders';
```

### Pattern 3: Bidirectional Time Travel Queries
Leverage Iceberg time travel across platforms:

```sql
-- Query historical data from Databricks (in Snowflake)
SELECT customer_id, SUM(amount) as total_spent
FROM orders_from_databricks
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 00:00:00'::timestamp
GROUP BY customer_id;

-- Query historical data from Snowflake (in Databricks)
SELECT *
FROM snowflake_conn.prod_db.customer_snapshot
VERSION AS OF 123456789;  -- Snapshot ID
```

### Pattern 4: Schema Evolution Handling
Manage schema changes across federated tables:

```sql
-- Add column in Databricks
ALTER TABLE catalog.schema.customer_data
ADD COLUMN phone_number STRING;

-- Schema change automatically reflected in Snowflake
-- No manual sync required
SELECT customer_id, name, phone_number
FROM customer_data_from_databricks;

-- Safe schema evolution with Iceberg
ALTER TABLE catalog.schema.orders
ADD COLUMN discount_applied BOOLEAN DEFAULT false;
```

## Reference Files

- [Unity Catalog External Access](https://docs.databricks.com/aws/en/external-access/unity-rest) - Enable external engine access
- [Snowflake Catalog Integration](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-rest) - Connect to Unity Catalog
- [Lakehouse Federation](https://docs.databricks.com/aws/en/query-federation) - Register external catalogs
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/) - Table format specifications
- [Catalog-Linked Databases](https://docs.snowflake.com/en/LIMITEDACCESS/iceberg/tables-iceberg-catalog-linked-database) - Automatic table syncing

## Common Issues

| Issue | Solution |
|-------|----------|
| **External data access disabled** | Enable in metastore settings: Catalog → Metastore → Details → External data access |
| **Permission denied on schema/table** | Grant EXTERNAL USE SCHEMA or SELECT privileges to service principal |
| **Catalog integration fails** | Verify OAuth credentials and Unity Catalog REST endpoint URL |
| **Time travel queries fail** | Use correct syntax: `FOR SYSTEM_TIME AS OF` (Snowflake) or `VERSION AS OF` (Databricks) |
| **Schema changes not reflected** | Iceberg handles this automatically; wait for metadata refresh or check connection |
| **Performance issues with federation** | Consider materializing frequently accessed data or using Delta Sharing for read-heavy workloads |

## Key Takeaways

1. **Apache Iceberg Foundation** - Open table format enables cross-platform interoperability through standardized metadata
2. **Unity Catalog as Bridge** - Implements Iceberg REST Catalog standard for unified governance
3. **Bidirectional Federation** - Lakehouse Federation + Catalog Integration = write anywhere, read everywhere
4. **Time Travel & Schema Evolution** - Rich Iceberg features work seamlessly across platforms
5. **Security First** - Service principals and proper access controls maintain governance
6. **Future-Proof Architecture** - Supports adding new engines (Trino, etc.) without major changes

## When to Use This Skill

- Setting up cross-platform data architectures
- Migrating from data silos to unified access patterns
- Implementing real-time analytics across multiple platforms
- Building enterprise data meshes with unified governance
- Evaluating Delta Sharing vs Federation trade-offs

## Implementation Checklist

### Prerequisites
- [ ] Databricks Unity Catalog enabled
- [ ] Snowflake account with appropriate privileges
- [ ] Cloud storage access configured for both platforms
- [ ] Service principal created with necessary permissions

### Databricks Setup
- [ ] Enable external data access on metastore
- [ ] Grant EXTERNAL USE SCHEMA privileges
- [ ] Create Snowflake connection (for reverse queries)
- [ ] Test Unity Catalog REST API access

### Snowflake Setup
- [ ] Create catalog integration to Unity Catalog
- [ ] Configure OAuth authentication
- [ ] Create database from catalog integration
- [ ] Create external Iceberg tables
- [ ] Test cross-platform queries

### Validation
- [ ] Verify bidirectional read access
- [ ] Test time travel queries
- [ ] Validate schema evolution
- [ ] Performance benchmark queries
- [ ] Confirm security policies apply

## Related Skills

- unity-catalog-governance
- apache-iceberg-table-management
- databricks-lakehouse-federation
- snowflake-external-tables
- cross-platform-data-architecture