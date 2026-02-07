---
name: "databricks-data-archiving"
description: "Complete guide to archiving data in Databricks Lakehouse for cost optimization, including lifecycle policies, Delta table properties, and access patterns."
---

# Databricks Data Archiving: Cost Optimization & Best Practices

## Overview

This skill covers comprehensive data archiving strategies in Databricks Lakehouse to optimize storage costs while maintaining data accessibility. Learn about online vs offline archival tiers, Delta table archival properties, cloud storage lifecycle policies, partitioning strategies, and access control patterns. Includes real-world BI reporting examples and maintenance best practices for managing data lifecycles at scale.

## Quick Start

### Basic Archival Setup
Configure a Delta table for automatic archival:

```sql
-- Create partitioned table for archival
CREATE TABLE sales_data (
    customer_id STRING,
    product_id STRING,
    amount DECIMAL(10,2),
    sale_date DATE,
    ingestion_date DATE
) USING DELTA
PARTITIONED BY (ingestion_date);

-- Set archival retention period
ALTER TABLE sales_data SET TBLPROPERTIES (
    'delta.timeUntilArchived' = '365 days'
);
```

### Cloud Storage Lifecycle Policy
Configure automatic tier transitions (AWS S3 example):

```json
{
    "Rules": [
        {
            "ID": "ArchiveOldData",
            "Status": "Enabled",
            "Prefix": "lakehouse/sales/",
            "Transitions": [
                {
                    "Days": 30,
                    "StorageClass": "STANDARD_IA"
                },
                {
                    "Days": 365,
                    "StorageClass": "GLACIER"
                }
            ]
        }
    ]
}
```

### Access Control with Views
Create views to restrict access to active data:

```sql
-- View for recent data (hot tier)
CREATE VIEW sales_active AS
SELECT * FROM sales_data
WHERE ingestion_date >= CURRENT_DATE() - INTERVAL 30 DAYS;

-- View for quarterly reporting (cool tier)
CREATE VIEW sales_quarterly AS
SELECT * FROM sales_data
WHERE ingestion_date >= CURRENT_DATE() - INTERVAL 365 DAYS;

-- Grant permissions
GRANT SELECT ON VIEW sales_active TO analysts;
GRANT SELECT ON VIEW sales_quarterly TO reporters;
```

## Common Patterns

### Pattern 1: Time-Based Partitioning Strategy
Implement date-based partitioning for efficient archival:

```sql
-- Create table with optimal partitioning
CREATE TABLE events (
    event_id STRING,
    user_id STRING,
    event_type STRING,
    event_data MAP<STRING, STRING>,
    event_timestamp TIMESTAMP,
    ingestion_date DATE
) USING DELTA
PARTITIONED BY (ingestion_date);

-- Insert with computed partition key
INSERT INTO events
SELECT
    event_id,
    user_id,
    event_type,
    event_data,
    event_timestamp,
    DATE(event_timestamp) as ingestion_date
FROM raw_events;

-- Verify partitioning effectiveness
DESCRIBE DETAIL events;
```

### Pattern 2: Multi-Tier Access Strategy
Design views for different access patterns and tiers:

```sql
-- Hot tier: Recent data for real-time analytics
CREATE VIEW events_hot AS
SELECT * FROM events
WHERE ingestion_date >= CURRENT_DATE() - INTERVAL 7 DAYS;

-- Warm tier: Monthly data for operational reports
CREATE VIEW events_warm AS
SELECT * FROM events
WHERE ingestion_date >= CURRENT_DATE() - INTERVAL 90 DAYS;

-- Cold tier: Historical data for compliance (with warning)
CREATE VIEW events_cold AS
SELECT
    *,
    'WARNING: Accessing archived data may incur additional costs' as archival_notice
FROM events
WHERE ingestion_date < CURRENT_DATE() - INTERVAL 90 DAYS;

-- Archive tier: Compliance-only access (restricted)
CREATE VIEW events_archive AS
SELECT * FROM events
WHERE ingestion_date < CURRENT_DATE() - INTERVAL 365 DAYS
AND event_type IN ('compliance_required', 'audit_trail');
```

### Pattern 3: Automated Maintenance Pipeline
Implement regular maintenance to optimize archival:

```python
from pyspark.sql.functions import col, current_date, datediff

def optimize_archival_tables(spark, table_name, hot_window_days=30):
    """
    Automated maintenance for archival tables
    """
    
    # 1. Optimize recent partitions only (avoid resetting archival timers)
    spark.sql(f"""
        OPTIMIZE {table_name}
        WHERE ingestion_date >= current_date() - INTERVAL {hot_window_days} DAYS
        ZORDER BY (ingestion_date, customer_id)
    """)
    
    # 2. Vacuum old data (remove deleted files)
    retention_hours = 24 * 30  # 30 days
    spark.sql(f"""
        VACUUM {table_name} RETAIN {retention_hours} HOURS
    """)
    
    # 3. Analyze table statistics for query optimization
    spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
    
    # 4. Check archival status
    archived_files = spark.sql(f"""
        DESCRIBE DETAIL {table_name}
    """).select("numFiles", "sizeInBytes").collect()
    
    print(f"Table {table_name} maintenance completed")
    print(f"Files: {archived_files[0]['numFiles']}, Size: {archived_files[0]['sizeInBytes']} bytes")

# Usage in Databricks job
optimize_archival_tables(spark, "sales.sales_data", hot_window_days=30)
```

### Pattern 4: Cost Monitoring and Alerts
Monitor archival costs and set up alerts:

```python
def monitor_archival_costs(spark, table_name, cost_threshold=1000):
    """
    Monitor and alert on archival access costs
    """
    
    # Query system access audit logs for archival access
    archival_access = spark.sql(f"""
        SELECT
            event_time,
            user_id,
            service_name,
            action_name,
            request_params,
            response
        FROM system.access.audit
        WHERE event_time >= current_timestamp() - INTERVAL 24 HOURS
        AND service_name = 'databricks'
        AND action_name LIKE '%archive%'
        AND request_params LIKE '%{table_name}%'
    """)
    
    access_count = archival_access.count()
    
    if access_count > 0:
        # Calculate estimated costs (example rates)
        estimated_cost = access_count * 0.01  # $0.01 per archival access
        
        if estimated_cost > cost_threshold:
            alert_team(f"High archival access cost: ${estimated_cost:.2f} for {table_name}")
        
        print(f"Archival access for {table_name}: {access_count} queries, ~${estimated_cost:.2f}")
    
    return access_count, estimated_cost
```

## Reference Files

- [Delta Lake Archival](https://docs.databricks.com/en/optimizations/archive-delta.html) - Official archival documentation
- [Cloud Storage Lifecycle](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html) - AWS S3 lifecycle policies
- [Azure Blob Lifecycle](https://docs.microsoft.com/en-us/azure/storage/blobs/lifecycle-management-overview) - Azure storage lifecycle

## Common Issues

| Issue | Solution |
|-------|----------|
| **Archival delays from DML operations** | Use predicates in OPTIMIZE to avoid rewriting old files |
| **High access costs on archived data** | Create views with date filters to guide users to active data |
| **Query failures on offline archived data** | Restore files to online tier before querying |
| **Partitioning strategy conflicts** | Use ingestion_date for archival, business keys for query optimization |
| **Vacuum removing needed historical data** | Set appropriate retention periods, use shallow vacuum for safety |

## Key Takeaways

1. **Online vs Offline Tiers** - Online archival (cool/cold) for infrequent access, offline (archive) for compliance
2. **Delta Archival Properties** - `delta.timeUntilArchived` informs Databricks of retention periods
3. **Partitioning Strategy** - Date-based partitioning enables efficient archival and maintenance
4. **Access Control Views** - Views with predicates guide users to appropriate data tiers
5. **Maintenance Operations** - Regular OPTIMIZE and VACUUM with predicates preserve archival timing
6. **Cost Monitoring** - Track archival access patterns and set up alerts for cost control

## Archival Tier Comparison

| Tier | AWS S3 | Azure Blob | GCP | Access Time | Use Case |
|------|--------|------------|-----|-------------|----------|
| **Hot** | Standard | Hot | Standard | Milliseconds | Frequently accessed data |
| **Cool** | Standard-IA | Cool | Nearline | Seconds | Monthly accessed data |
| **Cold** | Glacier Instant | Cold | Coldline | 1-5 minutes | Quarterly accessed data |
| **Archive** | Glacier/Deep Archive | Archive | Archive | Hours | Compliance-only data |

## Implementation Checklist

### Planning Phase
- [ ] **Analyze data access patterns** - Determine hot/cool/cold data boundaries
- [ ] **Calculate storage costs** - Compare costs across archival tiers
- [ ] **Define retention policies** - Legal/compliance requirements
- [ ] **Design partitioning strategy** - Balance query performance and archival efficiency

### Implementation Phase
- [ ] **Create partitioned tables** - Use ingestion_date for time-based partitioning
- [ ] **Set Delta properties** - Configure `delta.timeUntilArchived`
- [ ] **Configure cloud lifecycle** - Set up automatic tier transitions
- [ ] **Create access views** - Implement tier-appropriate data access
- [ ] **Set up monitoring** - Track access patterns and costs

### Maintenance Phase
- [ ] **Schedule OPTIMIZE jobs** - Regular compaction with date predicates
- [ ] **Configure VACUUM policies** - Appropriate retention periods
- [ ] **Monitor archival status** - Track file transitions and access costs
- [ ] **Update lifecycle policies** - Adjust based on usage patterns
- [ ] **User education** - Train teams on cost implications

## Cost Optimization Strategies

### Tier Transition Policies
```json
// AWS S3 Lifecycle Configuration
{
    "Rules": [
        {
            "ID": "DataLakeArchival",
            "Status": "Enabled",
            "Prefix": "data-lake/",
            "Transitions": [
                {
                    "Days": 30,
                    "StorageClass": "STANDARD_IA"
                },
                {
                    "Days": 90,
                    "StorageClass": "GLACIER"
                },
                {
                    "Days": 365,
                    "StorageClass": "DEEP_ARCHIVE"
                }
            ],
            "Expiration": {
                "Days": 2555  // 7 years
            }
        }
    ]
}
```

### Query Optimization for Archival Data
```sql
-- Efficient queries on archived data
SELECT
    customer_id,
    SUM(amount) as total_sales,
    COUNT(*) as transaction_count
FROM sales_data
WHERE ingestion_date BETWEEN '2023-01-01' AND '2023-12-31'
  AND customer_id = 'CUST_123'  -- Use partition keys first
GROUP BY customer_id;

-- Avoid full table scans
-- Bad: SELECT * FROM sales_data WHERE year = 2023
-- Good: SELECT * FROM sales_data WHERE ingestion_date >= '2023-01-01'
```

### Automated Archival Pipeline
```python
from databricks.sdk import WorkspaceClient
from datetime import datetime, timedelta

def setup_automated_archival(workspace_client, table_name, archive_after_days=365):
    """
    Set up automated archival workflow
    """
    
    # Create archival job
    job_config = {
        "name": f"{table_name}_archival_maintenance",
        "tasks": [
            {
                "task_key": "optimize_recent",
                "sql_task": {
                    "query": {
                        "query_text": f"""
                            OPTIMIZE {table_name}
                            WHERE ingestion_date >= current_date() - INTERVAL 30 DAYS
                        """
                    }
                }
            },
            {
                "task_key": "vacuum_old",
                "sql_task": {
                    "query": {
                        "query_text": f"""
                            VACUUM {table_name} RETAIN 168 HOURS
                        """
                    }
                }
            },
            {
                "task_key": "check_archival_status",
                "sql_task": {
                    "query": {
                        "query_text": f"""
                            DESCRIBE DETAIL {table_name}
                        """
                    }
                }
            }
        ],
        "schedule": {
            "quartz_cron_expression": "0 0 2 * * ?",  # Daily at 2 AM
            "timezone_id": "UTC"
        }
    }
    
    job = workspace_client.jobs.create(**job_config)
    return job.job_id
```

## When to Use This Skill

- Managing storage costs for large historical datasets
- Implementing data retention and compliance policies
- Optimizing cloud storage expenses in data lakes
- Setting up tiered storage strategies
- Building cost-effective data warehousing solutions
- Planning data lifecycle management

## Performance Considerations

### Query Performance by Tier
- **Hot Tier**: Full performance, standard query latency
- **Cool Tier**: Slight latency increase, same query capabilities
- **Cold Tier**: Moderate latency increase, occasional throttling
- **Archive Tier**: Significant latency, requires restoration

### Optimization Strategies
```sql
-- Use partition pruning for better performance
SELECT * FROM events
WHERE ingestion_date >= '2024-01-01'
  AND ingestion_date < '2024-02-01'
  AND event_type = 'user_login';

-- Leverage ZORDER for archival data
OPTIMIZE events
WHERE ingestion_date < '2023-01-01'
ZORDER BY (user_id, event_type);

-- Use Delta caching for frequently accessed archived data
CACHE SELECT * FROM events_archive
WHERE ingestion_date >= '2023-01-01';
```

## Security and Compliance

### Access Control for Archived Data
```sql
-- Restrict access to archived data
CREATE VIEW sensitive_archived_data AS
SELECT * FROM historical_transactions
WHERE ingestion_date < CURRENT_DATE() - INTERVAL 7 YEARS
    AND user_role IN ('compliance_officer', 'auditor');

-- Audit access to archived data
SELECT
    event_time,
    user_id,
    service_name,
    action_name,
    resources
FROM system.access.audit
WHERE event_time >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
  AND resources LIKE '%archive%'
  AND action_name = 'executeQuery';
```

## Related Skills

- delta-lake-optimization
- cloud-storage-lifecycle
- data-lifecycle-management
- cost-optimization-data-lake
- data-retention-compliance