---
name: "High-Performance Salesforce Writes with PySpark"
description: "Write large DataFrames to Salesforce using custom Python Data Source with Bulk API v2, achieving 10x speedup through Spark parallelization."
author: "Neil Wilson"
url: "https://www.databricksters.com/p/bulking-up-high-performance-batch"
date: "2025-12-16"
tags: ["salesforce", "bulk-api", "python-data-source", "pyspark", "integration", "databricks"]
---

# High-Performance Salesforce Writes with PySpark

## Overview

Write large PySpark DataFrames to Salesforce using custom Python Data Source API and Bulk API v2. Spark automatically parallelizes writes across partitions, submitting multiple Bulk API jobs concurrently. Achieved 10x speedup (31s vs 372s for 300K records) compared to single-batch uploads. Critical: partition size must be <150MB per Salesforce Bulk API v2 limit.

**Use this skill when:** Writing large datasets to Salesforce, syncing data lake to CRM, or building reverse ETL pipelines.

## Quick Start

```python
# Step 1: Register custom Salesforce data source
from salesforce_batch_writer import SalesforceBatchDataSource
spark.dataSource.register(SalesforceBatchDataSource)

# Step 2: Configure credentials (use dbutils.secrets)
sf_creds = {
    "username": dbutils.secrets.get("salesforce", "username"),
    "password": dbutils.secrets.get("salesforce", "password"),
    "security_token": dbutils.secrets.get("salesforce", "token"),
    "instance_url": "https://mycompany.my.salesforce.com"
}

# Step 3: Prepare DataFrame (schema must match Salesforce object)
df_contacts = spark.table("crm.contacts").select(
    "FirstName",
    "LastName",
    "Email",
    "Phone"
)

# Step 4: Repartition for parallelism (< 150MB per partition)
NUM_PARTITIONS = 32  # Match cluster cores
df_repartitioned = df_contacts.repartition(NUM_PARTITIONS)

# Step 5: Write to Salesforce
(df_repartitioned.write
    .format("salesforce-batch")
    .mode("append")
    .options(**sf_creds)
    .option("sobject", "Contact")
    .option("api_version", "2")  # Bulk API v2
    .save()
)

# Result: 32 parallel Bulk API jobs, massive speedup!
```

## Common Patterns

### Pattern 1: Upsert with External ID

```python
# Upsert Contacts using Email as unique key
df_contacts = spark.table("warehouse.contacts").select(
    "FirstName",
    "LastName",
    "Email"  # Upsert key
)

(df_contacts.write
    .format("salesforce-batch")
    .mode("append")
    .options(**sf_creds)
    .option("sobject", "Contact")
    .option("api_version", "2")
    .option("operation", "upsert")
    .option("upsertField", "Email")
    .save()
)

# Creates new contacts or updates existing based on Email
```

### Pattern 2: Parent-Child Relationships

```python
# First: Upsert Accounts (parent)
df_accounts = spark.table("erp.accounts").select(
    "AccountNumber",  # External ID
    "AccountName",
    "Industry"
)

(df_accounts.write
    .format("salesforce-batch")
    .mode("append")
    .options(**sf_creds)
    .option("sobject", "Account")
    .option("api_version", "2")
    .option("operation", "upsert")
    .option("upsertField", "AccountNumber")
    .save()
)

# Second: Link Contacts to Accounts (child)
from pyspark.sql.functions import col

df_contacts_linked = spark.table("erp.contacts").select(
    "FirstName",
    "LastName",
    "Email",
    # Link to parent using External ID
    col("AccountExtId").alias("Account.AccountNumber")
)

(df_contacts_linked.write
    .format("salesforce-batch")
    .mode("append")
    .options(**sf_creds)
    .option("sobject", "Contact")
    .option("api_version", "2")
    .option("operation", "upsert")
    .option("upsertField", "Email")
    .save()
)

# Contacts automatically linked to Accounts via AccountNumber
```

### Pattern 3: Handling Null Values

```python
# Option 1: Ignore nulls (don't overwrite existing values)
(df.write
    .format("salesforce-batch")
    .mode("append")
    .options(**sf_creds)
    .option("sobject", "Contact")
    .option("api_version", "2")
    .option("operation", "upsert")
    .option("upsertField", "Email")
    .option("ignoreNullValues", "true")  # Omit nulls from payload
    .save()
)

# Option 2: Clear nulls (overwrite with NULL in Salesforce)
(df.write
    .format("salesforce-batch")
    .mode("append")
    .options(**sf_creds)
    .option("sobject", "Contact")
    .option("operation", "upsert")
    .option("upsertField", "Email")
    .option("ignoreNullValues", "false")  # Explicitly clear fields
    .save()
)
```

### Pattern 4: Optimal Partitioning Strategy

```python
# Calculate partition size to stay under 150MB limit
def calculate_partitions(df, max_mb_per_partition=140):
    """Calculate optimal partitions for Salesforce Bulk API v2"""
    # Estimate DataFrame size
    sample = df.limit(1000).toPandas()
    bytes_per_row = sample.memory_usage(deep=True).sum() / len(sample)
    total_rows = df.count()
    total_mb = (bytes_per_row * total_rows) / 1024 / 1024
    
    # Calculate partitions (with buffer)
    num_partitions = int(total_mb / max_mb_per_partition) + 1
    
    # Don't exceed cluster cores
    max_cores = sc.defaultParallelism
    num_partitions = min(num_partitions, max_cores)
    
    print(f"Estimated size: {total_mb:.2f} MB")
    print(f"Recommended partitions: {num_partitions}")
    
    return num_partitions

# Use calculation
optimal_partitions = calculate_partitions(df_contacts)
df_optimized = df_contacts.repartition(optimal_partitions)

(df_optimized.write
    .format("salesforce-batch")
    .options(**sf_creds)
    .option("sobject", "Contact")
    .save()
)
```

### Pattern 5: Error Handling and Failed Records

```python
# Monitor write job for failures
from pyspark.sql import Row

# Track job metrics
job_start = time.time()

try:
    (df.write
        .format("salesforce-batch")
        .options(**sf_creds)
        .option("sobject", "Contact")
        .option("api_version", "2")
        .save()
    )
    
    print(f"Write completed in {time.time() - job_start:.2f}s")
    
except Exception as e:
    print(f"Write failed: {e}")
    # Failed records are logged by writer
    # Check Spark UI logs for details

# Failed records CSV available via Salesforce API
# Use simple-salesforce to retrieve:
from simple_salesforce import Salesforce

sf = Salesforce(
    username=username,
    password=password,
    security_token=token
)

# Get failed records for a job
failed_csv = sf.bulk2.Contact.get_failed_records(job_id)
print(failed_csv[:1000])  # Preview failures
```

## Reference Files

- [Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html)
- [Salesforce Bulk API v2](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/)
- [simple-salesforce Library](https://github.com/simple-salesforce/simple-salesforce)
- [Example Implementation](https://github.com/neil-wilson-data/python-data-sources/blob/main/salesforce/salesforce_batch_writer.py)

## Common Issues

| Issue | Solution |
|-------|----------|
| **Partition > 150MB error** | Increase NUM_PARTITIONS to create smaller chunks. |
| **Schema mismatch** | DataFrame columns must exactly match Salesforce object fields. |
| **API limit exceeded** | Too many partitions = too many API calls. Balance parallelism with limits. |
| **Parent record not found** | Ensure parent objects (Accounts) are upserted before children (Contacts). |
| **OOM on executors** | Partition too large. Each partition materialized in memory during write. |
| **Authentication errors** | Verify credentials. Use `instance_url` or correct `domain` (test/login). |

## Advanced Tips

### Performance Benchmarks

```python
# Single batch (no parallelism): 372 seconds for 300K records
# 32 partitions (parallel): 31 seconds for 300K records
# Speedup: 12x

# Factors affecting performance:
# - Number of partitions (parallelism)
# - Salesforce API rate limits
# - Network latency to Salesforce
# - Record size and complexity
```

### API Call Management

```python
# Each partition = 1 Salesforce Bulk API job
# Each job uses multiple API calls:
# - Create job: 1 call
# - Upload data: 1 call  
# - Check status: N calls (polling)
# - Get results: 1 call

# Example: 32 partitions ≈ 128-256 API calls per run

# Balance parallelism with API limits:
# - Daily API limit: Check Salesforce org limits
# - Concurrent API limit: Typically 25 concurrent jobs
# - Recommended: 8-32 partitions for most use cases
```

### Sandbox vs Production

```python
# Sandbox configuration
sf_creds_sandbox = {
    "username": "user@company.com.sandbox",
    "password": "password",
    "security_token": "token",
    "domain": "test"  # Use test.salesforce.com
}

# Production configuration
sf_creds_prod = {
    "username": "user@company.com",
    "password": "password",
    "security_token": "token",
    "domain": "login"  # Use login.salesforce.com (default)
}

# Or use instance_url for explicit control
sf_creds_explicit = {
    "username": "user@company.com",
    "password": "password",
    "security_token": "token",
    "instance_url": "https://mycompany.my.salesforce.com"
}
```

## FAQ

**Q: What's the maximum partition size?**  
A: 150MB per partition (Bulk API v2 limit). Stay under 140MB for safety.

**Q: How many partitions should I use?**  
A: Match cluster cores (e.g., 32 cores = 32 partitions) while staying under API limits.

**Q: Can I write to custom objects?**  
A: Yes! Use API name: `custom_object__c`. Schema must match custom fields.

**Q: Does this work with Bulk API v1?**  
A: Yes, set `api_version = "1"`. V2 recommended for 150MB limit vs v1's 10MB per batch.

**Q: How do I handle duplicate detection?**  
A: Use upsert with external ID. Salesforce handles duplicates based on upsert key.

**Q: What about relationship fields?**  
A: Use dot notation: `Account.ExternalId__c` to link via parent's external ID.
