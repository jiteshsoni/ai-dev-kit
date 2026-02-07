---
name: "salesforce-batch-writes-pyspark"
description: "High-performance batch writes to Salesforce using PySpark Data Source API: custom Salesforce sink, Bulk API v2.0 integration, partition optimization, and parent/child relationship handling."
---

# High-Performance Batch Salesforce Writes with PySpark

## Overview

This skill covers implementing high-performance batch writes to Salesforce using PySpark's Python Data Source API. Learn how to create custom Salesforce sinks, integrate with Salesforce Bulk API v2.0, optimize partition sizes for parallel writes, handle parent/child relationships, and achieve 10x+ performance improvements through parallel job execution.

## Quick Start

### Register Salesforce Data Source
Set up the custom Salesforce writer:

```python
from salesforce_batch_writer import SalesforceBatchDataSource

# Register the data source
spark.dataSource.register(SalesforceBatchDataSource)

print("Salesforce data source registered")
```

### Basic Salesforce Write
Write DataFrame to Salesforce:

```python
# Configure Salesforce credentials (use Databricks secrets)
SF_USERNAME = dbutils.secrets.get(scope="salesforce", key="username")
SF_PASSWORD = dbutils.secrets.get(scope="salesforce", key="password")
SF_TOKEN = dbutils.secrets.get(scope="salesforce", key="token")
SF_INSTANCE_URL = dbutils.secrets.get(scope="salesforce", key="instance_url")

sf_creds = {
    "username": SF_USERNAME,
    "password": SF_PASSWORD,
    "security_token": SF_TOKEN,
    "instance_url": SF_INSTANCE_URL,
}

# Write DataFrame
df_accounts.write \
    .format("salesforce-batch") \
    .mode("append") \
    .options(**sf_creds) \
    .option("sobject", "Account") \
    .option("api_version", "2") \
    .save()
```

## Common Patterns

### Pattern 1: Optimize Partition Count for Performance
Balance throughput with API limits:

```python
def optimize_salesforce_partitions(df, target_partition_size_mb=100):
    """
    Calculate optimal partition count for Salesforce writes.
    
    Args:
        df: DataFrame to write
        target_partition_size_mb: Target partition size in MB (max 150MB for Bulk API v2)
    """
    # Estimate DataFrame size
    row_count = df.count()
    sample_size = min(1000, row_count)
    sample_df = df.limit(sample_size)
    
    # Approximate size per row (rough estimate)
    estimated_size_mb = (df.count() / sample_size) * (
        len(sample_df.toPandas().to_json()) / (1024 * 1024)
    )
    
    # Calculate partitions (ensure < 150MB per partition)
    optimal_partitions = max(1, int(estimated_size_mb / target_partition_size_mb))
    
    # Match to cluster cores for parallelism
    num_cores = spark.sparkContext.defaultParallelism
    optimal_partitions = min(optimal_partitions, num_cores)
    
    print(f"Optimal partitions: {optimal_partitions}")
    print(f"Estimated size per partition: {estimated_size_mb / optimal_partitions:.2f} MB")
    
    return optimal_partitions

# Usage
NUM_PARTITIONS = optimize_salesforce_partitions(df_to_write, target_partition_size_mb=100)
df_repartitioned = df_to_write.repartition(NUM_PARTITIONS)

# Write with optimized partitions
df_repartitioned.write \
    .format("salesforce-batch") \
    .mode("append") \
    .options(**sf_creds) \
    .option("sobject", "Account") \
    .option("api_version", "2") \
    .save()
```

### Pattern 2: Upsert with External ID
Handle updates and inserts:

```python
from pyspark.sql.functions import col

# Prepare DataFrame with external ID
df_accounts_ready = df_accounts.select(
    "AccountName",
    "Industry",
    "AnnualRevenue",
    col("OracleId").alias("Oracle_Id__c")  # External ID field
)

# Upsert using external ID
df_accounts_ready.write \
    .format("salesforce-batch") \
    .mode("append") \
    .options(**sf_creds) \
    .option("sobject", "Account") \
    .option("api_version", "2") \
    .option("operation", "upsert") \
    .option("upsertField", "Oracle_Id__c") \
    .save()
```

### Pattern 3: Parent/Child Relationships
Link related objects:

```python
# Step 1: Upsert parent records (Accounts)
df_accounts.write \
    .format("salesforce-batch") \
    .mode("append") \
    .options(**sf_creds) \
    .option("sobject", "Account") \
    .option("api_version", "2") \
    .option("operation", "upsert") \
    .option("upsertField", "Oracle_Id__c") \
    .save()

# Step 2: Link child records (Contacts) to parent Accounts
df_contacts_ready = df_contacts.select(
    "FirstName",
    "LastName",
    "Email",  # Contact upsert key
    col("AccountExtId").alias("Account.Oracle_Id__c")  # Link to Account
)

df_contacts_ready.write \
    .format("salesforce-batch") \
    .mode("append") \
    .options(**sf_creds) \
    .option("sobject", "Contact") \
    .option("api_version", "2") \
    .option("operation", "upsert") \
    .option("upsertField", "Email") \
    .save()
```

### Pattern 4: Handle Null Values
Control null value behavior:

```python
# Option 1: Ignore nulls (don't overwrite existing values)
df.write \
    .format("salesforce-batch") \
    .mode("append") \
    .options(**sf_creds) \
    .option("sobject", "Account") \
    .option("api_version", "2") \
    .option("ignoreNullValues", "true") \
    .save()

# Option 2: Clear nulls (overwrite with null)
df.write \
    .format("salesforce-batch") \
    .mode("append") \
    .options(**sf_creds) \
    .option("sobject", "Account") \
    .option("api_version", "2") \
    .option("ignoreNullValues", "false") \
    .save()
```

## Reference Files

- [Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html) - Custom sources/sinks
- [Salesforce Bulk API 2.0](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/bulk_api_2_0.htm) - API documentation
- [simple-salesforce](https://github.com/simple-salesforce/simple-salesforce) - Python Salesforce library

## Common Issues

| Issue | Solution |
|-------|----------|
| **Out of memory** | Reduce partition size, ensure partitions < 150MB |
| **API limit exceeded** | Reduce partition count, increase batch size |
| **Schema mismatch** | Ensure DataFrame schema matches Salesforce object schema |
| **Parent/child linking fails** | Use external ID format: "Account.Oracle_Id__c" |
| **Slow performance** | Increase partitions (match cluster cores), use Bulk API v2 |

## Key Takeaways

1. **Bulk API v2.0**: 150MB per job limit (vs 10MB per batch in v1)
2. **Partition Strategy**: Each partition = one Salesforce job (parallel execution)
3. **Performance**: 32 partitions = 31 seconds vs 372 seconds (single job) - 12x faster
4. **Partition Size**: Keep partitions < 150MB to avoid OOM errors
5. **API Limits**: Balance partition count with Salesforce API call limits
6. **Parent/Child**: Use "Parent.ExternalId__c" format for relationships

## Performance Optimization

### Benchmark Partition Counts
```python
def benchmark_salesforce_writes(df, partition_counts=[1, 8, 16, 32]):
    """Benchmark write performance across partition counts"""
    
    results = []
    
    for num_partitions in partition_counts:
        df_repartitioned = df.repartition(num_partitions)
        
        start_time = time.time()
        df_repartitioned.write \
            .format("salesforce-batch") \
            .mode("append") \
            .options(**sf_creds) \
            .option("sobject", "spark_perf_test__b") \
            .option("api_version", "2") \
            .save()
        end_time = time.time()
        
        duration = end_time - start_time
        records_per_second = df.count() / duration
        
        results.append({
            "partitions": num_partitions,
            "duration_seconds": duration,
            "records_per_second": records_per_second
        })
        
        print(f"{num_partitions} partitions: {duration:.2f}s ({records_per_second:.0f} rec/s)")
    
    return results

# Usage
benchmark_results = benchmark_salesforce_writes(df_to_write)
```

### Monitor API Usage
```python
def monitor_salesforce_api_usage():
    """Monitor Salesforce API call usage"""
    
    # Each partition creates one Bulk API job
    # Each job makes multiple API calls (create, upload, check status, etc.)
    
    # Estimate API calls:
    # - 1 job creation call per partition
    # - 1 status check call per partition (minimum)
    # - Additional calls for large jobs
    
    num_partitions = 32
    estimated_calls_per_partition = 3  # Conservative estimate
    
    total_estimated_calls = num_partitions * estimated_calls_per_partition
    
    print(f"Estimated API calls for {num_partitions} partitions: {total_estimated_calls}")
    print("Monitor your Salesforce org's API limits!")
    
    return total_estimated_calls

# Usage
api_calls = monitor_salesforce_api_usage()
```

## Complete Example: Production Workflow

```python
def production_salesforce_sync(df_accounts, df_contacts):
    """Complete production workflow for Salesforce sync"""
    
    # Step 1: Optimize partition count
    account_partitions = optimize_salesforce_partitions(df_accounts)
    contact_partitions = optimize_salesforce_partitions(df_contacts)
    
    # Step 2: Upsert Accounts
    print("Upserting Accounts...")
    df_accounts_ready = df_accounts.select(
        "AccountName",
        "Industry",
        "AnnualRevenue",
        col("OracleId").alias("Oracle_Id__c")
    ).repartition(account_partitions)
    
    df_accounts_ready.write \
        .format("salesforce-batch") \
        .mode("append") \
        .options(**sf_creds) \
        .option("sobject", "Account") \
        .option("api_version", "2") \
        .option("operation", "upsert") \
        .option("upsertField", "Oracle_Id__c") \
        .option("ignoreNullValues", "true") \
        .save()
    
    # Step 3: Upsert Contacts (linked to Accounts)
    print("Upserting Contacts...")
    df_contacts_ready = df_contacts.select(
        "FirstName",
        "LastName",
        "Email",
        col("AccountExtId").alias("Account.Oracle_Id__c")
    ).repartition(contact_partitions)
    
    df_contacts_ready.write \
        .format("salesforce-batch") \
        .mode("append") \
        .options(**sf_creds) \
        .option("sobject", "Contact") \
        .option("api_version", "2") \
        .option("operation", "upsert") \
        .option("upsertField", "Email") \
        .option("ignoreNullValues", "true") \
        .save()
    
    print("Salesforce sync completed!")

# Usage
production_salesforce_sync(df_accounts, df_contacts)
```

## When to Use This Skill

- Writing large datasets to Salesforce
- Syncing data from Databricks to Salesforce
- Handling parent/child relationships
- Optimizing Salesforce write performance
- Managing Salesforce API limits
- Implementing upsert operations

## Related Skills

- pyspark-data-source-api
- salesforce-integration-patterns
- batch-write-optimization
- parent-child-relationships