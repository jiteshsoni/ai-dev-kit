---
name: ethereum-etl-pipeline
description: Build a complete Ethereum blockchain ETL pipeline using Databricks Free Edition, Auto Loader, and Delta Lake. Use when ingesting blockchain data, processing Ethereum blocks and transactions, or building analytics pipelines on public blockchain datasets.
---

# Ethereum ETL Pipeline

## Overview

Build a production-ready Ethereum blockchain ETL pipeline using Databricks Free Edition (no cost), Auto Loader for streaming ingestion, and Delta Lake for queryable storage. This pipeline ingests raw Ethereum blocks, extracts transaction data, and stores everything in Delta tables for analytics.

## Quick Start

### Setup Unity Catalog

```python
# Create catalog, schema, and volumes
CATALOG = "blockchain"
SCHEMA = "ethereum"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.ethereum")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.ethereum_checkpoints")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.ethereum_schemas")

DATA_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/ethereum"
CHECKPOINT_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/ethereum_checkpoints"
SCHEMA_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/ethereum_schemas"
```

### Download Historical Data from AWS

```python
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import os

# Configure anonymous S3 client (no credentials needed)
s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

AWS_BUCKET = "aws-public-blockchain"
S3_PREFIX = "v1.0/eth/blocks/"

# List and download Parquet files
keys = []
for obj in s3.list_objects_v2(Bucket=AWS_BUCKET, Prefix=S3_PREFIX).get("Contents", []):
    if obj["Key"].endswith(".parquet"):
        keys.append(obj["Key"])

# Download to Unity Catalog volume
for key in keys[:20]:  # Download first 20 files
    dest_path = os.path.join(DATA_VOLUME, key.replace("v1.0/eth/", ""))
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    s3.download_file(AWS_BUCKET, key, dest_path)
```

### Stream with Auto Loader

```python
# Stream Ethereum blocks using Auto Loader
reader = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", SCHEMA_VOLUME)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.schemaHints", "number BIGINT, baseFeePerGas BIGINT")
    .load(f"{DATA_VOLUME}/blocks/")
)

# Write to Delta table
blocks_query = (reader
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_VOLUME}/blocks/")
    .trigger(availableNow=True)
    .table(f"{CATALOG}.{SCHEMA}.blocks")
    .start()
)
```

## Common Patterns

### Pattern 1: Extract Transactions from Blocks

```python
from pyspark.sql.functions import explode, col

# Blocks contain nested transaction arrays
# Explode to create one row per transaction
transactions_df = (blocks_df
    .select(
        col("number").alias("block_number"),
        col("timestamp").alias("block_timestamp"),
        col("hash").alias("block_hash"),
        explode(col("transactions")).alias("transaction")
    )
    .select(
        "block_number",
        "block_timestamp",
        "block_hash",
        col("transaction.hash").alias("tx_hash"),
        col("transaction.from").alias("from_address"),
        col("transaction.to").alias("to_address"),
        col("transaction.value").alias("value"),
        col("transaction.gas").alias("gas"),
        col("transaction.gasPrice").alias("gas_price")
    )
)

# Write transactions to separate Delta table
transactions_query = (transactions_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_VOLUME}/transactions/")
    .trigger(availableNow=True)
    .table(f"{CATALOG}.{SCHEMA}.transactions")
    .start()
)
```

### Pattern 2: Schema Evolution with Auto Loader

```python
# Auto Loader handles schema changes automatically
reader = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", SCHEMA_VOLUME)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Add new columns
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")  # Capture incompatible data
    .load(f"{DATA_VOLUME}/blocks/")
)

# New fields in blocks automatically added to schema
# Incompatible data saved to _rescued_data column
```

### Pattern 3: Trigger Types for Blockchain Data

```python
# availableNow: Process all available data, then stop
# Best for scheduled ingestion (e.g., every hour)
.trigger(availableNow=True)

# processingTime: Continuous processing
# Best for real-time blockchain monitoring
.trigger(processingTime="30 seconds")

# For blockchain: availableNow is usually preferred
# - Blocks arrive in batches
# - Cost-effective (cluster stops after processing)
# - Same correctness guarantees
```

## Reference Files

### AWS Public Blockchain Dataset

- **Bucket**: `aws-public-blockchain`
- **Ethereum Path**: `v1.0/eth/blocks/`
- **Format**: Parquet (compressed, partitioned by date)
- **Access**: Anonymous (no credentials needed)
- **Cost**: Free

### Auto Loader Benefits

- **Automatic file discovery**: Watches folders continuously
- **Schema evolution**: Handles schema changes automatically
- **Exactly-once processing**: Checkpoint-based guarantees
- **Schema hints**: Type hints for specific columns
- **Rescued data**: Captures incompatible data

### Delta Lake Advantages

- **ACID transactions**: Consistent reads/writes
- **Time travel**: Query historical versions
- **Schema enforcement**: Prevents bad data
- **Optimize**: Bin-packing and Z-ordering
- **SQL queries**: Standard SQL on blockchain data

## Common Issues

| Issue | Solution |
|-------|----------|
| **S3 access denied** | Use anonymous client (UNSIGNED config) |
| **Schema evolution errors** | Enable `schemaEvolutionMode` and `rescuedDataColumn` |
| **Large nested structures** | Use `explode()` to flatten transactions array |
| **Checkpoint conflicts** | Use unique checkpoint per stream |
| **Slow ingestion** | Use `availableNow` trigger; adjust `maxFilesPerTrigger` |

## Advanced Tips

### Optimize Delta Tables

```python
# After initial ingestion, optimize tables
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.blocks")
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.transactions")

# Z-order on frequently queried columns
spark.sql(f"""
    OPTIMIZE {CATALOG}.{SCHEMA}.transactions
    ZORDER BY (block_number, from_address)
""")
```

### Query Blockchain Data

```python
# Find top senders by transaction count
spark.sql(f"""
    SELECT 
        from_address,
        COUNT(*) as tx_count,
        SUM(value) as total_value
    FROM {CATALOG}.{SCHEMA}.transactions
    GROUP BY from_address
    ORDER BY tx_count DESC
    LIMIT 10
""").show()

# Find blocks with most transactions
spark.sql(f"""
    SELECT 
        block_number,
        block_timestamp,
        COUNT(*) as tx_count
    FROM {CATALOG}.{SCHEMA}.transactions
    GROUP BY block_number, block_timestamp
    ORDER BY tx_count DESC
    LIMIT 10
""").show()
```

### Production Considerations

```python
# Use notification mode for Auto Loader (reduces listing costs)
reader = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.useNotifications", "true")
    .option("cloudFiles.notificationLocation", f"{DATA_VOLUME}/_notifications")
    .option("cloudFiles.format", "parquet")
    .load(f"{DATA_VOLUME}/blocks/")
)

# Partition Delta tables by date for efficient queries
transactions_df.writeStream \
    .format("delta") \
    .partitionBy("date") \
    .option("checkpointLocation", checkpoint_path) \
    .start(f"{CATALOG}.{SCHEMA}.transactions")
```

## FAQ

**Q: Is Databricks Free Edition sufficient for production?**
A: Free Edition is great for learning and small datasets. For production, consider paid tiers for larger scale.

**Q: How do I get real-time Ethereum data?**
A: Use web3.py to poll Ethereum nodes and save blocks as Parquet files. Auto Loader will pick them up automatically.

**Q: Can I use this for other blockchains?**
A: Yes. AWS provides Bitcoin and other blockchain datasets. Adapt the schema hints and extraction logic.

**Q: How much does this cost?**
A: Databricks Free Edition is free. AWS S3 public dataset is free. Only storage costs apply (minimal for small datasets).

**Q: What if schema changes in Ethereum blocks?**
A: Auto Loader's `schemaEvolutionMode` handles this automatically. New columns added, incompatible data saved to `_rescued_data`.
