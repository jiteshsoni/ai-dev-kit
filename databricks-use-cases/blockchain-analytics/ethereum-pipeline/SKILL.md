---
name: ethereum-analytics-pipeline
description: Build a complete blockchain analytics pipeline on Databricks using free-tier resources. Covers streaming ingestion from public datasets, data transformation with Medallion Architecture, and analytics on Ethereum blockchain data.
author: Canadian Data Guy
type: use-case-collection
source_blogs:
  - title: "Build an Ethereum ETL Pipeline for Free Using Databricks Free Edition"
    url: https://www.canadiandataguy.com/p/build-an-ethereum-etl-pipeline-for
    author: Canadian Data Guy
  - title: "Unlocking Sub-Second Latency with Databricks"
    url: https://www.canadiandataguy.com/p/unlocking-sub-second-latency-with
    author: Canadian Data Guy
---

# Ethereum Blockchain Analytics Pipeline

## Overview

This use case demonstrates how to build a complete blockchain analytics pipeline on Databricks using entirely free resources. By leveraging AWS's public blockchain datasets and Databricks Free Edition, you can analyze Ethereum data without any infrastructure costs.

**Business Value:**
- Free access to complete Ethereum blockchain history
- Real-time analytics on token transfers and DeFi activity
- Fraud detection and anomaly identification
- NFT market analysis and tracking
- Gas fee optimization insights

**Architecture Overview:**

```
┌─────────────────────────────────────────────────────────────────┐
│                    Ethereum Analytics Pipeline                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │   Bronze     │───▶│    Silver    │───▶│    Gold      │      │
│  │ (Raw Blocks) │    │(Transactions)│    │ (Analytics)  │      │
│  └──────────────┘    └──────────────┘    └──────────────┘      │
│         ▲                                                    │
│         │                                                      │
│  ┌──────────────┐                                              │
│  │ AWS Public   │                                              │
│  │ S3 Bucket    │                                              │
│  └──────────────┘                                              │
│                                                                  │
│  Data Flow:                                                      │
│  S3 → Autoloader → Bronze Delta → Transform → Silver → Gold     │
└─────────────────────────────────────────────────────────────────┘
```

## Prerequisites

- [Databricks Free Edition](https://www.databricks.com/learn/free-edition) account
- AWS public dataset access (no credentials required)
- Basic familiarity with PySpark and SQL

## Step 1: Setup Unity Catalog Objects

Create the catalog, schema, and volumes for data organization:

```python
# === CONFIGURATION ===
dbutils.widgets.text("catalog_name", "blockchain", "Catalog Name")
dbutils.widgets.text("schema_name", "ethereum", "Schema Name")
dbutils.widgets.text("num_files", "20", "Number of Files to Download")

CATALOG = dbutils.widgets.get("catalog_name")
SCHEMA = dbutils.widgets.get("schema_name")
NUM_FILES = int(dbutils.widgets.get("num_files"))

# AWS Public Blockchain Dataset
AWS_BUCKET = "aws-public-blockchain"
S3_PREFIX = "v1.0/eth/blocks/"

# Volume paths
DATA_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/ethereum"
CHECKPOINT_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/ethereum_checkpoints"
SCHEMA_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/ethereum_schemas"

print(f"🔧 Configuration: {CATALOG}.{SCHEMA}")
print(f"📦 Processing {NUM_FILES} files from s3://{AWS_BUCKET}/{S3_PREFIX}")
```

```sql
-- Create Unity Catalog objects
CREATE CATALOG IF NOT EXISTS ${catalog_name};
CREATE SCHEMA IF NOT EXISTS ${catalog_name}.${schema_name};

-- Create volumes for data organization
CREATE VOLUME IF NOT EXISTS ${catalog_name}.${schema_name}.ethereum;
CREATE VOLUME IF NOT EXISTS ${catalog_name}.${schema_name}.ethereum_checkpoints;
CREATE VOLUME IF NOT EXISTS ${catalog_name}.${schema_name}.ethereum_schemas;
```

## Step 2: Download Sample Data

Download historical Ethereum block data from AWS's public dataset:

```python
import os
import boto3
from botocore import UNSIGNED
from botocore.client import Config

# Configure anonymous S3 client (no credentials needed!)
s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

print(f"📥 Downloading to: {DATA_VOLUME}")
os.makedirs(DATA_VOLUME, exist_ok=True)

# List parquet files from S3
keys = []
token = None

while len(keys) < NUM_FILES:
    params = {
        "Bucket": AWS_BUCKET,
        "Prefix": S3_PREFIX,
        "MaxKeys": min(1000, NUM_FILES - len(keys))
    }
    if token:
        params["ContinuationToken"] = token
    
    resp = s3.list_objects_v2(**params)
    
    for obj in resp.get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            keys.append(obj["Key"])
            if len(keys) >= NUM_FILES:
                break
    
    if not resp.get("IsTruncated"):
        break
    token = resp.get("NextContinuationToken")

print(f"Found {len(keys)} parquet files")

# Download files
for i, key in enumerate(keys, 1):
    rel_path = key.replace("v1.0/eth/", "")
    dest_path = os.path.join(DATA_VOLUME, rel_path)
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    
    print(f"[{i}/{len(keys)}] Downloading {os.path.basename(key)}...", end=" ")
    s3.download_file(AWS_BUCKET, key, dest_path)
    print("✓")

print("✅ Download complete!")
```

## Step 3: Bronze Layer - Raw Ingestion

Use Autoloader for streaming ingestion of block data:

```python
from pyspark.sql import functions as F

# Schema hints for type coercion
schema_hints = "number BIGINT, baseFeePerGas BIGINT, gasLimit BIGINT, gasUsed BIGINT"

# Streaming reader with Autoloader
bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", SCHEMA_VOLUME)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.schemaHints", schema_hints)
    .load(f"dbfs:{DATA_VOLUME}/blocks/")
)

# Bronze table schema includes metadata
bronze_df = bronze_stream.select(
    "*",
    F.col("_metadata.file_path").alias("source_file"),
    F.col("_metadata.file_modification_time").alias("ingestion_time")
)

# Write to Bronze Delta table
bronze_query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_VOLUME}/bronze_blocks/")
    .trigger(availableNow=True)  # Process all available data
    .table(f"{CATALOG}.{SCHEMA}.bronze_blocks")
)

bronze_query.awaitTermination()
print("✅ Bronze layer loaded")
```

### Bronze Table Structure

| Column | Type | Description |
|--------|------|-------------|
| `hash` | STRING | Block hash |
| `number` | BIGINT | Block number |
| `timestamp` | TIMESTAMP | Block timestamp |
| `miner` | STRING | Miner address |
| `gasLimit` | BIGINT | Gas limit |
| `gasUsed` | BIGINT | Gas used |
| `baseFeePerGas` | BIGINT | Base fee per gas (EIP-1559) |
| `transactions` | ARRAY | Raw transaction list |
| `source_file` | STRING | Source file path |
| `ingestion_time` | TIMESTAMP | Ingestion timestamp |

## Step 4: Silver Layer - Transaction Extraction

Transform raw blocks into clean transaction records:

```python
# Read from Bronze
bronze_df = spark.table(f"{CATALOG}.{SCHEMA}.bronze_blocks")

# Extract and flatten transactions
transactions_df = bronze_df.select(
    F.col("number").alias("block_number"),
    F.col("hash").alias("block_hash"),
    F.col("timestamp").alias("block_timestamp"),
    F.col("miner").alias("block_miner"),
    F.col("baseFeePerGas").alias("base_fee_per_gas"),
    F.explode("transactions").alias("tx")
).select(
    "block_number",
    "block_hash",
    "block_timestamp",
    "block_miner",
    "base_fee_per_gas",
    F.col("tx.hash").alias("tx_hash"),
    F.col("tx.from").alias("from_address"),
    F.col("tx.to").alias("to_address"),
    F.col("tx.value").alias("value"),
    F.col("tx.gas").alias("gas_limit"),
    F.col("tx.gasPrice").alias("gas_price"),
    F.col("tx.nonce").alias("nonce"),
    F.col("tx.input").alias("input_data")
).withColumn(
    "value_eth",
    F.col("value").cast("decimal(38,0)") / 1e18  # Convert Wei to ETH
).withColumn(
    "tx_fee_eth",
    (F.col("gas_limit") * F.col("gas_price")).cast("decimal(38,0)") / 1e18
)

# Create Silver table
transactions_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_transactions")

print(f"✅ Silver layer created: {transactions_df.count():,} transactions")
```

### Silver Table Structure

| Column | Type | Description |
|--------|------|-------------|
| `block_number` | BIGINT | Block number |
| `tx_hash` | STRING | Transaction hash |
| `from_address` | STRING | Sender address |
| `to_address` | STRING | Recipient address |
| `value_eth` | DECIMAL | Transaction value in ETH |
| `gas_limit` | BIGINT | Gas limit |
| `gas_price` | BIGINT | Gas price in Wei |
| `tx_fee_eth` | DECIMAL | Transaction fee in ETH |
| `block_timestamp` | TIMESTAMP | Block timestamp |

## Step 5: Gold Layer - Analytics Models

Create curated datasets for common analytics use cases:

### 5.1 Daily Transaction Summary

```python
# Daily aggregated metrics
daily_metrics = spark.table(f"{CATALOG}.{SCHEMA}.silver_transactions").groupBy(
    F.date_trunc("day", "block_timestamp").alias("date")
).agg(
    F.count("*").alias("transaction_count"),
    F.countDistinct("from_address").alias("unique_senders"),
    F.countDistinct("to_address").alias("unique_recipients"),
    F.sum("value_eth").alias("total_eth_transferred"),
    F.avg("value_eth").alias("avg_transaction_value"),
    F.sum("tx_fee_eth").alias("total_fees_eth"),
    F.avg("tx_fee_eth").alias("avg_fee_eth"),
    F.avg("gas_price").alias("avg_gas_price")
).orderBy("date")

daily_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_daily_metrics")

print("✅ Daily metrics created")
```

### 5.2 Top Wallets by Activity

```python
# Wallet activity analysis
wallet_stats = spark.table(f"{CATALOG}.{SCHEMA}.silver_transactions").groupBy(
    "from_address"
).agg(
    F.count("*").alias("tx_count"),
    F.sum("value_eth").alias("total_sent_eth"),
    F.avg("value_eth").alias("avg_sent_eth"),
    F.min("block_timestamp").alias("first_seen"),
    F.max("block_timestamp").alias("last_seen")
).withColumn(
    "activity_days",
    F.datediff("last_seen", "first_seen") + 1
).orderBy(F.desc("tx_count"))

wallet_stats.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_wallet_stats")

print("✅ Wallet stats created")
```

### 5.3 Gas Price Analysis

```python
# Gas price trends by hour
gas_trends = spark.table(f"{CATALOG}.{SCHEMA}.silver_transactions").groupBy(
    F.date_trunc("hour", "block_timestamp").alias("hour")
).agg(
    F.avg("gas_price").alias("avg_gas_price"),
    F.percentile_approx("gas_price", 0.5).alias("median_gas_price"),
    F.percentile_approx("gas_price", 0.95).alias("p95_gas_price"),
    F.min("gas_price").alias("min_gas_price"),
    F.max("gas_price").alias("max_gas_price"),
    F.count("*").alias("tx_count")
).orderBy("hour")

gas_trends.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_gas_trends")

print("✅ Gas trends created")
```

## Step 6: Streaming Ingestion (Production)

For production, set up continuous streaming ingestion:

```python
# Continuous streaming from new files
streaming_bronze = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", SCHEMA_VOLUME)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(f"dbfs:{DATA_VOLUME}/blocks/")
)

# Optimized for streaming
optimized_query = (
    streaming_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_VOLUME}/streaming_blocks/")
    .option("maxFilesPerTrigger", 100)
    .option("maxBytesPerTrigger", "1g")
    .trigger(processingTime="5 minutes")  # Check every 5 minutes
    .table(f"{CATALOG}.{SCHEMA}.bronze_blocks_streaming")
)

# Start streaming (in production, this runs continuously)
# optimized_query.awaitTermination()
```

## Step 7: Analytics Queries

### Query 1: Daily Transaction Volume

```sql
SELECT 
    date,
    transaction_count,
    total_eth_transferred,
    total_fees_eth,
    avg_gas_price / 1e9 as avg_gas_price_gwei
FROM ${catalog_name}.${schema_name}.gold_daily_metrics
ORDER BY date DESC
LIMIT 30;
```

### Query 2: Top Gas Spenders

```sql
SELECT 
    from_address,
    tx_count,
    total_sent_eth,
    activity_days,
    tx_count / activity_days as avg_tx_per_day
FROM ${catalog_name}.${schema_name}.gold_wallet_stats
WHERE tx_count > 100
ORDER BY total_sent_eth DESC
LIMIT 20;
```

### Query 3: Gas Price Heatmap by Hour

```sql
SELECT 
    DATE(hour) as date,
    HOUR(hour) as hour_of_day,
    avg_gas_price / 1e9 as avg_gas_gwei,
    tx_count
FROM ${catalog_name}.${schema_name}.gold_gas_trends
WHERE hour >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY date DESC, hour_of_day;
```

### Query 4: Smart Contract Interactions

```sql
-- Identify potential smart contracts (addresses with many incoming txs)
SELECT 
    to_address,
    COUNT(*) as incoming_tx_count,
    COUNT(DISTINCT from_address) as unique_senders,
    AVG(value_eth) as avg_value_received,
    SUM(value_eth) as total_value_received
FROM ${catalog_name}.${schema_name}.silver_transactions
WHERE to_address IS NOT NULL
GROUP BY to_address
HAVING COUNT(*) > 50
ORDER BY incoming_tx_count DESC
LIMIT 20;
```

## Step 8: Visualization

Create dashboards using Databricks SQL:

```sql
-- Create a view for easy dashboarding
CREATE OR REPLACE VIEW ${catalog_name}.${schema_name}.v_transaction_summary AS
SELECT 
    date,
    transaction_count,
    unique_senders,
    unique_recipients,
    total_eth_transferred,
    total_fees_eth,
    avg_gas_price / 1e9 as avg_gas_price_gwei
FROM ${catalog_name}.${schema_name}.gold_daily_metrics;
```

## Best Practices

### Cost Optimization (Free Tier)

1. **Use `trigger(availableNow=True)`** for batch-style processing
2. **Limit file counts** during development
3. **Auto-shutdown clusters** - configure 10-minute auto-terminate
4. **Use serverless compute** when available

### Data Quality

```python
# Add data quality checks
def validate_transactions(df):
    """Validate transaction data."""
    validation_df = df.withColumn(
        "validation_errors",
        F.filter(
            F.array(
                F.when(F.col("gas_used") > F.col("gas_limit"), "GAS_EXCEEDED"),
                F.when(F.col("value") < 0, "NEGATIVE_VALUE"),
                F.when(F.col("to_address").isNull() & F.col("input_data").isNull(), "INVALID_CONTRACT")
            ),
            lambda x: x.isNotNull()
        )
    ).withColumn(
        "is_valid",
        F.size(F.col("validation_errors")) == 0
    )
    return validation_df
```

### Schema Evolution

```python
# Handle schema changes gracefully
stream_with_evolution = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", SCHEMA_VOLUME)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Add new columns automatically
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")  # Capture unparseable data
    .load(f"dbfs:{DATA_VOLUME}/blocks/")
)
```

## Extensions

### Real-Time Ingestion with Web3

For true real-time ingestion, connect directly to Ethereum nodes:

```python
from web3 import Web3

# Connect to Ethereum node
w3 = Web3(Web3.HTTPProvider("https://mainnet.infura.io/v3/YOUR_KEY"))

# Custom streaming source (see custom streaming sources skill)
class EthereumBlockSource(DataSource):
    def latestOffset(self):
        return {"block_number": w3.eth.block_number}
    
    def partitions(self, start, end):
        # Create block range partitions
        pass
    
    def read(self, partition):
        # Fetch blocks for partition
        for block_num in range(partition.start, partition.end):
            yield w3.eth.get_block(block_num, full_transactions=True)
```

### Token Transfer Analysis

```python
# Analyze ERC-20 token transfers
token_transfers = spark.table(f"{CATALOG}.{SCHEMA}.silver_transactions").filter(
    (F.length(F.col("input_data")) == 138) &  # ERC-20 transfer signature
    (F.substring(F.col("input_data"), 1, 10) == "0xa9059cbb")  # transfer() selector
)
```

## Attribution

This use case is based on content by **Canadian Data Guy** demonstrating:

1. Free blockchain data access via AWS public datasets
2. Databricks Free Edition capabilities
3. Streaming ingestion with Autoloader
4. Medallion Architecture for blockchain data

## Related Skills

- [spark-streaming-custom-source](../databricks-skills/spark-streaming-custom-source/SKILL.md) - Custom streaming sources for direct node connections
- [real-time-analytics-pipeline](../streaming-analytics/real-time-pipeline/SKILL.md) - Real-time streaming patterns
- [delta-streaming-exactly-once](../databricks-skills/delta-streaming-exactly-once/SKILL.md) - Checkpoint and exactly-once semantics
