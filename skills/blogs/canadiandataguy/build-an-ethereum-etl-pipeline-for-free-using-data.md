---
name: build-an-ethereum-etl-pipeline-for-free-using-data
description: 
---

# Build an Ethereum ETL Pipeline for Free Using Databricks Free Edition

## Overview


## Source
- **Author:** Canadian Data Guy
- **URL:** https://www.canadiandataguy.com/p/build-an-ethereum-etl-pipeline-for

## Tags
streaming, joins, ai, spark, performance, data-engineering, deltalake, databricks

## Full Content

# Build an Ethereum ETL Pipeline for Free Using Databricks Free Edition

**Source:** https://www.canadiandataguy.com/p/build-an-ethereum-etl-pipeline-for

**Blog:** Canadian Data Guy

---



Build an Ethereum ETL Pipeline for Free Using Databricks Free Edition

Canadian Data Guy Unfiltered

Subscribe

Sign in

Build an Ethereum ETL Pipeline for Free Using Databricks Free Edition

Build a zero-infrastructure streaming pipeline: Step-by-step Ethereum data ingestion, schema evolution, and Delta storage

Yogita Nesargi

Sep 23, 2025

4

1

Share

The Ethereum blockchain generates one of the richest transactional datasets in the world. Yet analyzing this data directly from a node proves impractical — blocks and transactions arrive as deeply nested structures, making efficient querying nearly impossible without transformation.

Best of all? You can build this entire pipeline without spending a dollar. Databricks Free Edition is a no-cost version of Databricks designed for students, educators, hobbyists, and anyone interested in learning or experimenting with data and AI 

Databricks

. Simply 

sign up for Databricks Free Edition

 — no credit card required — and you'll get a workspace with serverless compute ready to go.

Thanks for reading CanadianDataGuy’s No Fluff Newsletter! Subscribe for free to receive new posts and support my work.

Subscribe

In this technical walkthrough, we'll build a streaming ETL pipeline in Databricks that:

Ingests raw Ethereum blocks from AWS's public dataset

Extracts and flattens transaction data

Stores everything in queryable Delta Lake tables for analytics

=

Why Databricks + AutoLoader for Blockchain Data?

Databricks Autoloader provides a Structured Streaming source called 

cloudFiles

 that automatically processes new files as they arrive, with the option of also processing existing files 

Microsoft Learn

Databricks

. This makes it ideal for blockchain data because:

Scalable storage

: Blockchain data grows continuously — Delta Lake handles petabyte-scale datasets effortlessly

Schema enforcement

: Flatten complex nested Ethereum data into clean, queryable tables with automatic schema evolution

Streaming ingestion

: Process blocks and transactions in near real-time as they arrive

SQL + ML integration

: Run ad-hoc queries or feed data directly into ML models for fraud detection, token analytics, or NFT tracking

Step 1: Load Historical Ethereum Data from AWS S3

AWS provides free access to blockchain datasets through the aws-public-blockchain S3 bucket, with data optimized for analytics by being transformed into compressed Parquet files, partitioned by date for efficient querying 

AWS

AWS Open Data Registry

.

Instead of setting up an Ethereum node, we'll directly pull historical block data that's already preprocessed into Parquet format — completely free.

Here's what we'll accomplish:

Connect anonymously to AWS S3

List available Ethereum block files (stored as Parquet)

Download selected files into a Unity Catalog Volume in Databricks

Verify successful data landing

This foundational step prepares our workspace for efficient block processing using Spark and Delta Lake.

# Databricks notebook source
# === SIMPLE PARAMETERIZATION (ONLY NECESSARY VARIABLES) ===
# Parameterize only what needs to be variable for reusability
dbutils.widgets.text(&quot;catalog_name&quot;, &quot;blockchain&quot;, &quot;Catalog Name&quot;)
dbutils.widgets.text(&quot;schema_name&quot;, &quot;ethereum&quot;, &quot;Schema Name&quot;)
dbutils.widgets.text(&quot;num_files&quot;, &quot;20&quot;, &quot;Number of Files to Download&quot;)

# === CONFIGURATION ===
# Get widget values
CATALOG = dbutils.widgets.get(&quot;catalog_name&quot;)
SCHEMA = dbutils.widgets.get(&quot;schema_name&quot;)
NUM_FILES = int(dbutils.widgets.get(&quot;num_files&quot;))

# Hard-coded values (no need to parameterize constants)
AWS_BUCKET = &quot;aws-public-blockchain&quot;
S3_PREFIX = &quot;v1.0/eth/blocks/&quot;

# Unity Catalog volume paths for data organization
DATA_VOLUME = f&quot;/Volumes/{CATALOG}/{SCHEMA}/ethereum&quot;
CHECKPOINT_VOLUME = f&quot;/Volumes/{CATALOG}/{SCHEMA}/ethereum_checkpoints&quot;
SCHEMA_VOLUME = f&quot;/Volumes/{CATALOG}/{SCHEMA}/ethereum_schemas&quot;

print(f&quot;🔧 Using Catalog: {CATALOG}, Schema: {SCHEMA}&quot;)
print(f&quot;📦 Downloading {NUM_FILES} files from s3://{AWS_BUCKET}/{S3_PREFIX}&quot;)

# === UNITY CATALOG SETUP ===
stmts = [
 f&quot;CREATE CATALOG IF NOT EXISTS {CATALOG}&quot;,
 f&quot;CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}&quot;,
 f&quot;CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.ethereum&quot;,
 f&quot;CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.ethereum_checkpoints&quot;,
 f&quot;CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.ethereum_schemas&quot;,
]

for i, s in enumerate(stmts, 1):
 print(f&quot;[{i}/{len(stmts)}] {s}&quot;)
 try:
 spark.sql(s)
 print(&quot; ✅ Success&quot;)
 except Exception as e:
 print(f&quot; ❌ Error: {e}&quot;)

print(f&quot;\nCreated/verified UC objects. Paths available:&quot;)
print(f&quot; Data: {DATA_VOLUME}&quot;)
print(f&quot; Checkpoints: {CHECKPOINT_VOLUME}&quot;)
print(f&quot; Schemas: {SCHEMA_VOLUME}&quot;)

# === DATA DOWNLOAD ===
import os
import boto3
from botocore import UNSIGNED
from botocore.client import Config

print(f&quot;\n📥 Downloading to: {DATA_VOLUME}&quot;)
os.makedirs(DATA_VOLUME, exist_ok=True)

# Configure anonymous S3 client (no AWS credentials needed!)
s3 = boto3.client(&quot;s3&quot;, config=Config(signature_version=UNSIGNED))

# Collect parquet files from S3
keys = []
token = None

while len(keys) &lt; NUM_FILES:
 params = {
 &quot;Bucket&quot;: AWS_BUCKET, 
 &quot;Prefix&quot;: S3_PREFIX, 
 &quot;MaxKeys&quot;: min(1000, NUM_FILES - len(keys))
 }
 if token:
 params[&quot;ContinuationToken&quot;] = token

 resp = s3.list_objects_v2(**params)

 for obj in resp.get(&quot;Contents&quot;, []) or []:
 if obj[&quot;Key&quot;].endswith(&quot;.parquet&quot;):
 keys.append(obj[&quot;Key&quot;])
 if len(keys) >= NUM_FILES:
 break

 if not resp.get(&quot;IsTruncated&quot;):
 break
 token = resp.get(&quot;NextContinuationToken&quot;)

if not keys:
 raise RuntimeError(f&quot;No parquet files found under s3://{AWS_BUCKET}/{S3_PREFIX}&quot;)

# Download files with progress tracking
for i, key in enumerate(keys, 1):
 rel_path = key.replace(&quot;v1.0/eth/&quot;, &quot;&quot;)
 dest_path = os.path.join(DATA_VOLUME, rel_path)
 os.makedirs(os.path.dirname(dest_path), exist_ok=True)

 print(f&quot;[{i}/{len(keys)}] {os.path.basename(key)} ...&quot;, end=&quot; &quot;, flush=True)
 s3.download_file(AWS_BUCKET, key, dest_path)
 print(&quot;✓&quot;)

print(&quot;✅ Download complete!&quot;)

Step 2: Stream Raw Blocks with Autoloader

With Ethereum blockchain data now in our Unity Catalog volume, we can continuously process new blocks as they arrive. In production, you'd use web3.py to poll the Ethereum network and save new blocks as Parquet files. For now, we'll stream the historical Parquet files we downloaded.

Autoloader's cloudFiles source automatically processes new files as they arrive 

Microsoft

, making it perfect for blockchain data ingestion.

How Autoloader Works

Configuration options specific to the cloudFiles source are prefixed with cloudFiles so that they are in a separate namespace from other Structured Streaming source options 

Databricks

. Key features include:

Automatic file discovery

: Watches folders continuously and picks up new files

Schema evolution

: Auto Loader can detect schema drifts, notify you when schema changes happen, and rescue data that would have been otherwise ignored or lost 

What is Auto Loader? - Azure Databricks | Microsoft Learn

Exactly-once processing

: Maintains state in checkpoint location to ensure no data loss or duplication

Schema Hints: Provides control if you want to specially handle a few column without handling each and every column which is tedious

# === STREAMING READER ===
reader = (
 spar



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
