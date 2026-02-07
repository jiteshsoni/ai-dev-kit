---
name: the-hidden-price-of-streaming-cutting-s3-api-calls
description: 
---

# The Hidden Price of Streaming: Cutting S3 API Calls for Massive Cloud Savings

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/the-hidden-price-of-streaming-cutting

## Tags
streaming, joins, ai, spark, performance, data-engineering, deltalake, databricks, clustering

## Full Content

# The Hidden Price of Streaming: Cutting S3 API Calls for Massive Cloud Savings

**Source:** https://www.databricksters.com/p/the-hidden-price-of-streaming-cutting

---



The Hidden Price of Streaming: Cutting S3 API Calls for Massive Cloud Savings

Databricksters

Subscribe

Sign in

Data Engineering

The Hidden Price of Streaming: Cutting S3 API Calls for Massive Cloud Savings

A practical approach to cutting cloud expenses through smarter S3 API usage

Geethu

May 20, 2025

7

Share

As data pipelines scale in size and complexity, keeping operational costs under control becomes increasingly important particularly for streaming workloads. Unlike batch jobs that run at scheduled intervals, streaming pipelines operate continuously, generating a steady stream of read and write operations. This constant interaction with cloud storage services like S3 can quickly accumulate API costs if not properly managed. Gaining visibility into these patterns and optimizing them is essential to maintaining efficient, cost-effective, and scalable data systems.

Thanks for reading Databricksters! Subscribe for free to receive new posts and support my work.

Subscribe

Why S3 API Calls Matter for Cost

S3 is widely used for its scalability, durability, and integration with modern data platforms. However, beyond storage fees, S3 also charges for API requests - an often underestimated factor that can significantly impact the cost of streaming data pipelines.

In autoloader or DLT workloads in Databricks, S3 is frequently accessed for operations such as file reads, writes, checkpoints, schema inference, and metadata management. These operations translate into S3 API calls, each of which incurs a cost depending on the request type.

Use case Overview:

As an example, let's take a Delta Live Tables (DLT) pipeline or a Structured Streaming job with the default trigger interval of 500 milliseconds to illustrate how S3 API calls can quietly drive up the cost of a streaming pipeline.

Assumptions with 500ms trigger interval:

100 S3 API calls every 500 ms = 200 API calls per second = 17,280,000 API calls per day

 Breakdown

:

40%

 PUT, LIST, POST → 

69 calls/sec

60%

 GET, READ → 

131 calls/sec

📊 Daily API Call Volumes:

📅 Monthly Cost (30 Days):

 $38.71/day × 30 days = 

~$1,161.30/month

This is per pipeline cost and if we have 10 pipelines like this easily it can cost you ~10000/ month

Common Scenarios Leading to High S3 API Costs

Certain patterns such as small file writes, frequent checkpointing, or excessive schema discovery can unintentionally amplify S3 API usage. Since S3 charges per request type, even optimizations at the infrastructure or code level (e.g., reducing unnecessary LIST calls or batching writes) can have a noticeable impact on cost.

Understanding where and why S3 API calls occur in a streaming pipeline is crucial. Without this insight, organizations may see ballooning storage-related costs that don't correlate directly with the amount of data stored or processed. Therefore, monitoring and minimizing unnecessary API calls is key to building cost-efficient streaming architectures. Below are proven strategies to slash API expenses while maintaining performance.

Core Optimization Strategies

Option 1: Increase Trigger Interval in Bronze and Silver Layers

Reducing the frequency of micro-batch execution helps lower the number of S3 API calls, especially in high-volume streaming jobs where each trigger performs multiple GET, PUT, and LIST operations on S3. This optimization can be possible only when the latency requirement is not too low(in subseconds) per microbatch.

**The following metrics are based on the assumption that increasing micro-batch size leads to fewer files being read and written, as each batch processes more data.

New Assumptions with 2-Second Interval

Old Trigger Rate:

 every 500 ms → 2 triggers/sec

New Trigger Rate:

 every 2 seconds → 1 triggers/sec (consider only fewer files are written and the read rate remains same but writes are lower)

Reduction Factor:

 2× fewer triggers

New API call rate:

100 API calls every 1 seconds = 100 API calls per second

= 8,640,000 API calls/day

📊 Daily API Call Breakdown (Same 40/60 Split):

📅 Monthly Cost (30 Days):

$19.35/day × 30 = 

~$580.50/month

This shows 2x reduction in cost per pipeline which can add up to multiple pipelines and help in significant reduction in cost.

Option 2: Table Properties Settings – Use v2 Checkpointing

Delta Lake’s v2 checkpointing is an optimized format that improves how Delta tables manage transaction logs. Unlike the default checkpointing (v1), which may trigger additional reads of Parquet data to gather stats or metadata, v2 checkpointing stores those stats directly in the checkpoint files. This reduces the need to make additional S3 GET or LIST API calls—thereby lowering I/O overhead and S3 request costs.

Why It Matters for Streaming:

In streaming pipelines, frequent checkpointing is common (especially in the bronze/silver layers). Traditional checkpoints often involve multiple S3 metadata reads. v2 checkpointing reduces this footprint by minimizing how often the engine needs to fetch additional files, helping control the number of REST API calls made to S3.

Benefits:

Reduces S3 GET/LIST API calls

Speeds up streaming reads and commits

Lowers cloud storage access costs

To enable v2 Checkpointing set the below property:

ALTER TABLE my_table

SET TBLPROPERTIES (

'delta.feature.v2Checkpoint' = 'supported',

'delta.checkpointPolicy' = 'v2'

);

Option 3: Delta Lake Metadata Management

Over time, Delta Lake’s transaction log can grow significantly—especially in high-frequency streaming jobs. In one observed case, 30 days of logs resulted in 150GB+ of metadata in the _delta_log directory. This metadata bloat increases s3 list and get api calls.

Fix: Reduce Metadata Retention Durations

Set shorter retention periods for transaction logs and deleted files to control metadata growth:

ALTER TABLE silver SET TBLPROPERTIES (

'delta.logRetentionDuration' = '7 days',

'delta.deletedFileRetentionDuration' = '3 days'

);

Benefits:

85% smaller _delta_log directories

Reduced metadata scanning and S3 API usage

Lowers cloud storage access costs

Option 4: Increase the Frequency of Manual Maintenance Jobs the source and staging tables

In a continuous streaming pipeline, smaller files tend to accumulate in the intermediate tables due to high-frequency writes. Since each micro-batch writes individual records to storage, this results in a large number of small files, a scenario that can heavily degrade query performance and increase the frequency of S3 API calls (e.g., LIST and GET), leading to higher storage and I/O costs. So, it’s essential to run OPTIMIZE regularly to consolidate small files and better organize the data. Enabling auto-optimize and auto-compaction settings in Delta Lake can help automatically reduce the number of small files, ensuring more efficient file management and improved query performance.

Benefits

:

Optimizes large data volumes by consolidating smaller files, reducing file fragmentation.

Fewer S3 LIST and GET API calls due to fewer, larger files.

Continuous and regular maintenance ensures tables do not degrade over time due to excessive fragmentation.

Option 5: Reduce the Number of Min Batches to Retain

In streaming jobs, the minBatchesToRetain setting controls how many of the most recent micro-batches are retained in memory for processing. By default, spark.sql.streaming.minBatchesToRetain is set to 100, meaning the system keeps the latest 100 batches in memory. Lowering this number can help reduce API calls—particularly to S3—thereby optimizing costs.

Smaller State to Manage = Fewer Metadata Reads

When fewer micro-batches are retained, the streaming engine manages less state during job execution and checkpointing. Delta Lake may avoid reading and re-validating as many past logs or files (via list and get calls to s3



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
