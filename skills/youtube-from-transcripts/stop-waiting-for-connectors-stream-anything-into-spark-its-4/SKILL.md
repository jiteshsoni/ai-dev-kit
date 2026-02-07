---
name: stop-waiting-for-connectors-stream-anything-into-spark-its-4
description: To effectively use Apache Spark for streaming data from various APIs, follow these steps:

1. **Understand the Basics of Spark Streaming**:
   - Spark...
---

# Stop Waiting for Connectors: Stream ANYTHING into Spark (It's 4 Functions)

## Overview
To effectively use Apache Spark for streaming data from various APIs, follow these steps:

1. **Understand the Basics of Spark Streaming**:
   - Spark Streaming processes real-time or live data streams using Resilient Distributed Datasets (RDDs). It allows you to handle events as they occur.

2. **Custom Data Readers**:
   - Develop custom reader functions tailored to your data source. These functions will read data from APIs and format it appropriately for Spark processing.
   - Example: For an API like Web3, create a function that reads JSON data and converts it into a structured format (e.g., Parquet).

3. **Offset Management**:
   - **Initial Offset**: Determine the starting point of your stream. If you have historical data, set the initial offset to resume processing from where it lef

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2025-11-03
- **URL:** https://www.youtube.com/watch?v=LnfIB-u4Ja8

## Tags
ai, streaming, spark

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** LnfIB-u4Ja8

### Summary

To effectively use Apache Spark for streaming data from various APIs, follow these steps:

1. **Understand the Basics of Spark Streaming**:
   - Spark Streaming processes real-time or live data streams using Resilient Distributed Datasets (RDDs). It allows you to handle events as they occur.

2. **Custom Data Readers**:
   - Develop custom reader functions tailored to your data source. These functions will read data from APIs and format it appropriately for Spark processing.
   - Example: For an API like Web3, create a function that reads JSON data and converts it into a structured format (e.g., Parquet).

3. **Offset Management**:
   - **Initial Offset**: Determine the starting point of your stream. If you have historical data, set the initial offset to resume processing from where it left off.
   - **Latest Offset**: Identify when the most recent data is available to know when to stop processing or switch over.

4. **Shuffle Partitions**:
   - Use shuffle partitions in Spark to distribute processing across executors efficiently. This helps in managing large datasets by partitioning them into manageable chunks.

5. **Max Retries Configuration**:
   - Configure max retries for tasks to handle errors gracefully, ensuring Spark retries failed tasks without crashing the entire job.

6. **Checkpoints for Reliability**:
   - Implement checkpoints in a reliable storage system like a Unity Catalog to maintain consistent state between Spark sessions and enable resuming interrupted jobs.

7. **Data Storage with Delta Tables**:
   - Use Delta tables for fast, incremental data writing and reading. This is ideal for streaming without needing full batch processing.

8. **Implementation Steps**:
   - Create custom reader functions.
   - Set up offsets to track the stream's progress.
   - Configure shuffle partitions for efficient task distribution.
   - Implement checkpoint management.
   - Use Delta tables for data storage.

### Full Transcript

If you can define a block of work, you can stream any sports in Spark. Today I'm joined with Yogita who process the Ethereum blockchain with Spark streaming. The same pattern works for REST APIs and more. Yoga, can you introduce yourself and share what you built? Hello everyone, my name is Yogita and I've been working as a data engineer for the past four years building end to end data pipelines using Azure and AWS. So basically when we talk about ingesting data from Ethereum, it's not like pulling data from a database or a CSV file. The data lives on a decentralized blockchain. To access you typically have to call an API exposed by the Ethereum nodes like the JSON RPC or web3 APIs. These APIs return data in a deeply nested JSON format such as blocks, transactions, smart contract logs, and so on.

While Spark ships native streaming sources for Kafka, Kinesis, and S3 ADLS, ETM node API aren't one of them. In this demo, I will walk you through how we have built a custom reader and the how the custom reader ingest the Ethereum data in a streaming fashion. So, by the way, this pattern is very repeatable. A lot of times I see people ingesting data from rest API calls and throttle themselves. They'll get a lot of rate limiting errors. So what we are showing is super repeatable where you can take this code edit it a bit and make any rest API source injection happen.

Absolutely. Thank you for your comment here Chesh. So as you can see that we have built this custom reader API. So before we call in the custom reader function like we have to do few setup of the catalog where you need to pull the data into everything is handled by the code itself and and it is done automatically. So the first block of code is essentially handling the creation of the catalog and the remaining folders such as checkpoints folders. So let's get right into execution.

And just to clarify this, your target here is a delta table. Yeah, exactly. Oh, right. And technically you can replace this delta table with Kafka or GIS or anything else you like. Yeah, exactly. Let's execute the second cell. The checkpoint is a really important uh folder for the Spark structured streaming. So it essentially saves all the metadata about the checkpoint and offsets in this particular folder.

That's another common mistake which people do. People actually keep the checkpoint in the DBFS which is not the right place because it's a throwaway space. You should always keep it in a Unity catalog volume. The Unity catalog behind the scenes is a S3 and AD list location. So it will remain no matter what happens to the workspace. Absolutely. This is the main custom reader function which has been implemented to read the Ethereum data from the API.

As I mentioned, what you need for the spark structured streaming essentially these PI main functions which you need to implement and tell spark how to retrieve the data from the Ethereum blockchain and that is what is done in this block of code. We have functions for initial offset which is the starting offset for new streaming queries. When it starts fresh from scratch, this is the function which gets called. Latest offset returns the latest available block from the blockchain. This is called frequently in every microbatch trigger and it is called before partitions for each batch. and it is called on the driver node.

Moving on, we have the partition functions which essentially creates the partitions for the parallel block processing. So this is called after the latest offset and called on the driver node and it determines the parallelism for this batch. You have the very important function which is the read which reads the actual blockchain data for a given partition. After partition returns a partition objects. This is called in parallel on the executor nodes.

Couple of questions and just want to understand if I am following correctly. So can you go back to the image which you had? As long as I am able to implement the initial offse



---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
