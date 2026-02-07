---
name: stop-waiting-for-connectors-stream-anything-into-s
description: 
---

# Stop Waiting for Connectors: Stream ANYTHING into Spark (It's 4 Functions)

## Overview


## Source
- **Author:** Canadian Data Guy
- **URL:** https://www.canadiandataguy.com/p/stop-waiting-for-connectors-stream

## Tags
streaming, joins, ai, spark, clustering

## Full Content

# Stop Waiting for Connectors: Stream ANYTHING into Spark (It's 4 Functions)

**Source:** https://www.canadiandataguy.com/p/stop-waiting-for-connectors-stream

**Blog:** Canadian Data Guy

---



Stop Waiting for Connectors: Stream ANYTHING into Spark (It&#x27;s 4 Functions)

Canadian Data Guy Unfiltered

Subscribe

Sign in

Playback speed

×

Share post

Share post at current time

Share from 0:00

0:00

/

0:00

4

1

1

Stop Waiting for Connectors: Stream ANYTHING into Spark (It's 4 Functions)

How to ingest data from any source into Apache Spark — demystified with real-world example of BlockChain Ingestion

Canadian Data Guy

 and 

Yogita Nesargi

Nov 03, 2025

4

1

1

Share

💡 What You’ll Learn

By the end of this guide, you’ll understand that building a custom Spark streaming source isn’t rocket science. It’s actually a well-defined conversation between Spark and your code, with just 

5 key methods

 to implement. We’ll use a real Ethereum blockchain streaming example to show you exactly how it works.

The Problem: You Have Data, Spark Wants It

You’ve got data streaming in from somewhere unique — maybe it’s IoT sensors, a blockchain, a custom message queue, or an internal database. You want to process it with Spark’s powerful distributed engine, but there’s no pre-built connector. What do you do?

The good news: 

You can build your own custom source

. The even better news: 

It’s simpler than you think

.

Real-World Use Case:

 In this guide, we’ll walk through streaming Ethereum blockchain data into Spark. The same principles apply to any data source — from proprietary APIs to custom databases. The pattern is universal.

The Secret: It’s Just a Conversation

Think of building a custom Spark streaming source as a conversation between two specialists:

The Two Characters in Our Story

Spark’s job

 (the Project Manager) is to handle all the complex distributed computing stuff: checkpointing, fault tolerance, distributing work across a cluster, and guaranteeing exactly-once processing semantics.

Your code’s job

 (the Data Specialist) is much simpler: answer Spark’s questions about where your data is, how to access it, and how to break it into chunks that can be processed in parallel.

🎯 Key Insight:

 You don’t need to understand distributed systems, fault tolerance algorithms, or checkpoint mechanisms. You just need to implement 5 simple methods that answer Spark’s questions about your data source.

The 5 Questions Spark Will Ask You

Spark’s conversation with your code follows a predictable pattern. It asks 5 questions, and you provide straightforward answers. Let’s look at each one:

Let’s See Real Code: Streaming Ethereum Blocks

Theory is great, but let’s look at actual implementation. Here’s how these 5 methods work in practice for streaming Ethereum blockchain data:

1.

 initialOffset() — Setting the Starting Point

def initialOffset(self) -> dict:
 “”“
 Called ONCE when starting a brand new query.
 Return where to begin reading.
 “”“
 start_block = self.options.get(”start_block”, 0)
 return {”offset”: int(start_block)}

That’s it! Just return a dictionary with your starting position. Spark saves this and uses it as the baseline for the entire query lifecycle.

2.

 latestOffset() — Checking What’s Available

def latestOffset(self) -> dict:
 “”“
 Called at the START of every batch.
 Connect to your source and return the newest available data.
 “”“
 latest_block = self.w3.eth.block_number
 return {”offset”: int(latest_block)}

This method connects to your data source (in this case, an Ethereum node) and asks “what’s the latest?” The answer defines the upper bound for the current batch.

⚠️ Python API Limitation:

 In PySpark, 

latestOffset()

 must return the absolute latest data point. If you’re backfilling from very old data, your first batch could be huge. The Scala API offers more fine-grained control here, but for most real-time use cases, the Python API works perfectly.

📝 Note:

 This limitation is actively being addressed - there’s currently a pull request in progress to fix this in Spark.

3.

 partitions() — Dividing the Work

def partitions(self, start: dict, end: dict) -> list:
 “”“
 Spark gives you a range (start → end).
 You break it into smaller chunks for parallel processing.
 “”“
 start_block = start[”offset”]
 end_block = end[”offset”] # This is EXCLUSIVE (not included)

 num_partitions = self.spark.conf.get(”spark.sql.shuffle.partitions”, “4”)
 blocks_per_partition = (end_block - start_block) // int(num_partitions)

 partitions = []
 for i in range(int(num_partitions)):
 partition_start = start_block + (i * blocks_per_partition)
 partition_end = partition_start + blocks_per_partition
 if i == int(num_partitions) - 1: # Last partition gets any remainder
 partition_end = end_block

 partitions.append(BlockRangePartition(partition_start, partition_end))

 return partitions

How Partitioning Works

s

🔑 Critical Detail:

 Notice that the end block (1100) is 

exclusive

. This means partition ranges are [1000, 1025), [1025, 1050), etc. Block 1100 is NOT processed—it becomes the start of the next batch. This [start, end) pattern is how Spark guarantees no data is ever processed twice.

4

 read() — Actually Fetching the Data

def read(self, partition: BlockRangePartition):
 “”“
 This runs on EXECUTOR nodes (distributed across the cluster).
 Each executor gets one partition and must fetch its assigned data.

 Must be DETERMINISTIC - same input = same output, every time.
 This allows Spark to safely retry failed tasks.
 “”“
 for block_number in range(partition.start_block, partition.end_block):
 # Connect to Ethereum and fetch this specific block
 block = self.w3.eth.get_block(block_number, full_transactions=True)

 # Convert to Spark Row format
 yield Row(
 block_number=block.number,
 block_hash=block.hash.hex(),
 timestamp=block.timestamp,
 transaction_count=len(block.transactions),
 # ... more fields ...
 )

This is where the real work happens! Each executor in your cluster runs this method for its assigned partition, fetching the actual data.

💪 The Power of Parallelism:

 If you have 10 executors and create 100 partitions, all 10 executors work simultaneously. Each one processes its chunk, and as executors finish, Spark automatically assigns them new partitions. This is how Spark achieves massive throughput.

5

 commit() — Cleanup (Usually Empty)

def commit(self, end: dict):
 “”“
 Called AFTER all partitions successfully complete.
 The checkpoint/commit/{N} file gets created at this point.
 This method is optional - mainly used for cleanup tasks.
 “”“
 pass # Usually empty unless you need cleanup

In most cases, this method is empty. The checkpoint/commit/{N} file gets created automatically. You only need to implement this if you have cleanup tasks to perform after a batch completes.

The Complete Flow: Visual Walkthrough

Now let’s see how these methods work together in a complete streaming query:

Why This Design Is Brilliant

🛡️ Fault Tolerance

If an executor fails while reading blocks 1025-1050, Spark simply restarts that task on another machine. Because 

read()

 is deterministic, it fetches exactly the same data again. The user never knows a failure occurred.

⚡ Exactly-Once Semantics

The [start, end) exclusive range pattern means no block is ever processed twice. Block 1100 is the start of the next batch, not the end of the previous one. Combined with checkpointing, this guarantees exactly-once processing.

🚀 Massive Parallelism

By implementing 

partitions()

, you tell Spark how to break work into chunks. Spark handles distributing those chunks to hundreds or thousands of executors. You get massive scale “for free.”

🧩 Separation of Concerns

You focus on 

your data source’s logic

. Spark handles scheduling, distribution, checkpointing, fault recovery, and coordination. Clean boundaries make complex systems manageable.

What About Edge Cases?

Handling Source Failures

What if Ethereum node goe



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
