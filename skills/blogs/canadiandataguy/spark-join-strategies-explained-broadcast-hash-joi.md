---
name: spark-join-strategies-explained-broadcast-hash-joi
description: 
---

# Spark Join Strategies Explained: Broadcast Hash Join

## Overview


## Source
- **Author:** Canadian Data Guy
- **URL:** https://www.canadiandataguy.com/p/spark-join-strategies-explained-broadcast

## Tags
streaming, joins, ai, spark, performance, data-engineering, deltalake, databricks, clustering

## Full Content

# Spark Join Strategies Explained: Broadcast Hash Join

**Source:** https://www.canadiandataguy.com/p/spark-join-strategies-explained-broadcast

**Blog:** Canadian Data Guy

---



Spark Join Strategies Explained: Broadcast Hash Join

Canadian Data Guy Unfiltered

Subscribe

Sign in

Deep Dive

Spark Join Strategies Explained: Broadcast Hash Join

Everything You Need to Know About Broadcast Hash Join

Canadian Data Guy

Apr 14, 2025

9

1

Share

Apache Spark employs multiple join strategies to efficiently combine datasets in a distributed environment. This guide provides a 

zero-to-hero

 explanation of the three primary join strategies – 

Broadcast Hash Join (BHJ)

, 

Shuffle Hash Join (SHJ)

, and 

Sort-Merge Join (SMJ)

 – with a focus on Databricks. We will explore how each strategy works, their execution plans (DAG stages, partitioning, memory and shuffle behavior), and how to tune these joins on Databricks (including relevant configurations like AQE and join hints). A visual cheat sheet and further reading resources are provided at the end.

Introduction to Spark Join Strategies

In Spark SQL, a 

join

 combines two datasets by matching rows on a common key. The way Spark executes the join greatly impacts performance, especially with large data. Spark’s Catalyst optimizer will choose a join strategy based on data statistics (size of each side, join type, etc.), or you can influence it via hints and settings. The three main join strategies for equi-joins are:

Broadcast Hash Join (BHJ)

 – Broadcasts the entire smaller dataset to all executors, avoiding shuffles for that side​, Very fast when one side is sufficiently small, analogous to a map-side join in Hadoop​

Shuffle Hash Join (SHJ)

 – Shuffles both datasets on the join key, then builds a hash table on the smaller side of each partition and streams the larger side to find matches​.

Avoids the sort step of SMJ but requires enough memory per partition.

Sort-Merge Join (SMJ)

 – Shuffles both datasets on the join key and sorts them, then merges sorted partitions to find matches​. This is Spark’s default strategy for large data and supports all join types​ . It’s robust (can spill to disk if needed) but involves heavy network and CPU overhead for sorting.

Each strategy has optimal use cases and pitfalls. In Databricks (which uses Spark under the hood), adaptive query execution (AQE) can dynamically optimize joins (e.g. switching strategies or handling skew) to improve performance​. We’ll now dive into each strategy in detail.

What is a Broadcast Hash Join (BHJ)?

A 

Broadcast Hash Join

 is an efficient strategy used to join two datasets in Spark when one of them is significantly smaller than the other. Instead of moving data across the network (shuffling) for both sides of the join, Spark copies—or &quot;broadcasts&quot;—the entire small dataset to every worker node (executor). Then, each executor performs a local hash join between its partition of the larger dataset and the entire, locally cached, small dataset. This approach helps to avoid expensive network shuffling and the need for sorting on either side of the join.

The Broadcast Process in Detail

The broadcast procedure involves:

Collecting the Data:

The driver first gathers the entire small dataset and converts it into an efficient in-memory data structure (typically a hash map).

Distributing the Data:

This hash map is then distributed (broadcast) to all executor nodes, usually via a network distribution algorithm akin to torrent distribution.

Utilizing the Broadcast Data:

Each executor then uses the broadcasted data to quickly look up matching join keys when processing its partition of the larger dataset.

Understanding these steps is crucial because if any stage fails—whether due to memory limits on the driver, executor constraints, or even network issues—the entire query may fail.

When Does Spark Use BHJ?

Spark will automatically choose to perform a Broadcast Hash Join under these conditions:

Dataset Size:

 One side of the join is smaller than a pre-configured threshold, which is by default 10 MB in open-source Spark. In Databricks environments, this threshold is commonly increased (e.g., ~30 MB with adaptive execution), meaning Databricks can handle moderately larger tables.

Join Type:

 The join condition is an equality condition (equi-join).

The setting 

spark.sql.autoBroadcastJoinThreshold

 controls this threshold and can be adjusted based on available memory and expected performance benefits.

BHJ works well with these join types:

Supported:

 Inner joins, and left, semi, or anti joins (as long as the correct side is broadcast).

Limitations:

 It is not supported for full outer joins. For right outer joins, only the left table can be broadcast; similarly, in left joins only the right table can be broadcast.

If the join type is not supported by a BHJ, Spark may revert to another join strategy, such as a sort-merge join or a broadcast nested loop join when dealing with non-equi conditions.

Databricks and Adaptive Query Execution (AQE)

In Databricks:

Adaptive Query Execution (AQE):

 AQE can dynamically convert a sort-merge join into a broadcast hash join if it determines at runtime that one side of the join is smaller than the broadcast threshold.

Higher Thresholds:

 Databricks’ default setting for auto-broadcast (often 

spark.databricks.adaptive.autoBroadcastJoinThreshold

) may be set higher (e.g., 30 MB) to allow for broadcasting moderately larger tables.

Forcing Broadcasts:

 Although AQE works automatically, you might sometimes use explicit hints (such as 

/*+ BROADCAST(table) */

 in SQL or wrapping a DataFrame with 

broadcast(df)

 in PySpark) to ensure the small dataset is broadcast immediately, thereby skipping unnecessary shuffles.

Common Misconception- Order of Joins

For optimal join order performance: Perform joins from smallest to largest tables first to minimize data shuffling⁠⁠​ 

However, do broadcast joins last

, even though this seems counterintuitive. This is because:⁠⁠​

Broadcast joins don't require shuffles and can be executed efficiently even on large fact tables

If broadcast joins are done first, the joined data needs to be shuffled again for later joins

By doing broadcast joins last, we avoid having to shuffle that data again.

Group together joins that share the same ON clause to reduce shuffling, since the data is already arranged properly

Memory and Shuffle Considerations

Using BHJ provides tremendous speedups by eliminating the costly shuffle of the larger dataset. However, it comes with some significant memory considerations:

Driver Memory:

 The whole small dataset must be collected on the driver before it can be broadcast. The driver has a memory limit, defined by 

spark.driver.maxResultSize

, and exceeding this limit will cause the job to fail.

Executor Memory:

 Each executor must have enough memory to store the broadcasted dataset along with its own processing workload. The available memory on the node with the smallest capacity is the practical limit.

Timeout and Overload Risks:

 If the dataset is even moderately large, broadcasting it might overwhelm the driver or network, leading to out-of-memory (OOM) errors or timeouts. For example, while Databricks has even seen broadcasts for datasets up to a few GB in size, one must exercise extreme caution when attempting such operations.

Compression Differences:

 Note that the on-disk size of data (like Parquet files in Delta tables) might be much smaller than the in-memory representation. Spark’s decisions are based on disk size, so actual in-memory data after decompression might far exceed the expected limits.

To address these issues, you can either disable auto-broadcast by setting 

spark.sql.autoBroadcastJoinThreshold

 to -1 or lower the threshold to ensure no large table is inadvertently broadcasted. On Databricks with the Photon engine, 

executor-side broadcasts

 further alleviate press



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
