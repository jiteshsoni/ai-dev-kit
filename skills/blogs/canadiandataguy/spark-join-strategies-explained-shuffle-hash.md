---
name: spark-join-strategies-explained-shuffle-hash
description: 
---

# Spark Join Strategies Explained: Shuffle Hash

## Overview


## Source
- **Author:** Canadian Data Guy
- **URL:** https://www.canadiandataguy.com/p/spark-join-strategies-explained-shuffle

## Tags
streaming, joins, ai, spark, performance, databricks, clustering

## Full Content

# Spark Join Strategies Explained: Shuffle Hash

**Source:** https://www.canadiandataguy.com/p/spark-join-strategies-explained-shuffle

**Blog:** Canadian Data Guy

---



Spark Join Strategies Explained: Shuffle Hash

Canadian Data Guy Unfiltered

Subscribe

Sign in

Deep Dive

Spark Join Strategies Explained: Shuffle Hash

Everything You Need to Know About Shuffle Hash Join

Canadian Data Guy

Apr 10, 2025

4

Share

1. Introduction

Modern big data applications often require joining huge datasets efficiently. Choosing the right join strategy is critical to optimize performance and resource usage. Apache Spark offers several join methods, including broadcast joins, sort-merge joins, and shuffle hash joins. SHJ stands out as a middle-ground approach:

It 

shuffles

 both tables like sort-merge joins to align data with the same key.

Instead of sorting, it builds an 

in-memory hash table

 for the smaller dataset per partition and probes it with rows from the larger dataset.

This dual approach has the potential to improve execution time by reducing the sorting overhead but demands careful memory management.

2. Understanding Shuffle Hash Join

Shuffle Hash Join

 is best understood as a hybrid that borrows elements from two traditional join methods:

Sort Merge Join (SMJ)

Mechanism:

 Both datasets are sorted by the join key and then merged.

Pros:

 Reliable for large datasets.

Cons:

 Sorting is CPU intensive.

Broadcast Hash Join (BHJ)

Mechanism:

 The smaller table is broadcast to all nodes, and each executor performs a local hash join.

Pros:

 Eliminates shuffling.

Cons:

 Limited by broadcast size, not suitable when the smaller table exceeds available memory on executors.

How SHJ Differentiates Itself:

Key Step:

 It shuffles both datasets based on the join key so that every partition contains matching keys.

In-Partition Operation:

 Instead of sorting the data in each partition, Spark builds a hash table from the smaller dataset's partition and then probes that table with each row from the larger dataset.

Memory Sensitivity:

 The approach assumes that each partition of the smaller side can be held in memory, which is crucial for performance and avoiding runtime errors.

Key Concepts to Remember:

No Sorting:

 Eliminates the costly sort phase.

Memory Requirement:

 High dependency on the ability to fit the hashed partition in memory, risking OOM errors if miscalculated.

3. When to Use SHJ

Historical Perspective

Pre-Spark 3.0:

Spark defaulted to Sort Merge Join for equality-based joins due to the risk of OOM when building in-memory hash tables.

Spark 3.x and Beyond:

With enhancements like Adaptive Query Execution (AQE), Spark can dynamically decide to use SHJ when it detects that:

The smaller dataset, after partitioning, is of manageable size.

Avoiding the expensive sorting operation is beneficial for performance.

Practical Scenarios

Moderately Small Datasets:

When one dataset is small enough that its partitions are lightweight (e.g., 5 MB per partition out of 5 GB divided across 1000 partitions), yet not small enough for a broadcast join.

High Sorting Overhead:

When joining a massive fact table (e.g., 1 TB) with a dimension table that is too big to broadcast but small enough per partition, the cost of sorting the entire dataset (as in SMJ) may dominate and thus SHJ becomes more efficient.

Decision Factors

Estimated Partition Size:

Spark’s optimizer checks if the estimated per-partition size of the smaller table is below a threshold (set via 

spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold

).

Configuration and Hints:

Users can guide Spark’s optimizer using hints like 

/*+ SHUFFLE_HASH(tab) */

 or disable sort-merge joins by toggling 

spark.sql.join.preferSortMergeJoin

.

spark.conf.set(&quot;spark.sql.join.preferSortMergeJoin&quot;,&quot;false&quot;)

4. How SHJ Works

The execution of a Shuffle Hash Join can be understood through two primary phases, with some literature breaking it into a three-phase model for clarity.

A. Shuffle Phase

Objective:

Bring together all rows associated with a given join key within the same partition.

Process:

Repartitioning:

Both datasets are re-distributed (shuffled) using the join key as the partitioning key. Note that 

both

 sides are shuffled – so network cost is still incurred for both datasets.

Data Co-location:

Post-shuffle, each partition will hold all the relevant rows for a specific range of join keys.

Network I/O:

While shuffling ensures correct join semantics, it incurs the cost of network communication for both datasets.

Example Scenario:

Imagine two datasets, 

Person

 and 

Address

, initially spread across different partitions. In the shuffle phase, rows with the same key (e.g., 

A001

) are sent to the same partition. This guarantees that later join operations will have all matching keys available on the same executor.

B. Hash Join Phase

After the shuffle phase, the join is executed within each partition through these steps:

Hash Table Creation:

Selection:

Spark selects the smaller dataset based on statistics or join hints.

Building the Hash Table:

For every partition, Spark creates an in-memory hash table that maps join keys to the associated rows.

Probing the Hash Table:

Streaming Data:

The larger dataset’s rows are processed sequentially within the partition.

Lookup and Join:

For each row in the larger dataset, the hash table is queried using the join key. If a match exists, Spark produces the joined row as output.

Because no sort is done, if the data per partition is large, the hash table may also be large. Spark assumes the build side will fit in memory. If it doesn’t, the task can spill partitions of the build side to disk (Spark has some support for spilling hash tables, but it is more complex than spilling a sort). In worst cases, an SHJ can run out of memory if the hash table grows too big, causing the executor to OOM. This is why Spark is conservative in using SHJ unless it’s confident the partitions are small enough​

Conceptual Diagram:

Imagine a partition where:

The smaller dataset’s partition (say, 5 MB worth of data) is fully loaded into a hash table.

The larger dataset streams through, and for each key, Spark quickly checks the in-memory hash table for corresponding rows.

This operation is performed concurrently across all partitions on different worker nodes.

Alternative Three-Phase View

For some, a detailed three-phase breakdown clarifies the process:

Shuffle:

Repartition both datasets so that all rows sharing the same join key are co-located.

Hash Table Creation:

For each partition, build the in-memory hash table using the smaller dataset.

Hash Join:

Join the larger dataset’s partition by probing the hash table.

This view underlines the importance of parallel execution, where each worker node processes its partitions independently, which is key to Spark’s scalability.

5. Supported Join Types

Shuffle Hash Join is designed to work primarily with 

equi-joins

. In Apache Spark, it supports:

Inner Joins:

Only matching rows are returned.

Left, Right, Semi, and Anti Joins:

These join types function well as long as the join condition is based on equality.

Additional Notes:

Full Outer Join:

Initially, SHJ did not support full outer joins in Spark 3.0 but was later introduced in Spark 3.1+.

Non-equi Joins and Cross Joins:

SHJ does not naturally handle cross joins or non-equi conditions. In such cases, Spark falls back on other, more suitable join strategies.

6. Performance Characteristics &amp; Trade-Offs

Understanding the performance implications of SHJ is critical for designing robust, high-performance Spark jobs.

Advantages

No Sorting Required:

By eliminating the sort step used in SMJ, SHJ significantly reduces CPU overhead.

Efficient CPU Usage:

Hash functions and probing operations are generally less costly than sorting large datasets.

Parallel Execution:

The join is process



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
