---
name: what-is-delta-lake-how-does-it-work
description: 
---

# What is Delta Lake? How does it work?

## Overview


## Source
- **Author:** Canadian Data Guy
- **URL:** https://www.canadiandataguy.com/p/what-is-delta-lake-how-does-it-work

## Tags
streaming, joins, ai, spark, performance, data-engineering, deltalake, databricks, clustering

## Full Content

# What is Delta Lake? How does it work?

**Source:** https://www.canadiandataguy.com/p/what-is-delta-lake-how-does-it-work

---



What is Delta Lake? How does it work?

Canadian Data Guy Unfiltered

Subscribe

Sign in

What is Delta Lake? How does it work?

Dive into the inner workings of Delta Lake, from ACID transactions to time travel, and see how it combines the best of data warehouses and data lakes.

Canadian Data Guy

Sep 19, 2024

8

1

Share

This blog was written as part of our ongoing series of reading Delta, IceBerg, and Hudi white papers. I hold a lively Q&amp;A session where participants ask insightful questions about any data topic. For privacy concerns, I do not publish the Q&amp;A section of the meeting.

To join the next session follow me on 

LinkedIn

 or join 

Discord

. It's an excellent opportunity to learn, ask questions, and connect with community members.

Video Recording Of The Session

1. What is Delta Lake?

Delta Lake (or Delta Table) represents a significant evolution in data storage technology, designed to bring reliability and robustness to data lakes. As an open-source storage layer, it introduces ACID transactions to cloud object stores like Amazon S3, addressing critical challenges that have long plagued traditional data lakes. These challenges include data corruption, consistency issues, and performance problems that have hindered the effectiveness of big data analytics.

By providing a unified data management layer, Delta Lake bridges the gap between traditional data warehouses and modern data lakes, offering the best of both worlds: the scalability and flexibility of data lakes with the reliability and performance of data warehouses.

2. The Core of Delta Lake: The Transaction Log

At the heart of Delta Lake's functionality lies the transaction log. This crucial component is stored in a &quot;_delta_log&quot; subdirectory within the table's location. The log consists of two main elements:

JSON files: These are numbered sequentially (e.g., 00000.json, 00001.json) and represent individual atomic commits.

Parquet checkpoints: These provide efficient snapshots of the table state at specific points in time.

Each JSON file in the transaction log contains actions that describe changes made to the table. These actions can include:

Image Credits to Databricks https://www.databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html

Inside the Delta Log

Add file action

s: Indicates files committed to the table, including statistics

Remove file actions

: Used for logical deletion of files

Update metadata

 - Updates the table’s metadata (e.g., changing the table’s name, schema or partitioning).

Set transaction

 - Records that a structured streaming job has committed a micro-batch with the given ID.

Change protocol

 - enables new features by switching the Delta Lake transaction log to the newest software protocol.

Commit info

 - Contains information around the commit, which operation was made, from where and at what time.

The transaction log serves as the single source of truth for the table's state, enabling Delta Lake to provide its key features and guarantees. Those actions are then recorded in the transaction log as ordered, atomic units known as 

commits.

3. Write-Ahead Logging and Optimistic Concurrency Control in Delta Lake

Write-Ahead Logging (WAL)

Write-Ahead Logging is a crucial mechanism in Delta Lake that ensures data integrity and enables ACID transactions. Key aspects of WAL include:

Before any changes are made to the data files, Delta Lake writes the details of the transaction to the transaction log.

The log is stored in the &quot;_delta_log&quot; subdirectory and contains JSON files describing all operations, including file additions and removals.

Each JSON file represents a new version of the table and is updated atomically for every operation.

WAL ensures atomicity and durability of transactions.

If a system failure occurs, the log contains sufficient information to either complete the transaction during recovery or roll it back, preventing partial updates.

Optimistic Concurrency Control

Optimistic Concurrency Control (OCC) is a method used in database management systems and other distributed systems to handle concurrent access to shared resources. In the context of Delta Lake, OCC is employed to manage multiple writers without locking the entire table.

Allows for high concurrency, which is particularly effective for big data workloads where appends are more common than updates to existing records.

Enables writers to perform operations without acquiring locks, improving performance in scenarios with low conflict rates.

Involves checking for conflicts only at the time of commit, rather than throughout the entire transaction.

Relationship between WAL and Optimistic Concurrency Control

WAL and optimistic concurrency control work together in Delta Lake to provide ACID transactions while maintaining high performance and concurrency. Their relationship functions as follows:

When a writer starts a transaction, it records the start version of the table from the transaction log.

The writer performs its operations and attempts to commit by creating the next numbered JSON file in the transaction log.

If another writer has committed in the meantime, it checks for conflicts by examining the changes in the log since its read version.

If there are no conflicts (e.g., both were appends), it can still commit without rewriting files or redoing computations.

In case of conflicts, the transaction fails and can be retried.

This synergy between WAL and optimistic concurrency control allows Delta Lake to provide robust transaction management while optimizing for the high-concurrency, append-heavy workloads common in big data environments.

4. Ensuring ACID Transactions on a Data Lake

Delta Lake's implementation of ACID properties is central to its functionality. Here's how each property is achieved: 

Atomicity

Delta Lake achieves atomicity through write-ahead logging. Before executing any changes to the data files, it writes the details of the transaction to the transaction log. 

atomicity

, guarantees that operations (like an INSERT or UPDATE) performed on your 

data lake

 either complete fully, or don’t complete at all. Without this property, it’s far too easy for a hardware failure or a software bug to cause data to be only partially written to a table, resulting in messy or corrupted data.

The transaction log is the mechanism through which Delta Lake is able to offer the guarantee of atomicity. 

For all intents and purposes, if it’s not recorded in the transaction log, it never happened. By only recording transactions that execute fully and completely, and using that record as the single source of truth, the Delta Lake transaction log allows users to reason about their data, and have peace of mind about its fundamental trustworthiness, at petabyte scale.

Consistency

Consistency in Delta Lake is maintained through two primary mechanisms:

Schema Enforcement: Delta Lake automatically checks that incoming data adheres to the table's schema, as defined in the transaction log metadata. If a transaction tries to insert data that doesn't comply with the current schema, it is rejected.

Invariant Checking: Any user-defined invariants (like NOT NULL constraints) are enforced before committing the transaction. If these invariants are violated, the transaction is aborted.

Isolation

Delta Lake provides snapshot isolation, which protects reading transactions from ongoing modifications. This is achieved as follows:

When a read transaction starts, it points to a specific version of the data, which corresponds to a state of the transaction log.

Subsequent modifications (writes) do not affect this version, ensuring that the read transaction sees a consistent and unchanging view of the data, even as other modifications proceed.

Durability

Durability in Delt



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
