---
name: how-to-actually-delete-data-in-spark-streaming-wit
description: 
---

# How to Actually Delete Data in Spark Streaming (Without Breaking Things ) 💥

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/how-to-actually-delete-data-in-spark

## Tags
streaming, kafka, ai, spark, performance, data-engineering, deltalake, databricks

## Full Content

# How to Actually Delete Data in Spark Streaming (Without Breaking Things ) 💥

**Source:** https://www.databricksters.com/p/how-to-actually-delete-data-in-spark

---



How to Actually Delete Data in Spark Streaming (Without Breaking Things ) 💥 

Databricksters

Subscribe

Sign in

Data Engineering

How to Actually Delete Data in Spark Streaming (Without Breaking Things ) 💥 

What Every Data Engineer Needs to Know About GDPR-Ready Pipelines

Canadian Data Guy

Mar 25, 2025

6

Share

Modern data pipelines are increasingly adopting streaming paradigms, but handling deletes in streaming pipelines is far from trivial. In this blog, we’ll explore how to handle deletes effectively using Delta Lake and Spark Structured Streaming, with a focus on real-world use cases like GDPR.

Why Deletes Are Hard in Streaming

Streaming pipelines are traditionally append-only. Systems like Kafka or files as a source don’t natively support deletes. But in real-world applications like GDPR, customers expect their data to be deleted across all layers: Bronze, Silver, and Gold.

Delta Lake Capabilities for DELETE

Delta Lake supports 

DELETE

, 

UPDATE

, and 

MERGE

 on batch and streaming tables. This unlocks powerful patterns for Change Data Capture (CDC), Slowly Changing Dimensions (SCD), and data correction.

Supports ACID transactions

Deletes propagate only if you plan for it

Said another way, Delta is more flexible than Kafka and Kinesis as a streaming solution.

Streaming Table Design for Delete Propagation

For append-only sinks, here’s a common pattern:

Bronze (Append-Only Example)

CREATE STREAMING TABLE bronze ( ... )
TBLPROPERTIES (
 delta.appendOnly = true
);

Note

: If 

delta.appendOnly

 is set to 

true

, 

no

 DELETE operations are allowed on that table. This setting locks the table to 

only

 support appends. So either we set the above or make sure to handle ignore DML commits (

skipChangeCommits)

 or handle DML Commits (

readChangeFeed)

. Anything else can cause failures in production.

Bronze (with Deletes/Updates/Merge)

ignore_dml_df = spark.readStream.format(&quot;delta&quot;) \
 .option(&quot;skipChangeCommits&quot;, &quot;true&quot;) \
 .table(&quot;bronze.table_with_deletes&quot;)

change_feed_df = spark.readStream.format(&quot;delta&quot;) \
 .option(&quot;readChangeFeed&quot;, &quot;true&quot;) \
 .table(&quot;bronze.table_with_deletes&quot;)

updates = change_feed_df.filter(&quot;_change_type = 'update_postimage'&quot;)
deletes = change_feed_df.filter(&quot;_change_type = 'delete'&quot;)

Understanding Deletion Vectors

Deletion vectors store metadata about logically deleted rows without immediately rewriting data files. They reduce the overhead associated with frequent DELETE operations, significantly improving DELETE efficiency.

Reduces file rewrites

Enhances performance of DELETE operations

Automatically used by Delta Lake to keep track of logically removed rows

When you want to physically remove these rows from the data files, use 

REORG TABLE ... APPLY (PURGE)

.

Importance of VACUUM and REORG

VACUUM

After running DELETE operations, Delta tables contain logically deleted data but physically retain this data until explicitly vacuumed. Without running 

VACUUM

, storage costs continually grow.

Default retention period for VACUUM is 7 days, after which data files are physically removed, optimizing storage.

Regularly running 

VACUUM

 ensures compliance with GDPR requirements by physically purging deleted data within a defined timeframe.

REORG TABLE ... APPLY (PURGE)

REORG

 is a command for rewriting data files in a Delta table that contain rows marked by deletion vectors. With 

APPLY (PURGE)

, logically deleted rows are physically removed:

REORG TABLE &lt;table_name> APPLY (PURGE);

Deletion vectors

 indicate rows that have been logically removed. 

REORG

 with 

APPLY (PURGE)

 re-creates data files without those rows.

This process permanently applies deletions, removing the need to store deletion vectors.

Essential for GDPR compliance when you need to ensure no residual data in physical storage.

Running both 

VACUUM

 and 

REORG

 periodically keeps your storage usage optimized and meets strict data compliance regulations like GDPR.

🙌 Keep This Post Discoverable: Your Support Matters!

We’re a small, independent team creating in-depth technical content like this to help the data engineering community. We don’t rely on sponsorships or ads—just passion and practical experience.

If you found this blog valuable, please take a moment to 

clap

, 

comment

, or 

share

 it. Your engagement helps search engines like Google surface this content for others and ensures you can find it again when needed.

Without interaction, even the most helpful posts can disappear into the internet void. Let’s keep high-quality content alive and accessible. 🙏

Subscribe

Now back to the blog

Choosing Between 

skipChangeCommits

 and 

readChangeFeed

When to Use 

skipChangeCommits

You do not care about Deletes/Merges/Update and you only need to handle appends then 

skipChangeCommits meets your needs.

only_appends_df = spark.readStream.format(&quot;delta&quot;) \
 .option(&quot;skipChangeCommits&quot;, &quot;true&quot;) \
 .table(&quot;bronze.table_with_deletes&quot;)

When to Use 

readChangeFeed

You need to explicitly handle all changes including DELETE and UPDATE operations.

You want detailed tracking of changes (e.g., 

_change_type

: 

insert

, 

update_preimage

, 

update_postimage

, 

delete

).

change_feed_df = spark.readStream.format(&quot;delta&quot;) \
 .option(&quot;readChangeFeed&quot;, &quot;true&quot;) \
 .table(&quot;bronze.table_with_deletes&quot;)

updates = change_feed_df.filter(&quot;_change_type = 'update_postimage'&quot;)
deletes = change_feed_df.filter(&quot;_change_type = 'delete'&quot;)

Use Case: GDPR Delete Propagation

GDPR compliance demands timely deletion:

DELETE FROM bronze WHERE email = 'user@domain.com';
DELETE FROM silver WHERE email = 'user@domain.com';

After deletes:

Execute 

REORG TABLE &lt;table_name> APPLY (PURGE)

 to physically purge rows marked in deletion vectors.

Run periodic 

VACUUM

 to remove data files no longer referenced.

Row-Level Concurrency

Databricks provides row-level concurrency, ensuring multiple writes (updates, deletes, merges) can be performed without corrupting the data. Instead of locking entire partitions or tables, Databricks manages concurrent writes at the file or row level.

How it Works

Write Conflicts

: If multiple transactions try to update the same rows simultaneously, Delta Lake checks the conflict at the row level.

Partition/File Boundaries

: For partitioned tables, concurrency can also be handled by partition-level transactions if updates touch different partitions.

Isolation Levels

: Databricks uses Snapshot Isolation by default and can escalate to Write-Serializable isolation when merges or updates overlap.

Why it Matters?

Prevents data corruption.

Ensures high availability — you don’t need to pause all jobs for one update.

Complex streaming and batch patterns can coexist safely.

Summary and Recommendations

✅ Best Practice:

set 

delta.appendOnly = true

 on tables where you want to lock your source table from DML operations.

Perform deletes explicitly at Bronze and Silver layers.

Periodically execute 

REORG TABLE ... APPLY (PURGE)

 and 

VACUUM.

Decide between 

skipChangeCommits

 and 

readChangeFeed

 based on downstream needs.

FAQ

If I enable predictive optimization, do I need to run 

REORG

 / 

PURGE

 myself?

Yes — enabling predictive optimization does 

not

 automatically execute the physical reorganization and purging of deleted data. You still must explicitly run 

REORG TABLE … APPLY (PURGE)

Do we need to pause batch and streaming jobs during the delete window?

If you are only doing streaming appends alongside a single DML operation (like MERGE, UPDATE, or DELETE), conflicts are minimal. 



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
