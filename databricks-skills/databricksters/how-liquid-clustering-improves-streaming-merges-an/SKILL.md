---
name: how-liquid-clustering-improves-streaming-merges-an
description: 
---

# How Liquid Clustering Improves Streaming Merges and P99 Latency

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/how-liquid-dv-and-rlc-help-improve

## Tags
streaming, joins, ai, spark, deltalake, databricks, clustering

## Full Content

# How Liquid Clustering Improves Streaming Merges and P99 Latency

**Source:** https://www.databricksters.com/p/how-liquid-dv-and-rlc-help-improve

**Blog:** Databricks

---



How Liquid Clustering Improves Streaming Merges and P99 Latency

Databricksters

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

Transcript

9

1

How Liquid Clustering Improves Streaming Merges and P99 Latency

The Trio Behind Simpler Streaming Merges: Deletion Vectors, Row-Level Concurrency, and Liquid Clustering

Canadian Data Guy

Sep 16, 2025

9

1

Share

Transcript

Why Liquid Clustering Improves Merge Efficiency

Unlike traditional Z-order clustering, which could end up re-clustering portions of the dataset even when unnecessary, Liquid Clustering is designed to be almost always incremental. It focuses on clustering only the new data that has arrived and hasn’t yet been organized, giving much stronger guarantees against re-clustering existing data. This incremental behaviour makes clustering more predictable and cost-efficient. The payoff shows up during merges: for merges to be efficient, you need to scan the fewest possible files, and that requires data to be sorted or clustered. By clustering/physically storing sorted data, Liquid Clustering ensures better file pruning which helps in faster merges, and lowers overall latency.

Reference

Behind the Scenes: How do deletion vectors actually work

 (Substack)

Deep dive into the mechanics of 

deletion vectors

: how they mark rows deleted without rewriting whole files.

Use row tracking for Delta tables

 (Delta Lake Docs) 

Delta Lake

Explains 

row tracking

: the new metadata fields (

row_id

, 

row_commit_version

) that identify and version rows. 

Delta Lake

How to enable/disable it, and what its limitations are. 

Delta Lake

Deep Dive: How Row-level Concurrency Works Out of the Box

 (Databricks Blog) 

Databricks

Describes what 

row-level concurrency

 means, and how it works in the Databricks Runtime. 

Databricks

Shows how Liquid Clustering + deletion vectors enable out-of-box conflict resolution (e.g. avoiding 

ConcurrentAppendException

 / 

ConcurrentUpdateException

). 

Databricks

Gives examples / internal logic of how concurrent modifications are tracked per row rather than per file or partition. 

Databricks

merge_and_optimize_parallel_demo_fixed.ipynb

 (GitHub notebook by “material_for_public_consumption”) 

GitHub

Demo notebook that shows merges and Optimize operations running in parallel. 

GitHub

Why Liquid Clustering

Discussion about this video

Comments

Restacks

Databricksters Podcast

A Newsletter Created by Specialists at Databricks to Make Technology Easier to Understand

A Newsletter Created by Specialists at Databricks to Make Technology Easier to Understand

Subscribe

Listen on

Substack App

RSS Feed

Appears in episode

Canadian Data Guy

Recent Episodes

Your Storage Bill Is Too High. Here Are 3 Levels of VACUUM to Fix It

Dec 2, 2025

•

Canadian Data Guy

A Deep Dive into Spark Stream Static Joins: Live Demo, Caveats and Tips

Jul 9, 2025

•

Canadian Data Guy

Ready for more?

Subscribe

© 2026 Soni

 · 

Privacy

 ∙ 

Terms

 ∙ 

Collection notice

 Start your Substack

Get the app

Substack
 is the home for great culture

 This site requires JavaScript to run correctly. Please 
turn on JavaScript
 or unblock scripts





---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
