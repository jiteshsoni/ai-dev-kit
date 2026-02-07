---
name: upgrading-spark-stream-with-new-checkpoints
description: 
---

# Upgrading Spark Stream with New Checkpoints

## Overview


## Source
- **Author:** Canadian Data Guy
- **URL:** https://www.canadiandataguy.com/p/how-to-upgrade-your-spark-stream

## Tags
streaming, kafka, joins, ai, spark, deltalake

## Full Content

# Upgrading Spark Stream with New Checkpoints

**Source:** https://www.canadiandataguy.com/p/how-to-upgrade-your-spark-stream

---



Upgrading Spark Stream with New Checkpoints

Canadian Data Guy Unfiltered

Subscribe

Sign in

Upgrading Spark Stream with New Checkpoints

Step-by-step instructions on implementing new checkpoints in Spark Stream applications during major upgrades, complete with practical, executable code examples

Canadian Data Guy

Aug 30, 2024

3

Share

Sometimes in life, we need to make breaking changes which require us to create a new checkpoint. Some example scenarios:

You are doing a code/application change where you are changing logic

Major Spark Version upgrade from Spark 2.x to Spark 3.x

The previous deployment was wrong, and you want to reprocess from a certain point

There could be plenty of scenarios where you want to control precisely which data(Kafka offsets) need to be processed.

Not every scenario requires a new checkpoint. 

Here is a list of things you can change without requiring a new checkpoint.

This blog helps you understand how to handle a scenario where a new checkpoint is unavoidable.

Photo by 

Patrick Tomasso

 on 

Unsplash

Kafka Basics: Topics, partition &amp; offset

Kafka Cluster has Topics: 

Topics are a way

to organize messages. Each topic has a name that is unique across the entire Kafka cluster. Messages are sent to and read from specific topics. In other words, producers write data on a topic, and consumers read data from the topic.

Topics have 

Partitions, 

and data/messages are distributed across partitions. Every message belongs to a single partition.

Partition has messages, each with a unique sequential identifier within the partition called the 

Offset.

What is the takeaway here?

We must identify what offset has already been processed for each partition, and this information can be found inside the checkpoint.

What information is inside the checkpoint?

Fetch metadata &amp; write it to WAL(write-ahead log) in the checkpoint. 

WAL

: a roll-forward journal that records transactions that have been committed but not yet applied to the main data

Fetch the actual data → process data with state info and then write it to the sink

Write the stateful information &amp; commit to the checkpoint

Under the checkpoint folder, there are four subfolders:

Sources (contain starting offset of Kafka)

Offsets (consist of WAL information)

Commits (after completion of the entire process, it goes to the commit)

State (only for stateful operations + 1 file of metadata)

How to fetch information about Offset &amp; Partition from the Checkpoint folder?

List the files at the checkpoint location; we are looking for the 

offsets

 folder.

checkpoint_location= &quot;/checkpoint_location/checkpoint_for_kafka_to_delta&quot;
dbutils.fs.ls(checkpoint_location)dbutils.fs.ls(f”{checkpoint_location}/”)

Next, we will list the files under the commits folder and identify the most recent commits.

dbutils.fs.ls(checkpoint_location)
dbutils.fs.ls(f”{checkpoint_location}/commits”)

/checkpoint_location/checkpoint_for_kafka_to_delta/commits/0
/checkpoint_location/checkpoint_for_kafka_to_delta/commits/1
/checkpoint_location/checkpoint_for_kafka_to_delta/commits/2

Once we identify the last 

commits

 file number; we will open the equivalent offsets file. In this example, we can see the latest commits is “

2”.

Now let’s view the contents of the offsets file.

#%fs head {FILL_THE_EXACT_PATH_OF_THE_FILE_WHICH_NEEDS_TO_BE_VIEWED}
%fs head /checkpoint_location/checkpoint_for_kafka_to_delta/offsets/2

{&quot;batchWatermarkMs&quot;:0,&quot;batchTimestampMs&quot;:1674623173851,&quot;conf&quot;:{&quot;spark.sql.streaming.stateStore.providerClass&quot;:&quot;org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider&quot;,&quot;spark.sql.streaming.join.stateFormatVersion&quot;:&quot;2&quot;,&quot;spark.sql.streaming.stateStore.compression.codec&quot;:&quot;lz4&quot;,&quot;spark.sql.streaming.stateStore.rocksdb.formatVersion&quot;:&quot;5&quot;,&quot;spark.sql.streaming.statefulOperator.useStrictDistribution&quot;:&quot;true&quot;,&quot;spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion&quot;:&quot;2&quot;,&quot;spark.sql.streaming.multipleWatermarkPolicy&quot;:&quot;min&quot;,&quot;spark.sql.streaming.aggregation.stateFormatVersion&quot;:&quot;2&quot;,&quot;spark.sql.shuffle.partitions&quot;:&quot;200&quot;}}
{&quot;topic_name_from_kafka&quot;:{&quot;0&quot;:400000, &quot;1&quot;:300000}}

The information of interest is in the end. This has the topic name and offset per partition.

{“topic_name_from_kafka”:{“0”:400000, “1”:300000}}

Now the easy part: Use Spark to start reading Kafka from a particular Offset

Spark Streaming start

s read stream by default 

with the 

latest 

offset. However, it provides a parameter “startingOffsets” to select a custom starting point.

startingOffsets = &quot;&quot;&quot;{&quot;topic_name_from_kafka&quot;:{&quot;0&quot;:400000, &quot;1&quot;:300000}}&quot;&quot;&quot;

kafka_stream = (spark.readStream
 .format(&quot;kafka&quot;)
 .option(&quot;kafka.bootstrap.servers&quot;, kafka_bootstrap_servers_plaintext ) 
 .option(&quot;subscribe&quot;, topic )
 .option(&quot;startingOffsets&quot;, startingOffsets )
 .load())

display(kafka_stream)

And we are Done!!. Recommend parameterizing your code so that “startingOffsets” can be passed as a parameter.

3

Share

Previous

Next

Discussion about this post

Comments

Restacks

Top

Latest

Discussions

No posts

Ready for more?

Subscribe

© 2026 Canadian Data Guy

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
