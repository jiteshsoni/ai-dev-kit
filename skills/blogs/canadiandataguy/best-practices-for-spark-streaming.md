---
name: best-practices-for-spark-streaming
description: 
---

# Best Practices For Spark Streaming

## Overview


## Source
- **Author:** Canadian Data Guy
- **URL:** https://www.canadiandataguy.com/p/spark-streaming-best-practices-a

## Tags
streaming, kafka, joins, ai, spark, deltalake, databricks, clustering

## Full Content

# Best Practices For Spark Streaming

**Source:** https://www.canadiandataguy.com/p/spark-streaming-best-practices-a

---



Best Practices For Spark Streaming

Canadian Data Guy Unfiltered

Subscribe

Sign in

Best Practices For Spark Streaming

Dive into this step-by-step checklist for Spark Streaming, covering foundational tips and advanced techniques to enhance your data processing capabilities.

Canadian Data Guy

Aug 26, 2024

1

Share

Most good things in life come with a nuance. While learning Streaming a few years ago, I spent hours searching for best practices. However, I would find answers to be complicated to make sense for a beginner’s mind. Thus, I devised a set of best practices that should hold true in almost all scenarios.

The checklist below has not been ordered; you should aim to check off as many items as you can.

Beginners best practices checklist for Spark Streaming:

[ ] Choose a trigger interval over nothing at all because it helps control storage transaction api/Listing costs. This is because some Spark jobs have a component which requires a s3/adls listing operation. If our processing is very fast think &lt;1 sec, we will keep repeating these operations and lead to unintended costs. Example .trigger(processingTime=’5 seconds’)

If you are using AutoLoader the switch to Notification mode 

https://docs.databricks.com/ingestion/auto-loader/file-notification-mode.html

Do not enable versioning on the S3 bucket, Delta tables have time travel to recover from failures as Versioning adds significant latency at scale.

Keep the compute and storage located in the same regions.

[ ] Use ADLS Gen2 on Azure over blob storage as it’s better suited for big data analytics workloads. 

Read more on the differences here.

[ ] Make sure the table partition strategy is chosen carefully and on low cardinality columns like date , region, country, etc. My rough rule of thumb says, if you have more than 100,000 partitions then you have over-partitioned your table. Date columns make a good partition column because they occur naturally. Example for a multinational e-commerce company which operates in 20 countries and wants to store 10 years of data. Once you partition by date &amp; country =( 365 * 10 ) * 20 = you will end up with 73,000 partitions.

[ ] Name your streaming query so it is easily identifiable in the Spark UI Streaming tab.

.option(“queryName”, “IngestFromKafka”)

(input_stream
 .select(col(&quot;eventId&quot;).alias(&quot;key&quot;), to_json(struct(col('action'), col('time'), col('processingTime'))).alias(&quot;value&quot;))
 .writeStream
 .format(&quot;kafka&quot;)
 .option(&quot;kafka.bootstrap.servers&quot;, kafka_bootstrap_servers_plaintext )
 .option(&quot;kafka.security.protocol&quot;, &quot;to_be_filled&quot;)
 .option(&quot;checkpointLocation&quot;, checkpoint_location )
 .option(&quot;topic&quot;, topic)
 .option(&quot;queryName&quot;, &quot;IngestFromKafka&quot;)
 .start()
)

spark.readStream.format(“kinesis”).

option(“streamName”, stream_name)

[ ] Each stream must have its own checkpoint; streams must never share checkpoints. Example, if you have 2 separate streams of different sources and data needs to be written to a single delta table. You should create 2 separate checkpoints and not share a common one. You can find an example with code 

here

.

[ ] Don’t run multiple streams on the same driver; if such requirements are there, please benchmark it by running the streams for a few days and watch for stability over driver-related issues. Multiplexing on the same cluster is generally not recommended.

[ ] Partition size of data in memory should be between 100–200MB. Use Spark UI and alter maxFilesPerTrigger &amp; maxBytesPerTrigger to achieve these partition sizes of around 100–200 MB.

[ ] Check if a sort merge join can be changed to Broadcast hash join. Only possible if the dataset being joined is small. The dataset being broadcasted should be around 100 MB. Increase auto-broadcast hash join threshold to 1gb if needed try a bigger instance family.

Advanced best practices checklist for Spark Streaming:

Establish a naming convention for your checkpoint: Over the course of the life of your table, you will end up having multiple checkpoints due to application upgrades, logic changes, etc. Give your checkpoint a meaningful name something which tells the following:

- Target table name

- Starting timestamp: When the checkpoint came into existence

Example naming conventions can be :

1. Generic {table_location}/_checkpoints/_{target_table_name}_starting_timestamp{_actual_timestamp

}

2. If source is Delta then use startingVersion {table_location}/_checkpoints/_{target_table_name}_startingVersion{_startingVersion

}

See Shuffle Spill (Disk) on Spark UI to be as minimum as possible. Ideally zero only shuffle read should be there. Shuffle spill disappears from UI if it’s zero.

Use rocks DB , if there are stateful transformations.

Prefer to Azure Event Hub using it it’s Kafka connector. For Azure EventHubs, the number of cores must be == to number of partitions. With Kafka connector it is different, as it can split Kafka partition into multiple Spark partitions, and this is one of the reasons to go with Kafka protocol on EventHubs.

If there is state always have a watermark so it can clean itself. In case you need to have infinite state, recommend you to store that in a Delta table and zorder on necessary column so lookups are fast.

At big scale, think close to Trillions of records in state store. If there is a deduplicate requirement, use delta merge approach over drop duplicate to avoid state store growing very large.

Azure instance family choices:

- F-series for map-heavy streams — parsing, json deserialization, etc.

- Fsv2-series if doing multiple streams from the same source or need a little spill space.

- DS_v2-series for streams that join tables or do aggregations. Also for delta optimize (both bin-packing and Z-Ordering) scheduled jobs

- L series has direct attached SSD which helps Delta caching

Don’t set the sql.shuffle.partitions too high — ideally they should be set to be equal to the total number of worker cores. You will need to clear the check point if it is not changing. It is because checkpoint has stored this information and is using that.

References:

best practices from this page

youtube video

 with tips on how to scale at production

1

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
