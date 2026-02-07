---
name: harnessing-spark-streaming-introduction-to-the-foreachbatch-
description: #
---

# Harnessing Spark Streaming: Introduction to the ForEachBatch Function

## Overview
#

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2025-03-31
- **URL:** https://www.youtube.com/watch?v=gur1oLe5t2o

## Tags
streaming, spark

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** gur1oLe5t2o

### Summary

#### **TL;DR**
- The `ForEachBatch` function in Spark Streaming is a versatile tool for handling targets not natively supported by Spark.
- It allows writing to various destinations like Postgres, SQL Server, Kafka, Delta, and more.
- It serves as an effective stepping stone for migrating from regular Spark applications to Spark Streaming.

#### **Key Ideas**
- **ForEachBatch** enables incremental processing of data batches for targets not natively supported by Spark, such as Postgres or Kafka.
- It facilitates the transition from regular Spark applications to Spark Streaming by providing a structured approach to incremental processing.
- Spark Streaming supports reading from diverse sources like Kafka, Kinesis, ADLS Gen 2/ Azure, S3, and Google Cloud Storage using `autoloader`.
- A common misconception is that Spark Streaming requires always-on processing; it operates on an configurable cadence (e.g., hourly or daily).

### Full Transcript

Here's a quick video about Spark streaming special for each batch mode. So first question first, why would you even ever use a for each batch? So not every target is supported by Spark streaming as of now. So that's where for each batch function comes in handy because Spark can support writes to Postgress, SQL server, Delta, Kafka, almost anything.

So what spark streaming's for each batch special function does is whosoever is not supported in spark streaming natively we can use the for each function to add support right also sometimes folks have complicated logic and they are right now in regular spark and they want to go to spark streaming so for each batch is a very good solution which helps them go through this journey of maturing to spark streaming without just diving deep into it straight up.

Spark streaming has a lot of sources which supports like Kafka, Kinesis which are message buffers. So they are supported also you can stream from ADLS Gen 2 or Azure or S3 or even Google Cloud Storage as long as you use datab bricks function called autoloader. autoloader essentially can look at your object storage like S3 and ADLS and identify what's new and process it.

Also one more misconception which people have is whenever they think spark streaming they think all the time meaning they'll have to run the compute all the time. It's actually a misconception. Spark streaming means incremental processing and the cadence or how often you run it is independent. Okay. So you can be reprocessing incremental data from any of the sources on the left and then you can decide whether you want to do it all the time or once a day, once every hour, once every 4 hours, whatever.

Say we read data from S3, right? And after reading from data from S3, we use our function spark readstream and then in result we get a data frame. So what is a data frame, right? So data frame in spark is a distributed data set meaning the data set is split across workers. Okay. So in this example what we are saying or in this image a data is split across four partitions. So partition 0 1 2 3 which is sitting on two different workers and all whenever you process it you are going to process them partition by partition or at least each partition independently.

And after that what you need to do is you have a special function in Spark streaming called for each batch which you'll use. And what you can after after this what you're going to do is basically define your own userdefined function. So you can write any custom code and for each batch it's it will provide you microbatch data frame to your function you have to write it in a way such that your function can receive a microbatch data frame and then do whatever right so whatever you'll define in your UDF is up to you and then you can decide to do writes into multiple sources so sometimes customers want to write in two targets in parallel or three targets in parallel so this is what spark streaming for each batch provides you.

---






---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
