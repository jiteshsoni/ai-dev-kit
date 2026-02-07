---
name: a-deep-dive-into-spark-stream-static-joins-live-demo-caveats
description: **Spark Stream Static Joins Notes**
---

# A Deep Dive into Spark Stream Static Joins: Live Demo, Caveats and Tips

## Overview
**Spark Stream Static Joins Notes**

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2025-07-09
- **URL:** https://www.youtube.com/watch?v=0d7MONcTDD0

## Tags
joins, spark

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** 0d7MONcTDD0

### Summary

**Spark Stream Static Joins Notes**

#### **TL;DR**
- **Purpose**: Use Spark stream static joins for real-time data enrichment where one dataset (e.g., IoT sensors) is fast-moving, and another (e.g., device dimensions) is slower-changing or static.
- **Key Benefit**: Delta tables enable efficient stateless joins with minimal checkpoint overhead, ensuring timely updates even when new devices are added.

#### **Key Ideas**

1. **Definition and Use Case**
   - **Stream Static Join**: Joins a fast-moving data source (e.g., IoT sensors) with a slower-changing table (e.g., device dimensions).
   - Ideal for scenarios requiring real-time enrichment, such as IoT applications where sensor data is quickly augmented with static or less frequent updates.

2. **Delta Tables**
   - Use delta tables to track changes efficiently.
   - Stateless joins ensure each batch processes independently, preventing dropped rows and enabling consistent updates.

3. **Demo Setup**
   - Small dimension table updated every minute.
   - Synthetic IoT stream with high volume (up to 1 million rows per second).
   - Stream static join enriches IoT data with device information, ensuring timely updates.

4. **Avoid Inner Joins in Production**
   - Inner joins can lead to data loss if the dimension table isn't updated in time.
   - Opt for left joins to ensure all IoT events are processed, even without the latest device info.

5. **Optimization Techniques**
   - Use broadcast hash join when possible (select fewer columns from IoT stream).
   - Monitor and optimize input rates vs processing times to maintain efficiency.

### Full Transcript

Okay, in this video we're going to talk about Spark stream static joins. We're going to do a live demo and we're going to wrap up with a mistake which I see customers make commonly when taking the job to production. Okay, so let's get started. So Spark has two types of joins in streaming. One is called stream stream join and stream static join. We are going to talk about stream static join in this video.

So when do you use spark stream static join? So when you have a fastmoving source in our case I created like a IoT data source and on the other side you have a table which gets updates less often. Now how do you define less often? So think updates happening every 15 minutes every hour something of that sort and the other side of the table which is not streaming should be slowly moving. It shouldn't be like a big volume table. So imagine a fact joining a dimension.

So in our scenario what we have is we have IoT streaming data. Now this could be very fast moving this could be 3 million rows a second doesn't matter and what we are trying to do is while this data is coming in we are trying to join it with a device dimension table. So we can take the example further which is here we have sensors coming online emitting data about the device stuff like that and on the other side we have a dimension table which has attributes about this device which has come online right and then what we're trying to do is join this basically enrich the data and write out to a delta table so that's what we have in our scenario your input source is synthetic but it could be a Kafka stream. It could be a kinesis stream. It could be just files landing on it.

So it doesn't matter as long as we can create a streaming source out of it which means Kafkais red panda or any kind of objects arriving on S3 ADLs. And on the other side we have a delta table. And now we are going to join it and then write it into our IoT table.

So this is how set my experiment up. I'm creating low volume three rows per second. You can do one row a second no problem. And then what I'm doing is I have a small dimension table and every minute I'm replacing or updating the table. Now realistically in real life I would say every 10 15 minutes is a reasonable number.

What spark is going to do for us is that it's going to process the input data in microbatches and then before it process every microbatch going to read the latest version of the data from the dimension table. Now I'm saying delta table specifically because this feature that every micro it will check if the data has changed is only for delta. So that is where this static word comes in because if the dimension table was a parket table, JSON table, anything which is not delta, this wouldn't have worked.

So if you had a job which ran for like 10 hours, the way this would have function is like spark only at the point where it started the first microbatch, it would look up the data of the other table and then every other micro it wouldn't have cared if anything changed on the dimension table side. So that is where the word static comes into the picture.

If you have a delta table then life is easy. Even if the dimension table is getting updated throughout the day, no problem. After every micro batch, Spark will check if the data is there or not and the join would happen. If your table has a lot of rows, you can apply some filters. You can limit the number of columns which you going to emit. The reason is you can do a broadcast hash join if the table on the other side is small. Sometimes you can't make the table small but you can subselect columns. So that's another way you can achieve it.

Another other features about this is that this is a stateless join. When you have a stateless join the benefit which you get is that the checkpoint becomes very light because the checkpoint does not have any data in it.

This is our small dimension table. It's 5:41 p.m. right now. And like I said in this table, basically what I'm doing is writing data at every minute. The application by the way has been running for 7 days. So it's all good. It's all tested. And as you can see updates are arriving every minute in our dimension table.

I am using DLB data gen library to generate data. Again, I'm generating small volume, but I personally pushed this to million ro



---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
