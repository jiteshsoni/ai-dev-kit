---
name: spark-streaming-master-class-ingest-data-from-kafka-to-delta
description: #
---

# Spark Streaming Master Class: Ingest data from Kafka to Delta with Spark Streaming

## Overview
#

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2025-01-15
- **URL:** https://www.youtube.com/watch?v=snJs2DlzA0o

## Tags
kafka, streaming, spark, deltalake

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** snJs2DlzA0o

### Summary

#### **TL;DR**
- **Key Benefits**:  
  - No need for a new paradigm—same APIs and engine work for both batch and streaming.  
  - Checkpointing handles duplicate data efficiently, improving scalability.  
  - Real-time decision-making is faster with Spark Streaming compared to batch processing.

- **Use Cases**:  
  - Near real-time (1-800ms) vs. true real-time (>800ms).  
  - Low-latency processing without compromising cost by strategically choosing data sources.  

#### **Key Ideas**
1. **Simplicity for Newbies**:  
   - Spark Streaming is user-friendly, even for newcomers who are already familiar with Spark.
   
2. **Enterprise Adoption**:  
   - Data Bricks supports Spark Streaming as a key tool for real-time and batch processing.

3. **Real-Time Capabilities**:  
   - Leverages Apache Kafka for streaming data to Delta for batch analytics seamlessly.

4. **Checkpointing**:  
   - Automatic duplicate handling reduces operational overhead and improves efficiency in cluster management.

5. **Fault Recovery**:  
   - Built-in mechanisms ensure minimal downtime, even during high traffic or system failures.

6. **Latency Considerations**:  
   - For near real-time (1s-800ms), Spark Streaming is cost-effective with optimized processing.
   - True real-time (>800ms) offers flexibility by balancing latency and costs through efficient data scheduling.

### Full Transcript

The way I have designed this is there is something for everyone if you're new to spark there's something for you if you have been building spark streaming applications for years there still in-depth content for you for the last four years at data bricks I have been taking streaming workloads through production almost every week.

decisions will be made in real time regardless of whether real time analytics is available or not you don't need to start learning a brand new paradigm the same apis work the same engine works uh checkpointing says you don't worry about what's n new I'll identify what's net new and only process that and after all the data is exhausted it will shut down the cluster automatically for you so that's the benefit of trigger available now.

As long as Max offsets behind latest is dropping for your running streaming job it means your processing rate is automatically greater than input rate that's why this is happening and you are not lagging behind one should always be on the latest technology stack or at least on the popular technology stack because it helps in career growth.

We're going to do a short intro on spark streaming then we are going to spend a majority of the time doing a live demo the way I have designed this is that there is something for everyone if you're new to spark there's something for you if you have been building spark streaming applications for years there's still some in-depth content for you.

For the last four years at datab briak I I've been taking streaming workloads to production almost every week so lots of in-depth experience on streaming I'm trying to give you the content which I did not get when I was starting my streaming Journey.

So why real time or why streaming this is a quote from Gartner which I really like decisions will be made in real time regardless of whether real time analytics is available or not I think that's just a very good summary like World Works in real time if you can work at the same speed good on you if you cannot you're going to just lose out I think that's the Crux of why you should be doing it.

So why are folks not able to do streaming the biggest reason is that it is complex there is a small learning curb involved and historically people used to have separate systems like four years ago you would have the realtime system which could be elastic search powered it could be uh spark powered for the batch and then there were two separate system which was one which was doing just pure Real Time stuff and another system which was just doing pure bad stuff.

So this is another slide which I like which is about hey for different latency what you can do right so if you are working in a daily weekly monthly fashion you're probably into forecasting or doing basic reporting and basic analytics if you are at a early Cadence where you process data every hour say you are Amazon and trying to figure out how many orders came in the last hour and then make predictions on how many employees to bring in to packet the stuff.

So where does Park currently operate spark actually supports both spark supports true real time there's a there is a spec spark streaming has a specific mode for that it's new it's still experimental uh but if you're thinking like seconds like 1 second 2 seconds 3 seconds or say between 1 to 10 seconds anything you spark stream has a Mode called micro patch mode which is the default mode as of now um that can easily operate in the near real time mod.

So just for clarification I think if you're trying to design something for less than 800 milliseconds I think right now the best technology is flink and if you get above 800 millisecond Benchmark um spark streaming just becomes your friend because it's just so much easier uh than Flink.

Why is Park structured streaming the first point honestly is my favorite which is you don't need to start learning a brand new paradigm the same apis work the same engine works uh this unified API layer is what I really enjoy about spark streaming such that I don't have to learn too Frameworks at once.

Top reasons to start streaming today apart from the fact that it's a hot area most of the engineers are starting to do this I think um data breaks has a metric of 14 million jobs are scheduled every week which are streaming I it's a hot technology and if you're trying to get better jobs being on a newer technology stack helps you crack your job.

Top reasons to start streaming apart from career growth so one thing I told you a life without input parameters so we are saying 



---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
