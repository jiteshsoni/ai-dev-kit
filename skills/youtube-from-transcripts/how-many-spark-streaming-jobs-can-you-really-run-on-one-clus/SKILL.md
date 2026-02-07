---
name: how-many-spark-streaming-jobs-can-you-really-run-on-one-clus
description: #
---

# How Many Spark Streaming Jobs Can You REALLY Run on One Cluster?

## Overview
#

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2025-11-29
- **URL:** https://www.youtube.com/watch?v=ylrJTCIVUUQ

## Tags
streaming, spark

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** ylrJTCIVUUQ

### Summary

#### **TL;DR**
- You can run 100 Spark streaming jobs on a single cluster with optimal configuration.
- Each job processes data at a faster rate than the input, ensuring efficient pipeline operation.

#### **Key Ideas**
1. **Cluster Utilization:** A single-node machine handles up to 100 streaming jobs without overloading, showing good resource management.
2. **Job Configuration:** Jobs are set to process data every 15 seconds, allowing for efficient batch processing and keeping the pipeline active.
3. **Performance Metrics:** The cluster demonstrates a healthy balance of CPU usage (around 50%) and sawtooth patterns in memory swap utilization, indicating optimal resource allocation.

### Full Transcript

Have you ever wondered how many streaming jobs you can actually run on a Spark cluster? In this video, we're going to find that out live. I'll walk you through a real demo, shared what I learned along the way, and leave you with the exact code so you can try it out for yourself.

Hey, I'm Sony, also known as the Canadian data guy. I have spent the last 10 years between Amazon and data bricks. In the past 5 years, I've been hands-on doing Spark streaming. My goal with the channel is very simple. to give you practical no fluff advice so you can confidently build and scale your own data pipelines in production.

All right, let's dive in. I have a streaming job which has been running for the last 11 hours and let's first start and see how the cluster is holding up. So, as you can see, it's been running about 12 hours or so. Let me open it up. So, if you click the metrics tab, it should show you all the hardware and let me tell you, it's running on a very small machine. It's running on a single node machine with eight cores.

So you can select a time frame. I selected a bigger time window. I'm showing you 9 hours of time window. And what you can see CPU is around 50% plus which is fantastic. Spark loves memory. So it used a lot of memory which I gave it. That's okay. No problem here. Memory swap utilization if you see is going up. After some point you should see it drop back. We call it a sawtooth pattern.

There are two ways to get here. One is you click the Spark UI. Once you open the Spark UI, you click the structured streaming tab once it opens up and then you get to this page. Okay. Now what's what is there to see here? So you can see 100 Spark streaming jobs I've been running for 12 hours. I'm producing for this demo. I produce 500 rows a second for each of these streams. Okay. So that's why you will see the average input is 499 something which is 500 rows a second and then we are processing this data at a faster rate.

you are creating a pipeline in spark streaming, you get to choose the processing time and this processing time defines how fast do we want a spark stream to go. So on this one I set it as 15 seconds. So what I'm asking spark to do is run my spark stream at every 15 seconds. Now let's connect the dots. If 500 by 15, you will get 7,500 rows a second. So what the graph is trying to say is I am able to process 7,500 rows in about 8 to 10 seconds.

If the stream was not keeping up then the operation duration would be greater than 15 seconds. What we are saying operation duration is 8 to 10 seconds and we have said hey spark streaming tried to run every 15 seconds. So what you can make out of this is that we are able to keep up right. We ask for a run every 15 but we have finished all our computation in 8 seconds. So this is what you want to see. Another way is your process rate should always be greater than input rate. That's a sign that you have a healthy spark streaming job running.

This code is ultra reusable. You can take out parts of it and basically do your own benchmark. is totally modular but let me walk you through the code quickly. We have some input parameters which I have specified. I'm doing a scale test. I gave one number of parameters together and made it a config. So I'm saying run 100 streams. Do 500 rows a second of input. Try to do it every 15 seconds.

I'm using DLB data gen which actually helps us create fake data. And here I'm creating IoT data. And basically all this code what it does it just helps me create like a data set at the scale which I want. And then I just have a loop and this loop is basically helping me create like 100 streams and here here is where processing time comes in. Here I have set it 15 seconds.

So if your business requirement is hey we want data every 5 minutes spark is really good at throughput it can do higher throughput very easily. So if your requirement is get me data every 5 minutes, then there's no point producing data every 15 seconds, right? Because every time you produce a file, you're going to commit that file. You're going to write that transaction in the delta log and then essentially you'll have collected data only for uh say 15 seconds.

What is the cost of RG2 extra large instance? Okay. So it's 40 cents an hour. So if you think about it, essentially you can process 100 streams or 100 tables. let's say in less than $24 a day like I'm including both data bricks and AWS course without discounts everything right so being able to process 100 tables at less than $24 a day is like super amazing in my head okay.

---





---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
