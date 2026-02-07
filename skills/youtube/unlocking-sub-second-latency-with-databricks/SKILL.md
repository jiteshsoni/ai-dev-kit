---
name: unlocking-sub-second-latency-with-databricks
description: ### Full Transcript

So, I'm tired of hearing Spark is good, but not for low latency. And honestly, it's been true for a while, but not anymore. So, S...
---

# Unlocking Sub-Second Latency with Databricks

## Overview
### Full Transcript

So, I'm tired of hearing Spark is good, but not for low latency. And honestly, it's been true for a while, but not anymore. So, Spark is entering subsecond territory now with realtime mode. And this year, I'm going to do a series of blogs where we are going to try Spark streaming realtime mode in a couple of scenarios.

Realtime mode makes sense in scenarios then latency is really important like fraud detection is one example where latency is really important. personalized offers. Someone was shopping in your app and they quit the app and went outside and now you immediately want to send them an offer or send them a reminder.

So these are examples where there is a cost associated like there is a revenue cost if you respond slowly like fraud detection or the other exam

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2026-01-22
- **URL:** https://www.youtube.com/watch?v=w6R0aUlZaAQ

## Tags
databricks, performance, streaming, spark

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** w6R0aUlZaAQ

### Summary

### Full Transcript

So, I'm tired of hearing Spark is good, but not for low latency. And honestly, it's been true for a while, but not anymore. So, Spark is entering subsecond territory now with realtime mode. And this year, I'm going to do a series of blogs where we are going to try Spark streaming realtime mode in a couple of scenarios.

Realtime mode makes sense in scenarios then latency is really important like fraud detection is one example where latency is really important. personalized offers. Someone was shopping in your app and they quit the app and went outside and now you immediately want to send them an offer or send them a reminder.

So these are examples where there is a cost associated like there is a revenue cost if you respond slowly like fraud detection or the other example is opportunity loss. If someone quit the shopping app and they might not come back ever, you lose the opportunity to make money. That's where latency really matters.

So for today's video, I dumped the whole Ethereum blockchain into Kafka and that's about 112 gigs of data and 23 million messages. I loaded that across four partitions and we are going to process it and see what latency we get.

I did not have a Kafka cluster and I know not everyone has a Kafka cluster handy to play with. So, I found Red Panda. Red Panda gives you $100 in credit for 14 days. You can go check them out. That's what I use for this example.

If you're saying, "Hey, Jes. I'm in the second range." In that scenario, microbatch board is your friend. If you can afford one or two seconds then you can go with regular microbatch mode because that would be more cost effective.

So about a year ago actually did a benchmark of reading data from Kafka and writing to Delta. I found that LakeFlow pipelines formally called DT was able to process data in 1.5 seconds. This was done at quite a scale. So I actually processed like 100 MBs a second when I did this experiment.

So let's get into what we are building today we have about 100 gigs of data sitting in a Kafka topic across four partitions we're going to process it we are going to write it to another topic my source has four partitions my target has four partitions.

We're going to do two things when we read these messages we are going to check hygiene so we're going to detect PI data we are going to check some patterns in this data and the other is we're going to do some data quality checks.

This is just connectivity. This is all the code for connecting to read data from Kafka. I wrote some binary data. I'm going to read it back. So, I'm just specifying my schema. This is the rules I'm applying. I'm applying a bunch of reax functions to my input data.

And this is the part where I'm going to read data. So, I'm going to do spark readstream specify kafka. And here I'm going to specify all the configurations. I'm going to start from the first messages on the Kafka cluster.

We have this special function called display function. So display function can actually display any kind of data frame including streaming data frames. So this is beautiful because I want to apply transformations when I'm building but also check what the transformations are doing.

A lot of people don't know about display function. Definitely use that. It really helps. And here the beauty of this display function is I'm not specifying the checkpoint location. the trigger interval nothing and I'm able to see what my data is looking like.

Now, this is writing data back to Kafka in a realtime fashion. And when you run this code, you have to set a parameter on your notebook. I'll use a specific cluster for this. If you like the output of DF enriched like I showed before, then what you need to do is just set the trigger and inside trigger you set realtime mode.

The default is five. I just set one for a specific reason. But the only difference between realtime mode and microbatch mode is controlled here. So if I run this using trigger dotprocessing time 1 second it's going to try to run micro batches which are 1 second long and if I do trigger realtime it runs in real time.

So that's the only difference right so that's the beauty of it which is I can run my whole application normally right my code doesn't change but in the literally in the last step in the right step I can say hey I want realtime mode can you run it as fast as possible and the rest of the code remains the same.

So if you look at Spark UI, what you are going to see is that my job has been running for some time and has processed eight batches. You can see 01 28. So it has processed nine batches and each of them the duration is a mirror.

And as long as my process rate is greater than my input rate I considered that as a good stream. Looks like you see the step which has happened. I think the reason it has happened is because I have processed everything. So I've been able to process my whole backlog. So I had about 23 million messages, right?

Another thing you must be thinking tell us about latency. So that's another cool thing. The driver logs measure the latency and let us know what the latency is. So I'm going to click driver logs and download this log 4j.

If you look at spark streaming made progress here you will find all the data. This is showing you your name of the job. This was the name I provided to my job. It's saying I processed at 69,000 rows a second.

What's special about real time works is that it also stores the latency. So it's saying that the P99 latency for this particular job was 1 millisecond. This is the latency which spark streaming added on their side. This is the time with Spark streaming took to process it.

In real time mode what happens is we actually run bigger batches. Default is 5 minutes. In my example I seted a minute. So every 1 minute spark tries to schedule and start running it. The difference is here the badge gets allocated the hardware and the codes in advance. So there i



---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
