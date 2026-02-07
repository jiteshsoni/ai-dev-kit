---
name: futureproof-your-data-engineering-skills-your-spark-streamin
description: ### Full Transcript

okay so thanks for joining in um if you don't know me my name is Jes we are going to have like a spark streaming master class on ...
---

# FutureProof Your Data Engineering Skills: Your Spark Streaming Journey Starts Here

## Overview
### Full Transcript

okay so thanks for joining in um if you don't know me my name is Jes we are going to have like a spark streaming master class on spark streaming today uh let me take you through the the agenda and before I go to the agenda if you can type in in the chat like how many years of experience with streaming do you have is this the first time you're trying streaming and um have you worked with Park before.

So this is the agenda for today so about me we're going to do a short intro on spark streaming then we are going to spend a majority of the time talking about doing like a live demo and the way I have designed this is there there's something for everyone so if you're new to spark there's something for you if you have been building spark streaming applications for 2 3 years

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2026-01-24
- **URL:** https://www.youtube.com/watch?v=9dH8d7SX6ek

## Tags
joins, streaming, spark

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** 9dH8d7SX6ek

### Summary

### Full Transcript

okay so thanks for joining in um if you don't know me my name is Jes we are going to have like a spark streaming master class on spark streaming today uh let me take you through the the agenda and before I go to the agenda if you can type in in the chat like how many years of experience with streaming do you have is this the first time you're trying streaming and um have you worked with Park before.

So this is the agenda for today so about me we're going to do a short intro on spark streaming then we are going to spend a majority of the time talking about doing like a live demo and the way I have designed this is there there's something for everyone so if you're new to spark there's something for you if you have been building spark streaming applications for 2 3 years 4 years there's still some in-depth content for you.

For the last four years at datab briak I I've been taking streaming workloads to production almost every week so lots of in-depth experience on streaming I'm trying to give you the content which I did not get when I was starting my streaming Journey.

decisions will be made in real time regardless of whether real time analytics is available or not I think that's that's just a very good summary like World Works in real time if you can work at the same speed good on you if you cannot you're going to just lose out I think that's the Crux of why you should be doing it.

So why are folks not able to do streaming the biggest reason is that it is complex uh there is a small learning curve involved and historically people used to have separate systems like four years ago um you would have like a realtime system which would be elastic search powered it could be uh spark powered for the batch and then uh there were two separate system which was one which was doing just pure Real Time stuff and then another system which was just doing pure bad stuff.

If you are working in a daily weekly monthly fashion you're probably into forecasting or doing basic reporting and basic analytics right um if you are at a early Cadence uh where you process data every hour say you are Amazon and you're trying to figure out how many orders came in the last one hour and then you're trying to make predictions on U how many how much uh how many employees to bring in to packets the stuff off.

Spark actually supports both of it so spark supports true real time there's a there is a spec spark streaming has a specific mode for that it's new it's still experimental uh but if you're thinking like seconds like 1 second 2 seconds 3 seconds or say between 1 to 10 seconds anything you spark stream has a Mode called micro patch mode which is the default mode as of now um that can easily operate in the near real time Paradigm.

So just for clarification I think if you're trying to design something for less than 800 milliseconds I think right now the best technology is Flink and if you get above 800 millisecond Benchmark um spark streaming just becomes your friend because it's just so much easier uh than Flink.

The first point honestly is my favorite which is you don't need to start learning a brand new paradigm the same apis work the same engine works uh so this unified API layer is what I really enjoy about spark streaming such that I don't have to learn two Frameworks at once um I can just I can just uh enhance my understanding of spark streaming and then I'm done.

One thing I told you a life without input parameters so we are saying hey let's process the data and uh let spark worry about what is there to process like spark specifically spark streaming will worry about what is there to process I don't have to worry about I don't have to tell it which days data has arrived so like in your traditional bad jobs you specify input parameters right like a date parameter or a country parameter in streaming you don't need to do that because streaming already takes care of that.

Apart from the fact that you don't have the time to learn about checkpoints you don't have the time to test your jobs and maybe hard this is a debatable thing which I'm going to say next if a lot of complex aggregations then maybe spark streaming is not the right tool for you but even that is debatable.

How is the spark job different from a spark streaming job um so spark job essentially you can think as three component there's a source there's a Transformations joins aggregations and then there is a target so Source transform and Target how is a spark streaming job different so spark streaming job has just one additional concept of checkpointing and then there is a caveat that not all sources have support for streaming.

Kafka is a distributed message engine if I can say that so in this example there are three nodes node one node 2 node 3 so node one node two node three which we are saying data is distributed across three different machines and within those machines that different partitions partition 1 to six so what we are saying is data is distributed across three machines across six partitions that's how data is distributed.

First things first which is for any job you need to have parameters so I have created a scope so I have to store kafka's Secrets somewhere so basically to connect to your Kafka cluster you'll need to have your servers address and then you'll need to have a secret key and a secret value and you need to store that inside data bricks uh such that it doesn't get exposed.

Look how simple it is to just start processing data so here is the code which is going to read data from Kafka pay attention to this there's only like three four lines here uh which you need to pay attention so this is just plain uh reading call which is here which we are giving the servers Kafka server details we're giving our username and password I'm using plain mode you can use a more secure mode of course.

Then I'm saying subscribing to that topic so I'm saying hey go read the topic 



---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
