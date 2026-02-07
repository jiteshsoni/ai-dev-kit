---
name: speeding-up-spark-streaming-mastering-parallel-writes-in-for
description: #
---

# Speeding Up Spark Streaming: Mastering Parallel Writes in ForEachBatch

## Overview
#

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2025-03-31
- **URL:** https://www.youtube.com/watch?v=n9jodzYq1e4

## Tags
streaming, spark

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** n9jodzYq1e4

### Summary

#### **TL;DR**
- Achieve parallel writes within a `ForEachBatch` loop to optimize Spark streaming performance.  
- Use Python threads and the `concurrent.futures` library for concurrent processing.  
- Cache microbatches to reduce redundant operations and handle exceptions gracefully.

#### **Key Ideas**
1. **Use Python Threads:** Leverage threading to execute multiple write operations in parallel, reducing latency.
2. **Concurrent Futures Library:** Utilize this library to run functions concurrently within the `ForEachBatch` loop.
3. **Caching Microbatches:** Cache microbatch data to avoid redundant processing and streamline operations.
4. **Exception Handling:** Implement exception handling to prevent failures from disrupting parallel writes.

### Full Transcript

so we're going to talk about how to parallelize within a for each batch when you're doing spark streaming the reason usually people want to do this is they have to use for each batch and they want the rights to happen in par because they want lower latency than what they are getting example when you are in a for each patch and you're writing say four tables just an example you'll do that sequentially that table one's would finish and table two is Right would finish and table three and table four and so on right now because the these things are happening in parallel you are going to end up with higher latency.

So in this video we're going to talk about how can we achieve that so what are we going to use special is basically we're going to use Python threads to do it we're going to use the concurrent Futures library to achieve it so let's go through the code once so let me do that okay so in the stop in the start we have our bunch of imports we have our catalog database Target so this is where we are going to write to and then we have a source our source is Delta table you can choose something else doesn't matter and this is our streaming Source data frame.

Also one more option which you have and you have similar options in Kafka and Kinesis this you can control the size of the micro batch like how big is the size of the microbat can be controlled by a parameter called Max files per trigger the default is th000 I'm doing 500 for cfes called Max offsets per batch this basically controls the size of the micro batch going inside the for each batch called.

Then I have written a function which is going to write data into a Delta table and this particular function would be run in parallel so that's my example again you can write your own function doesn't matter but this is the function which we are going to run in bar that's all what you need to know so in this example I am getting this micro this input data frame and this input data frame I'm filtering by C certain things and then creating my target data frame which I want to write.

And then what we do is we create a for each batch class the reason I'm using a class is because I want to parameterize if you write a regular fun function you won't be able to parameterize but if you write a pythonic class then you can basically pass any values to your for each batch.

So here I'm just passing catalog database you can pass other variables too and then this is the function which you need to pay attention to this is called process microbatch again this is custom UDF you can write whatever you want what really matters is this function gets two inputs which is your microbatch data frame you can't change this by the way you get a micro batch data frame and the batch ID those are the only two inputs allowed.

What I'm doing is first I'm caching my microbatch the reason I'm doing it because I'm going to run different actions so I'm going to run four actions I'm doing four rights so in my example I'm doing I'm going to access this microbatch four times that's why it's best to cach it and then once you're done processing you should unach it or UNP processed it.

So here is where I call my function which is called process table let me show you the function called so here I'm saying create a thread pool and I'm only creating two threads at a time now you can change this number 4 8 16 cautious of it don't take it too far for my needs I set it at two because I wanted two tables to be written in parallel.

So what we do is we call this function call Process table and the function call looks a bit different it's not something which is dependent on P this is dependent on python itself so what we are doing is we are calling this function process table and then passing these parameters so I'm passing device type batch ID catalog database and the microbat data frame.

And then this is in a loop so when this code runs basically you'll have two threads being two tables two threads being created and two threads finishing at the same time or sorry two threads being initiated at the same time again if I do four then all four would get initiated and after this now I launch the thread now I need to wait for them to finish so which is this line so this will ensure that I wait for my threads to finish and give a response back.

Now some of these threads would fail they may fail which so what I'm doing is rather than failing the whole program I'm just collecting all my exceptions first and then if any of my threads fail then I fail the full micro batch which makes sense.

By the way if you don't know about query name parameter this is a good way to give your stream a human readable name I'm calling it parameterized for each patch but you can call it anything you like. If you look at it now that it's giving getting us a name these graphs is going to pop up and if I go to the catalog now you should see four tables.

Whenever by the way you're doing streaming just pay heed to this parameter number of files outstanding and bytes outstanding for any streaming job these numbers should continue to drop the reason is significant signifies that the speed at which the source data



---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
