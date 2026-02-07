---
name: how-liquid-dv-rlc-help-improve-streaming-merges-and-p99-late
description: #
---

# How Liquid, DV & RLC help improve Streaming Merges and P99 Latency

## Overview
#

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2025-11-29
- **URL:** https://www.youtube.com/watch?v=miM1B-D8Eaw

## Tags
performance, streaming

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** miM1B-D8Eaw

### Summary

#### **TL;DR**
- Improved streaming performance through parallel execution of merge and optimize operations.  
- Reduced P99 latency by eliminating waiting periods between operations.

#### **Key Ideas**
1. **Traditional Method Limitations**:  
   - Stream jobs paused periodically to run optimization, causing higher latency due to no new data processing during pauses.
   
2. **Liquid Clustering + Role-Level Concurrency**:  
   - Enables parallel execution of merge and optimize operations without conflicts.
   
3. **Deletion Vectors**:  
   - Efficiently manage updates using binary files (bit maps) to track active rows in delta tables, supporting soft deletes.

4. **Row-Level Tracking**:  
   - Each row in the delta table is assigned a unique ID and version to prevent conflicts during concurrent operations.

### Full Transcript

Hey folks, in today's video we're going to talk about how liquid clustering makes streaming versus simpler and also improve the P99 latency. For those who are new here, I'm Sony, the Canadian data guy. Have spent over 10 years at Amazon and datab bricks. And now I work as a specialist architect. Each week me and my team, we publish a deep technical blog without any marketing fluff and just engineering insights and what we are seeing on the field.

Prior to liquid clustering and role level concurrency when you are doing merges in a streaming fashion every day what you would do is you'll have a streaming job running uh every patch you'll make a for each patch call. inside that for each batch call you run the merge operation and after every n batches you have to pause your stream and actually run optimize. Now why would you have to run optimize while pausing? That's because optimize and the streaming merge would conflict with each other.

The other reason you might want to run optimize in parallel was if you want merges to be very fast the data needs to be clustered in a way. So that those were the two reasons why people would do this. Now with liquid clustering and role level concurrency, thankfully you don't have to do this. So essentially your streaming jobs can keep opening up for each batches and inside that for each batch you can go and merge into your delta table and in parallel you can leave it to predictive optimization to figure out when it's time to run optimize on your table and these two operations are independent of each other and that's the benefit which you're getting.

So if you were running optimize after every 100 batches essentially the time the optimize needed to run the latency would go higher right because no new data is arriving so that's why your P99 would go high and now because now they can be done in parallel that's why your P99 would go lower the other benefit you're going to have is that your code would become simpler because previously if you had a streaming job you'll write the merge followed when optimize.

How does this get achieved? So, let's get into that. We'll have to cover two concepts here. One is deletion vectors and the other concept is role level concurrency. And once we combine both of them together, you will see the benefit here. Okay. So, the first concept we're going to talk about is deletion vectors. So, how does deletion vectors work behind the scenes? Deletion vectors basically creates a binary file. So if you have a delta table, you try to update a few rows. Say you're trying to delete some rows. Now it could be a little delete or an update. No matter what the case, someone needs to remember that this file has been updated. And because S3 and ADLS does not allow update operations, any object written on it is immutable. So a new file has to be written and the new file has to be very light.

So this new file which is written is the deletion vector which is binary behind the scenes and it's a bit map. So essentially imagine you have a park file and the rows written in that park file. What we are going to do is when we write our deletion vector aka uh binary which stores the bit map inside it the bit map is going to remember which of these rows are active. So when you read back deletion vector file and park file together, you get to know which rows are active. That's the magic here. That's how things are in action.

It's a three-step process. You can read about it. This is called merge on read. It can be also referred as soft deletes. So essentially the reader who is reading your delta table, they know how to read a delta table. So you don't need to know all the nuances which always as a good data engineer you need to see through the noise right if someone says this feature exists then question should be hey how does the feature exist.

So this is how deletes or updates are achieved using deletion vectors. The second thing which we need is road tracking. So in a row tracking feature every row inside your delta table gets a unique row ID and row commit version. So if you combine both of these concepts together you will see the benefit.

So before row level concurrency what would happen? You can imagine there's a file a with four rows and deletion vectors is all zero meaning all rows are active. Now if you delete one row essentially in the deletion vector there could be a change and you mark in the deletion vector as that row three has been deleted. So that's just soft delete or deletion vectors in action.

Now next step suppose if you have two transactions trying to update different rows. So in this example say T2 transaction trying to update row zero without rowle concurrency what would happen is if T1 has gone through then T2 who is trying to update the same file would get a conflict it would be rejected but if you add the rowle concurrency features which basically has the row ID and row commit version it would recognize that T1 has committed and T2 when it's trying to commit the file it will See something happened to this file. However, it impacted a different row. That's why 



---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
