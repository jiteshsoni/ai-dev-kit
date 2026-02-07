---
name: how-spark-structured-streaming-recovers-after-failures
description: **Spark Structured Streaming Recovery Mechanism**

1. **Checkpoint System**: 
   - Checkpoints include metadata (query ID), offsets, and commits to tr...
---

# How Spark Structured Streaming Recovers After Failures

## Overview
**Spark Structured Streaming Recovery Mechanism**

1. **Checkpoint System**: 
   - Checkpoints include metadata (query ID), offsets, and commits to track job progress.
   - Metadata identifies specific jobs; offsets record the current processing point; commits confirm batch completion.

2. **Normal Operation Flow**:
   - Upon restarting, Spark checks for the latest offset file.
   - If found, it verifies the commit file's existence to ensure the entire batch was processed.
   - If confirmed, it writes the next batch; otherwise, it skips duplicates by checking with query ID and epoch ID.

3. **Post-Failure Handling**:
   - If a crash occurs (e.g., node loss or driver failure), Spark cannot find the commit file for the current offset.
   - It attempts to write directly to the delta table if 

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2025-11-29
- **URL:** https://www.youtube.com/watch?v=Piw5dujnsqU

## Tags
deltalake, spark, ai, checkpoints, streaming

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** Piw5dujnsqU

### Summary

**Spark Structured Streaming Recovery Mechanism**

1. **Checkpoint System**: 
   - Checkpoints include metadata (query ID), offsets, and commits to track job progress.
   - Metadata identifies specific jobs; offsets record the current processing point; commits confirm batch completion.

2. **Normal Operation Flow**:
   - Upon restarting, Spark checks for the latest offset file.
   - If found, it verifies the commit file's existence to ensure the entire batch was processed.
   - If confirmed, it writes the next batch; otherwise, it skips duplicates by checking with query ID and epoch ID.

3. **Post-Failure Handling**:
   - If a crash occurs (e.g., node loss or driver failure), Spark cannot find the commit file for the current offset.
   - It attempts to write directly to the delta table if possible.
   - Uses query ID and epoch ID to check if data already exists, preventing duplicates.

4. **Post-Recovery Process**:
   - After recovery, Spark starts from the latest offset by checking the offsets folder and commit files.
   - Ensures smooth restart without missing processing steps or duplicate writes.

### Full Transcript

Hey everyone. So here's a question. What happens when your Spark streaming job crashes in the middle of processing a batch? Why does it not lose the data? Does it write everything twice? How does it not create duplicates? And more importantly, what's the recovery mechanism? Today we are going to dig deep into how structured streaming actually works.

Let me set the stage for everyone. What does the checkpoint look like? If you open any structured streaming job, you'll find a checkpoint and inside the checkpoint folder, you'll find four to five folders. For the purposes of today's discussion, we only need to talk about the metadata folder, the offsets folder, and the commits folder.

Okay, so first things first, metadata folder stores the query ID. This is your Spark streaming query ID. You can find it on spark streaming UI in your notebook and inside this metadata folder. And then you have to worry about two folders offsets folder and the commits folder. Let me explain what happens in a normal scenario.

When you restart any structured streaming job, it checks the offset folder and tries to find the latest offset file. And these are numbered files. So 012 till infinity, right? So let's say it finds the offset end file. Once it finds the offset end file then it goes and checks out the commits folder and then it checks hey does the commit/ file exist. If it exists then it knows that the batch n completed successfully and then it can go ahead creates the offset n +1 file.

So think as creating offset n+1 file as the intent that's it's going to process the next batch once it writes the commit n +1 file that shows that it's completed. So offset is the intent and commit shows that hey I finished processing the n plus1 file. This is the how Spark streaming work. It's very simple to understand, right?

Now let's talk about more complicated scenario. Say you killed your structured streaming job. Say you killed the driver, you lost a node, whatever happened, a crash happened in your cluster died. Let's talk about the scenario when that happened. So let's say you have you started processing offset n plus1. Again intent. This is the intent. We are trying to process these offsets. We wrote the output to the target table and the cluster died before we were able to write commit n plus1 file.

How does spark streaming then avoid duplicates when writing to the target delta? In our scenario, when we try to write into a delta table structured streaming is going to figure out what is my query ID from the metadata file. It's going to figure out what's my epoch ID. Think of epoch ID, batch ID as the same thing. And then it's going to check the delta log or the delta table. Has this combination been written before. If the answer is yes, then it's just going to skip writing because it's going to realize, hey, I have already processed this batch ID for this structured streaming job.

So that's the magic on how it avoids getting duplicates into the delta table. Let's talk about the end to-end scenario. Say you had a failure and restarted your structured streaming job. This job restart on the same cluster, another cluster, whatever, doesn't matter. When we restart the job, it's going to find that n +1 offset was the last offset written. Does commit n plus1 file exist? If it exists, this is our happy scenario. We move on to n plus2 offset.

If it does not exist, attempt write to delta table. And then we go and check is this structured streaming query ID and batch ID/ epoch id has it already been written. If it's already being written then skip because the data is already there and if no then write the data into ta table and then our life continues. It's that simple.

Another thing I wanted to talk about just some small detail is the offset folder actually contains information about till which offset it's trying to process. Example say let's say in our offset n we wrote start offset as 100 and end offset as 200. So when we process the data the end offset is exclusive. So we are going to process 100 to 199 and when the next batch run say n plus2 batch runs we're going to process from 200 to 299 assuming we are processing 100 offsets. Just a small level of detail that the front part is inclusive and the end part is exclusive.

---






---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
