---
name: mastering-checkpoints-in-spark-streaming-everything-you-need
description: #
---

# Mastering Checkpoints in Spark Streaming: Everything You Need to Know

## Overview
#

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2025-01-22
- **URL:** https://www.youtube.com/watch?v=21lcPXadHbc

## Tags
streaming, spark, checkpoints

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** 21lcPXadHbc

### Summary

#### **TL;DR**
- **Checkpoints in Spark Streaming**: Ensure data consistency by marking key points during processing.
- **Stateless vs. State Full Jobs**: 
  - Stateless: No stored state between batches (e.g., metadata, offsets).
  - State Full: Stores state like duplicates, watermarks, or batch details.
- **Watermark Policies**: Used to manage data retention and uniqueness in streams.

#### **Key Ideas**
1. **Purpose of Checkpoints**:
   - Mark key points for processing to ensure data consistency.
   - Store metadata (offsets) between batches but no state.
2. **Difference Between Stateless and State Full Jobs**:
   - Stateless: No stored state; only processes current batch without previous context.
   - State Full: Stores additional information like duplicates, watermarks, or batch details to manage data retention.
3. **Watermark Policies**:
   - Used in both Stateless and State Full jobs for managing data uniqueness.
   - Example: Watermark keys with timestamps (e.g., 1 minute) and remove duplicates within that timeframe.
4. **Monitoring Metrics**:
   - Important metrics include `Processing Rate > Input Rate` to ensure healthy job performance.
   - Track `Max offsets behind latest` to prevent lagging behind data production.

### Full Transcript

in today's video we are going to do a deep dive into the spark streaming checkpoint if you are planning to take any job to production is really important to understand the role of a checkpoint in a spark streaming job we're going to talk about three topics today we're going to do a comparison between stateless job versus the state full job and then we're going to finally talk about what is inside the state store.

If you list the checkpoint folder you'll find four to five folders inside it let's first look at the metad data folder so if you run this command which Stills FS head you'll be able to read the top part of your file and if you read this file you'll get a ID inside it this is your spark streaming ID when our streaming job is running you'll find this ID here if you go to your datab cluster or your spark cluster and you click the structured streaming tab you'll also find the ID here.

So what does a metadata folder tell us it's just telling us the spark streaming ID now let's look inside the sources folder so inside the sources folder we are only able to find one source in it so sources sl0 and if you do uh a listing of zero. zero folder you'll find uh a file which ends with 0 0 one question comes to mind is why do we have sources sl0 that's because you can have multiple streaming sources so you can have two sources for an example if you're doing a stream to stream join then you'll have two sources.

Now let's understand the offsets folder and the commit folder offset folder is the one which has the most amount of details. Anytime our spark streaming job runs it basically creates a new offset file right now the latest offset file is 600 you can sort this data on the modification Tim stamp we are seeing 600 comits have happened using this checkpoint and the latest commit is 600.

Before I show you what is inside the offsets file I want to also just quickly show you the commits file for every offset for every micro batch which is procet there's a offset file created and correspondingly a commit file is created when spark streaming starts reading data it writes the offset file after the data is committed a Comm commit file is created in our case commit 600 so this numbers would always match up.

If there's offset 600 there would be a corresponding commit 600 if there is offset 599 there would be a corresponding commit 599. What happens if you kill the job in withbe whereas the offset file is created but the commit file is not created that means that microbat did not Su success y finish and when the next time the spark streaming job runs it's going to figure out that his offsets and commits in sync and if they're not that means the last offset did not have a corresponding commit.

This does not have a concept of State meaning um the checkpoint is not storing much information it's just storing hey till what offset do I need to read and what have I written so far that's all it needs to remember. Now let's add the concept of state so now we going to create another job and this is going to have the concept of State the code is very similar with the previous example except this has two extra lines so we are adding a water marking policy where we are saying Watermark the keys with a time stamp of 1 minute and then we are saying drop duplicate within Watermark.

Now we're going to have three graphs previously remember we had the input rate the processing rate the batch duration now we'll have another graph which is the aggregated state Because by the way this this is an example of a state full job where there is a state involved and the previous was the example of a state less job where the checkpoint has no State.

If we list the checkpoint for this streaming query you will see five folder previously we saw four folders now we see this additional folder which has the state what is a state your checkpoint trying to remember something in our case we asked a checkpoint to remember this to not allow any duplicates on partition on offset column with a water mark of 1 minute meaning it's going to remember this um partition offset combination for a minute.

One thing you should take care of is the processing rate should always be greater than the input rate another thing which is even more important than the previous one is there's a metric over here which is going to be Max offsets behind latest so here is your metric Max offset behind latest this metric for all spark streaming jobs should either be consistent or always be dropping.

How do we read this data this gets us into the topic three for today's discussion which is how do you read the state folder so datab break has two apis on how you can query the sta



---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
