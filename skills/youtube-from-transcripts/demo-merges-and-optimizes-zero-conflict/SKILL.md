---
name: demo-merges-and-optimizes-zero-conflict
description: **Video Title:** Demo: Merges and Optimizes, Zero Conflict
---

# Demo: Merges and Optimizes, Zero Conflict

## Overview
**Video Title:** Demo: Merges and Optimizes, Zero Conflict

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2025-09-16
- **URL:** https://www.youtube.com/watch?v=nFL8sz_B3Xc

## Tags
data-engineering

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** nFL8sz_B3Xc

### Summary

**Video Title:** Demo: Merges and Optimizes, Zero Conflict

#### TL;DR
- The demo successfully tested merges and optimizations in parallel without conflicts.
- Runs for 32 hours (7 days) showed no issues with data integrity during testing.

#### Key Ideas
- **Streaming Job Performance:** A streaming DataFrame generates 1,000 rows per second with a 50% update rate and 50% inserts. Merge rates are configurable.
- **Merge Implementation:** Runs merges in parallel alongside optimizations without conflicts.
- **Optimization Strategy:** Uses deletion vectors for role-level currency to simplify code.
- **Conflict-Free Operation:** Tested under forced optimization intervals, no conflicts detected.

### Full Transcript

Now all this is the theory but I like to see things in action. So I created a demo to prove that this works. So I have been running a job which is doing merge and optimize in parallel to prove this out. And I ran this experiment for 32 hours which is roughly 7 days for the purposes of this video and my demo.

So what's happening right now is we have a streaming job. This is my streaming data frame which is creating,000 rows a second. It has 50% updates. 50% inserts sorry 10% update. So that's the ratio. This is configurable so don't worry too much. And what happens is you have a streaming data frame and then you call a for each badge and inside that for each badge I'm basically running my merge.

So if you go here you will see that I'm running a merge on the delta table and then in the prior world after every n batches I'll write the if condition here which will say hey after every n batch ids let's run an optimize but now that I'm using deletion vectors role level currency and stuff like that I don't need to do it my life is much simpler so technically your code would stop here that's it like you just need to call a for each patch and run the merge and then you'll have streaming merges.

I'm running like 500 optimize in sequence and then I'm running it at random intervals. Basically I'm trying to force a conflict to happen but as you can see it's been like we have had like 46 optimize run and yeah it's running fine. Basically the optimize and the merge are not conflicting with each other. So the theory is matching the practicality and yeah this is some code which is there to prove that this works when you are doing this and you actually can leave it to predictive optimization.

---





---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
