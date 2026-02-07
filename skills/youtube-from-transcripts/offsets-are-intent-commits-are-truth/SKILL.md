---
name: offsets-are-intent-commits-are-truth
description: ### Full Transcript

When you restart any structured streaming job, it checks the offset folder and trying to find the latest offsets file. And these ...
---

# Offsets are intent. Commits are truth.

## Overview
### Full Transcript

When you restart any structured streaming job, it checks the offset folder and trying to find the latest offsets file. And these are numbered files. So 012 till infinity, right? So let's say it finds the offset end file. Once it finds the offset end file, then it goes and checks out the commits folder and then it checks, hey, does the commit/ file exist? If it exists then it knows that the batch n completed successfully and then it can go ahead creates the offset n plus1 file.

So think as creating offset n+1 file as the intent that's it's going to process the next batch. Once it writes the commit n +1 file that shows that it's completed. So offset is the intent and commit shows that hey I finished processing the n plus1 file. This is the how spark streaming works. Ver

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2026-01-27
- **URL:** https://www.youtube.com/watch?v=Mjvwx4qKQ8M

## Tags
streaming, spark

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** Mjvwx4qKQ8M

### Summary

### Full Transcript

When you restart any structured streaming job, it checks the offset folder and trying to find the latest offsets file. And these are numbered files. So 012 till infinity, right? So let's say it finds the offset end file. Once it finds the offset end file, then it goes and checks out the commits folder and then it checks, hey, does the commit/ file exist? If it exists then it knows that the batch n completed successfully and then it can go ahead creates the offset n plus1 file.

So think as creating offset n+1 file as the intent that's it's going to process the next batch. Once it writes the commit n +1 file that shows that it's completed. So offset is the intent and commit shows that hey I finished processing the n plus1 file. This is the how spark streaming works. Very simple to understand, right?

---





---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
