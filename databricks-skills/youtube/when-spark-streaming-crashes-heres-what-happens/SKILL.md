---
name: when-spark-streaming-crashes-heres-what-happens
description: #
---

# When Spark Streaming Crashes, Here's What Happens

## Overview
#

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2025-10-03
- **URL:** https://www.youtube.com/watch?v=uPq0mRQIO7A

## Tags
streaming, spark

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** uPq0mRQIO7A

### Summary

#### **TL;DR**
- Spark streaming jobs recover without losing data due to checkpoints.
- Checkpoints contain metadata (query ID), offsets, and commits folders.
- Each folder has a specific role in data recovery and processing continuity.

#### **Key Ideas**
- **Checkpoints**: Stores metadata (e.g., query ID) and ensures data isn't lost on crashes.
- **Metadata Folder**: Holds the Spark streaming query ID for tracking.
- **Offsets Folder**: Manages session offsets, preventing duplicate writes.
- **Commits Folder**: Tracks commits to determine recovery progress.

### Full Transcript

What happens when your Spark streaming job crashes in the middle of processing a batch? Why does it not lose the data? Does it write everything twice? How does it not create duplicates? And more importantly, what's the recovery mechanism? Today we are going to dig deep into how structured streaming actually works.

Let me set the stage for everyone. What does the checkpoint look like? If you open any structured streaming job, you'll find a checkpoint. And inside the checkpoint folder, you will find four to five folders. For the purposes of today's discussion, we only need to talk about the metadata folder, the offsets folder, and the commits folder.

Okay, so first things first, metadata folder stores the query ID. This is your Spark streaming query ID. You can find it on Spark streaming UI in your notebook and inside this metadata folder. And then you have to worry about two folders offsets folder and the commits folder. Let me explain what happens.

---





---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
