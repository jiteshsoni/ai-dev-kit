---
name: how-you-can-turn-your-notes-into-a-personal-knowledge-agent-
description: ### Full Transcript

Has this ever happened to you where someone asks you a question and you're like, "Yeah, I've solved that problem before." Or, "I ...
---

# How you can turn your notes into a personal Knowledge Agent — no code required

## Overview
### Full Transcript

Has this ever happened to you where someone asks you a question and you're like, "Yeah, I've solved that problem before." Or, "I saw something on YouTube where I saw someone solve the problem." And then you spend 10, 15 minutes trying to find what you already knew at some point.

So, in this video, we are going to solve the recall problem, the system which I have been using for the last few months. I'm going to show you how you can build your own person knowledge agent which will help you retrieve your own thinking without code and almost no cost and it's going to run on your laptop.

So the real problem here is knowledge fragmentation. You have lot of transcripts, blogs, things which you watch and then people ask you a question like for me it's a lot of spark question

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2026-01-11
- **URL:** https://www.youtube.com/watch?v=TW2UycVhrgw

## Tags
ai, spark

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** TW2UycVhrgw

### Summary

### Full Transcript

Has this ever happened to you where someone asks you a question and you're like, "Yeah, I've solved that problem before." Or, "I saw something on YouTube where I saw someone solve the problem." And then you spend 10, 15 minutes trying to find what you already knew at some point.

So, in this video, we are going to solve the recall problem, the system which I have been using for the last few months. I'm going to show you how you can build your own person knowledge agent which will help you retrieve your own thinking without code and almost no cost and it's going to run on your laptop.

So the real problem here is knowledge fragmentation. You have lot of transcripts, blogs, things which you watch and then people ask you a question like for me it's a lot of spark questions and it takes me minutes to remember where I have written about it.

So when people ask a question to you, they are not looking for a response which they'll get from GPT. They're not looking a response which you'll find from Gemini. The question is coming to you because you have some specific context which chat GP does not have. Right? So that's why the question is coming to you.

So, that's when I discovered obsidian. So, Obsidian is a note takingaking tool. So, if you're thinking one note uh notion, it can replace both of them. It's free. The storage format is marked down. So, your files are actually yours and you don't have to pay anyone.

So what I ended up doing was that I had a realization that if obsidian can take nodes and I remembered that cursor actually indexes everything. So if you have a coding tool like cursor on your machine, cursor indexes everything on the workspace. So what I did was I pointed obsidian and cursor to the same workspace.

So now uh when I ask a question to cursor it replies like me because it's fed by all the content all of my content and also the videos and stuff which I consume. So that's why the response is very much like me and how I would respond.

Now just a difference I'm solving the recall problem. Right? So I'm trying to recall whatever I knew. So questions I have seen before, problems I have solved, summarizing my own thinking, that's 80% of where my time is spent anyway.

This is what Obsidian looks like. There are lot of extensions here but this is a blog which you see on screen. This is from my team itself. So this is where all the blogs are going and then I can basically open anything up and look at what's happening.

Now let's get into how am I using it. So this is cursor. If you're using something like cursor, the same technique should work. So again and cursor are pointing to the same workspace. That's what I have done. That's the trick.

So this is an example. So basically I click new agent and then I can ask it a question. And here you can choose the agent. I find Gemini to be good most days. Sometimes I would go to chat GPT. So I'll ask this a question right say give me top six ways to save money in Spark streaming job and let me say without adding more.

So what it's going to do is going to go through my files and figure out which ones are relevant. Right? So basically it's going to read find the relevant block. So it's basically looking at here. It's trying to find it and then it's going to basically read across the blogs and try to come up with answers.

You can run multiple streams on one cluster. Yes, I actually did a blog where I ran 100 streaming jobs on 8 core machine. That's like 100 streams are running in less than $20 a day. That's how cheap. A small thing which you can do is increase trigger interval.

So this is a good thing which happens. This one is from article from a colleague of from my team and I actually did not read this article and I did not find its relevance till I asked that question. So that's the beauty right? If I subscribe to highquality content, I'll end up with high quality results.

---





---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
