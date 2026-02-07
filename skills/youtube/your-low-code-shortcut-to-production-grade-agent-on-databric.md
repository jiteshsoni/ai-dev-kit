---
name: your-low-code-shortcut-to-production-grade-agent-on-databric
description: **Obsidian-Ready Markdown Note: Building a Production-Grade Agent on Databricks**
---

# Your Low-Code Shortcut to Production-Grade Agent on Databricks

## Overview
**Obsidian-Ready Markdown Note: Building a Production-Grade Agent on Databricks**

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2025-11-19
- **URL:** https://www.youtube.com/watch?v=Ixi7jvIziNw

## Tags
databricks, ai

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** Ixi7jvIziNw

### Summary

**Obsidian-Ready Markdown Note: Building a Production-Grade Agent on Databricks**

#### **TL;DR**
1. **Agent Bricks Overview**: A low-code platform enabling rapid agent development on Databricks, ideal for embedding knowledge sources and querying via embeddings.
2. **Evaluation Process**: Utilizes LLM judges and human reviewers to assess agent performance iteratively, ensuring improvements without regression.
3. **Use Cases**: Ideal for scenarios like customer support, document search, and specialized knowledge extraction.

#### **Key Ideas**
- **Agent Bricks Functionality**:
  - Enables the creation of agents that interact with data sources and apply LLM models to provide answers or generate content.
  - Supports multiple agent types (e.g., information extraction, knowledge assistant) tailored for various applications like document search or FAQs.

- **Vector Search Integration**: Utilizes embeddings to optimize document retrieval, enhancing query efficiency and relevance.

- **Iterative Improvement**:
  - **Evaluation Metrics**: Incorporates user feedback through judges and reviews to refine agent performance.
  - **Feedback Loops**: Encourages continuous testing and adaptation based on real-world outcomes and community reviews.

#### **Action Items**
1. **Implementation Guidance**: Start with the knowledge assistant agent for streamlined document integration, such as FAQs or taxonomies.
2. **Evaluation Setup**: Implement a feedback mechanism using judges to assess response quality iteratively.
3. **Monitoring & Improvement**: Track performance metrics and user feedback to ensure agent efficacy and adaptability.

### Full Transcript

If someone was supposed to follow and learn the best practices from you, they could build their agent on datab bricks, the production ready agent, right? Come up with version two, come up with version three, but be able to measure that it's improving and not regressive. Exactly. We can walk through production monitoring and just making sure the agent is improving constantly.

My name is Vina. I am a specialist here at data bicks in ML and AI. I've been at data bricks for about a year and a half now and largely I've been working on agents and LM and fine-tuning etc as you would have expected. And why are we doing this special series and not a standalone YouTube video? Yeah, the cool thing about agents and LMs generally is that we want to constantly improve them and evaluating and feedback becomes a little bit more complicated. So I think the path to improvement is a little bit more unclear. So we're setting up this series so we can follow how one we can build an agent and two we can continuously evaluate an agent and then three hopefully improve that agent over time.

How do you want to start? Yeah, this all started with a hackathon Jes and I were a part of a couple weeks ago. Both Jesh and I write frequently for this blog called data bricksters.com and we write about really specific problems that we've faced with customers and otherwise. We found that a lot of these blogs become really helpful internally to solving other problems or even tangentially solving other problems. So, we wanted to build a really straightforward rag agent that was able to answer people's questions based off of the blog post that we've written and a lot of YouTube videos. So, extracting all that information so it's quickly accessible. So, we started with agent bricks.

So for those of you who do not know, Agent Bricks is a new low code solution on data bricks that will allow you to essentially just drop in your data, not really worry about an LLM specifically and worry about your use case and optimize for your use case. So let's start. I'll first walk you through what a knowledge assistant agent is. This is what we use to build our agent Brick brain. We are able to like drop in our knowledge source really quickly here. So there's multiple different types of accepted files here. If you have PDFs, work files, etc., you can just drop that into a UC volume and allow the agent to extract that.

You can also use your own vector index that you have. That's what we did. So, we took an index that we already have because we have a lot of audio files that we wanted to transcribe as well. So, now that we have loaded this index, we can specify different columns. For example, here I'm going to specify the URL of this column and the text here. and start creating this agent. This creates a knowledge assistant that will allow us to interact with our data in a straightforward manner.

What's the difference between a agent and a LLM? Yeah. So, an agent is something that interacts with an LLM. So, an LLM you obviously like people have used GBT. It's a model that interacts with your child interface. But what an agent can do is allow the LLM to interact with your document sources, like different tools that you may have, stuff like that to give the agent or give your LLM power to answer questions. For example, like you're thinking about a brain, right? An agent may be your entire system, but the LLM is the brain, right? You need still like tools like your hands to move stuff around. That's what we're trying to build here, like an entire human.

We're trying to build a second brain. And if you go back to agent bricks, there are a lot of agents. Could you briefly touch on what kind of agents are available on datab bricks? So we have a lot of agents like you mentioned the information extraction agent. This one is able to convert a lot of text documents in a batch fashion to like different extracting key categories. For example, if you have like tax documentation, maybe like legal agreements and you want specific names extracted, you should be able to do that with the information extraction agent.

That's one. Two, the knowledge assistant agent, which is basically RAD. We also have a few other agents, including like Genie, a multi-agent supervisor, which allows you 



---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
