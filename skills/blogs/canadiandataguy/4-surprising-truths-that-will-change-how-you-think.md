---
name: 4-surprising-truths-that-will-change-how-you-think
description: 
---

# 4 Surprising Truths That Will Change How You Think About Spark Streaming

## Overview


## Source
- **Author:** Canadian Data Guy
- **URL:** https://www.canadiandataguy.com/p/4-surprising-truths-that-will-change

## Tags
streaming, kafka, ai, spark, data-engineering, deltalake, databricks

## Full Content

# 4 Surprising Truths That Will Change How You Think About Spark Streaming

**Source:** https://www.canadiandataguy.com/p/4-surprising-truths-that-will-change

**Blog:** Canadian Data Guy

---



4 Surprising Truths That Will Change How You Think About Spark Streaming

Canadian Data Guy Unfiltered

Subscribe

Sign in

4 Surprising Truths That Will Change How You Think About Spark Streaming

Spark gives you Real-Time without the complexity and pain

Canadian Data Guy

Dec 15, 2025

1

Share

TL;DR

Spark now competes with Flink on real‑time: Real‑Time mode achieves double‑digit millisecond latency; think 20 ms 

One engine, one API: Batch, near‑real‑time, and true real‑time in the same Spark paradigm.

Simplicity at scale: Checkpointing, fault tolerance, exactly‑once semantics built in.

Real‑time without friction: No second system or new programming model required.

Four Counter‑Intuitive Truths

Real-time without a new paradigm

You don’t need a separate engine + a separate mental model. Same Spark APIs, same ecosystem, same team skillset.

Real-time to hourly, depending on the business need

Streaming doesn’t mean 24/7. Spark Streaming is incremental, not perpetual. Choose the schedule your business needs—continuous, 15‑minute, hourly, or weekly. triggerAvailableNow kicks off, processes all new data in a single efficient run, and exits. You get batch‑style cost control with streaming‑grade correctness.

Checkpointing changes the game operationally

Build batch with the streaming paradigm. Design batch pipelines as streaming from day one to avoid rewrites when SLAs tighten. Going from daily to every four hours can be a small code change, not an architectural overhaul. And you drop brittle input parameters (e.g., process_date): Spark tracks progress so engineers focus on logic, not bookkeeping. Streaming can be cheaper than batch Late or frequently updated data punishes batch. Checkpointing persists a “bookmark” so Spark processes only net‑new changes and skips what it has already seen. 

“checkpointing says you don’t worry about what’s net new I’ll identify what’s net new and only process that”

Latency is a business decision, not an ego metric

The learning curve is flatter than you think Spark’s unified API means you reuse the same DataFrame/Dataset logic for batch and streaming. The shift is incremental: same engine, same abstractions, different triggers and sinks. Teams extend what they know instead of adopting a second framework.

Conclusion 

If this blog has reshaped how you think about Spark Streaming, the YouTube session demonstrates 

how it actually works in practice

.

In the video, I go beyond concepts and walk through:

How Spark Structured Streaming achieves 

double-digit millisecond latency

 in real deployments

Kafka → Delta ingestion patterns that teams run in production

How checkpointing simplifies operations and reduces cost compared to batch reprocessing

When to use continuous mode vs triggerAvailableNow vs micro-batch — and why this is a 

business decision

, not a technical flex

This isn’t a theoretical take. It’s based on 

shipping streaming workloads to production week after week at Databricks

, dealing with real SLAs, real failures, and real cost pressure.

Watch the full walkthrough here:

👉 

Thanks for reading CanadianDataGuy’s No Fluff Newsletter! Subscribe for free to receive new posts and support my work.

Subscribe

1

Share

Previous

Next

Discussion about this post

Comments

Restacks

Top

Latest

Discussions

No posts

Ready for more?

Subscribe

© 2026 Canadian Data Guy

 · 

Privacy

 ∙ 

Terms

 ∙ 

Collection notice

 Start your Substack

Get the app

Substack
 is the home for great culture

 This site requires JavaScript to run correctly. Please 
turn on JavaScript
 or unblock scripts





---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
