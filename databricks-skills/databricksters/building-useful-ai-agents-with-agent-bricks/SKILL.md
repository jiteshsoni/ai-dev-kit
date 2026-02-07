---
name: building-useful-ai-agents-with-agent-bricks
description: 
---

# Building Useful AI Agents with Agent Bricks

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/building-useful-ai-agents-with-agent

## Tags
streaming, mlops, ai, spark, performance, databricks

## Full Content

# Building Useful AI Agents with Agent Bricks

**Source:** https://www.databricksters.com/p/building-useful-ai-agents-with-agent

**Blog:** Databricks

---



Building Useful AI Agents with Agent Bricks

Databricksters

Subscribe

Sign in

Playback speed

×

Share post

Share post at current time

Share from 0:00

0:00

/

0:00

Transcript

8

Building Useful AI Agents with Agent Bricks

From docs to dependable answers: building a Databricks Knowledge Assistant with Agent Bricks

Canadian Data Guy

 and 

Veena

Jan 20, 2026

8

Share

Transcript

What is Agent Bricks? What is the Knowledge Assistant?

The Knowledge Assistant brick is one of several specialized agent types available in Databricks Agent Bricks, which is a low-code solution to help developers easily create and optimize domain specific agents with a few clicks.

The Knowledge Assistant brick is a no-code RAG agent that delivers reliable responses with citations. Instead of relying solely on what a language model learned during training, RAG returns relevant information from your documents in real time and is used to generate responses. The Knowledge Assistant brick uses optimizations out of the box, so you do not need to worry about optimizing the index or LLM. In our demo, we built a Knowledge Assistant for a team working with Databricks. The agent uses blog posts (written by our team, here on Databricksters) to answer questions about Spark streaming, coding practices, etc.

This same approach can work for any scenario where you have custom knowledge and want to create “general intelligence” on your data. Think of unstructured data that you already have-- HR policies, production information, or technical docs.

Prerequisites

Before building your own brick, ensure your workspace meets these requirements:

Mosaic AI Agent Bricks Preview (Beta) enabled

Production monitoring for MLflow (Beta) enabled

Serverless compute enabled

Unity Catalog enabled

Access to Mosaic AI model serving

Access to foundation models in Unity Catalog through 

system.ai

 schema

Serverless budget policy with non-zero budget

Workspace in a supported region

Data Requirements

Make sure you have one of the following:

Files in a Unity Catalog volume (supported formats: txt, pdf, md, ppt, docx)

A vector search index

The databricks-gte-large-en embedding model endpoint must have AI guardrails and rate limits disabled.

Adding feedback and improving Agent Quality through feedback

You can add feedback and assessments to each trace. This is where the human feedback begins-- you can start noting which responses are good, what needs improvement, etc. These assessments become a part of the MLflow experiment associated with the Agent Brick. You can aggregate this information using the MLflow API.

In my opinion, this is where Agent Bricks really shines. The Knowledge Assistant can automatically update itself according to the human feedback using a process called ‘Agent Learning from Human Feedback’. 

Learn more about this here. 

 This allows the agent to continuously improve based on expert expectations -- without you needing to retrain models or fiddle with prompts manually.

The feedback loop

The process is straightforward: (1) create your challenging questions, (2) have SMEs review the agent responses and add comments, and then (3) sync the feedback. “Syncing” will start the process to improve the agent based on the feedback.

How should you approach writing questions? Similar to creating an evaluation set, you should include a variety of questions that are critical for the use-case. Based on previous evaluations, you can include edge-case questions as well to see what SMEs want to see. Remember: you are only providing the questions, not the answers. The agent will generate the answers, and then the SMEs will provide the feedback on the answers.

Once you have your questions added, you can start a Labeling Session. Once that session is ready, share that link with SMEs and wait for the feedback to roll in. SMEs can add feedback on tone, style, or accuracy of that agent response. Make sure you communicate how to add feedback to your SMEs!

After ending a Labeling Session (after SMEs have added their feedback), you can start syncing the responses. Agent Bricks uses many techniques to sync the feedback with the agent. Congratulations! You have completed the first human feedback loop!

Discussion about this video

Comments

Restacks

Agents

How to build useful agents with AI

How to build useful agents with AI

Subscribe

Authors

Canadian Data Guy

Veena

Recent Posts

Your Low-Code Shortcut to Production-Grade Agent on Databricks

Nov 18, 2025

•

Canadian Data Guy
 and 
Veena

Ready for more?

Subscribe

© 2026 Soni

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
