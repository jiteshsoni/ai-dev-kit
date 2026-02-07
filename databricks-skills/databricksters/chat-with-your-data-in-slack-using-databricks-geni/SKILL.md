---
name: chat-with-your-data-in-slack-using-databricks-geni
description: 
---

# 💬 Chat with Your Data in Slack Using Databricks Genie – Part I

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/chat-with-your-data-in-slack-using

## Tags
performance, databricks, ai

## Full Content

# 💬 Chat with Your Data in Slack Using Databricks Genie – Part I

**Source:** https://www.databricksters.com/p/chat-with-your-data-in-slack-using

---



💬 Chat with Your Data in Slack Using Databricks Genie – Part I

Databricksters

Subscribe

Sign in

💬 Chat with Your Data in Slack Using Databricks Genie – Part I

Ambarish

Jun 23, 2025

3

1

Share

In today’s fast-paced work environments, Slack has become the go-to for team communication. But what if you could go beyond chatting with colleagues—and start chatting with your 

data

 directly from Slack?

Thanks to the integration between 

Slack

 and 

Databricks AIBI Genie

, that’s now possible. Imagine asking a question like “What were our top 5 products last quarter?” and getting not just an answer, but a clear explanation of how that answer was derived—right within Slack.

Thanks for reading Databricksters! Subscribe for free to receive new posts and support my work.

Subscribe

🤖 How It Works

When you send a message to the Slack bot, it routes your question to an 

AIBI Genie space

. Genie uses your organization’s business context to convert your natural language query into a meaningful SQL query, runs it, and then responds with:

📊 The answer to your question

🧠 The thought process behind the answer

🔍 Additional insights you might find useful

It’s like having a data analyst on-call inside your chat window.

🔧 Architecture Overview

This integration leverages several AWS services to make the magic happen:

API Gateway

: Acts as the interface between Slack and AWS Lambda.

Lambda

: Handles incoming Slack messages, forwards them to Genie, processes the response, and posts back to Slack.

DynamoDB

: Tracks conversations and user context to ensure reliable, contextual responses.

🛠️ Setup Steps

Here’s a high-level walkthrough of how to build this integration:

Set up AIBI Genie in Databricks

 Prepare your Genie Space using your datasets. Follow 

https://docs.databricks.com/aws/en/genie/set-up

 guide for setup instructions.

Configure the Slack Bot in your Workspace

 Build a Slack app that listens for messages and sends them to AWS Lambda. See Slack + API Gateway integration guide 

https://api.slack.com/quickstart

.

Create an API Gateway with Lambda Integration

 Set up a REST API in AWS with a Lambda proxy backend. Follow 

https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-create-api-as-simple-proxy-for-lambda.html

 tutorial.

Develop the Lambda Function

 The Lambda function receives Slack messages, communicates with Genie’s Conversational API, parses responses, and sends them back to Slack. Explore Genie Conversational APIs at 

https://docs.databricks.com/aws/en/genie/conversation-api

. 

Use DynamoDB for State Management

 Store message IDs and user tokens to manage context and prevent duplicates.

🎥 In Action

Check out a demo

📘 What’s Next?

This is 

Part I

 of our blog series on integrating Slack with Databricks AIBI Genie. In the next post, we’ll dive deeper into code snippets, error handling, and performance considerations.

Stay tuned—and start chatting with your data today!

UPDATE - Visit my new blog to learn how to connect Slack with Genie using Databricks Apps in 30 minutes with code!

Thanks for reading Databricksters! Subscribe for free to receive new posts and support my work.

Subscribe

3

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
