---
name: integrate-slack-with-genie-natively-using-databric
description: 
---

# Integrate Slack with Genie natively using Databricks Apps in 30mins!

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/integrate-slack-with-genie-natively

## Tags
streaming, databricks, ai

## Full Content

# Integrate Slack with Genie natively using Databricks Apps in 30mins!

**Source:** https://www.databricksters.com/p/integrate-slack-with-genie-natively

**Blog:** Databricks

---



Integrate Slack with Genie natively using Databricks Apps in 30mins! 

Databricksters

Subscribe

Sign in

Integrate Slack with Genie natively using Databricks Apps in 30mins! 

Ambarish

Oct 27, 2025

4

5

1

Share

Integrate Slack with Genie natively using Databricks Apps in 30min!

A Slack bot that integrates with Databricks Genie using conversational APIs, allowing users to query and interact with their data directly from Slack.

Features

🤖 

Conversational AI

: Ask questions about your data in natural language

💬 

Threaded Conversations

: Maintain context across multiple messages in Slack threads

📊 

Rich Responses

: Get query results and reasoning directly in Slack

🔄 

Real-time Updates

: Socket mode for instant message handling

🔐 

Secure

: Natively developed within the Databricks ecosystem and governed with Unity Catalog

Architecture

Prerequisites

Databricks Workspace

Active Databricks workspace

Genie space ID

Slack App

Slack workspace with admin access

Slack app created with socket mode enabled

Setup Instructions

1. Slack App Setup

Go to 

Slack API Apps

Click 

Create New App

 → 

From scratch

Name your app (e.g., “Databricks Genie Bot”) and select your workspace

Configure OAuth &amp; Permissions

Under 

OAuth &amp; Permissions

, add these bot token scopes:

Thanks for reading Databricksters! Subscribe for free to receive new posts and support my work.

Subscribe

app_mentions:read

chat:write

im:history

im:read

im:write

Enable Socket Mode

Go to 

Socket Mode

 in your app settings

Enable Socket Mode

Name Token (e.g., “Databricks Genie Bot Token”) and generate an app-level token with connections:write scope

Save the token (starts with xapp-)

Subscribe to Events

Under 

Event Subscriptions

:

Enable Events

Subscribe to bot events:

app_mention

message.im

Install App to Workspace

Go to 

Install App

Click 

Install to Workspace

Authorize the app

Save the 

Bot User OAuth Token

 (starts with xoxb-)

Note Slack Signing Secret

To find your Slack signing secret:

Go to your Slack app settings

Navigate to 

Basic Information

Find 

Signing Secret

 under 

App Credentials

2. Databricks Application Setup

Create a new Databricks App

Go to Databricks console, Compute, and create a new App using 

Create custom app

Name Databricks App (e.g., “databricks-genie-bot-app”)

Wait for the App to be ready and note the service principal from the 

Authorization

 tab, App authorization

Note Genie Space ID

Navigate to the specific Genie space within your Databricks workspace. The space ID can also be found and copied from the Settings tab of that Genie space.

Prepare Application Code

Download the application code and make the necessary updates to the config values in app.yaml. Download all files from repo: 

https://github.com/adgitdemo/ad_databricks/tree/main/genie-slack-app

Grant Permissions to Service Principal

Note that the Service Principal is created automatically for the Databricks app from the above step, and the following grants are performed.

Grant Genie Access with “Can Run” permissions

Grant Warehouse access with “Can Use” permissions

Grant access to Unity Catalog tables/views used in Genie Space via Unity Catalog or SQL

Alternatively, you can do it using Apps UI as below,

Select Databricks App and edit it.

Deploy Application to Databricks Apps

You can create or select an existing folder in the workspace where you want to upload application code and application files.

Go to the Databricks App and deploy the application from the folder where all the application code is stored.

Wait for the deployment to finish, and once you see the application is 

running

, it's time to talk to Genie from Slack!

Usage

In Slack

Direct Messages:

Send a DM to your bot

Ask questions like: “Show me sales data for last month”

Channel Mentions:

Invite the bot to a channel: /invite @YourBotName

Mention the bot: @YourBotName what are the top 10 customers?

Threaded Conversations:

Continue asking questions in a thread

The bot maintains conversation context within threads

Demo Time

Start chatting with 

Databricks Genie Bot

It will respond in a thread,

Let’s ask a real question,

You should give feedback to Genie Bot, and it will get recorded in Databricks Genie, so that the author of Genie Space will review it!

Please note that when you open Databricks App using the deployment URL like below, it will show the “App Not Available” page. This is expected as this application does not have any UI rendering component. You can check application logs to monitor any activity.

Thanks for reading Databricksters! Subscribe for free to receive new posts and support my work.

Subscribe

4

5

1

Share

Previous

Next

Discussion about this post

Comments

Restacks

Ambarish

Nov 4

Author

Here you go - 
https://www.databricksters.com/p/integrate-teams-with-genie-using
 !!!

Reply

Share

Ambarish

Nov 3

Author

Wonderful, stay tuned it's coming soon! 

Reply

Share

3 more comments...

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
