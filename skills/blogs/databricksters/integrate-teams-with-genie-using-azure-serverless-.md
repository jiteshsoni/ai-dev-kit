---
name: integrate-teams-with-genie-using-azure-serverless-
description: 
---

# Integrate Teams with Genie using Azure Serverless Framework in 30mins!

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/integrate-teams-with-genie-using

## Tags
streaming, databricks, ai

## Full Content

# Integrate Teams with Genie using Azure Serverless Framework in 30mins!

**Source:** https://www.databricksters.com/p/integrate-teams-with-genie-using

**Blog:** Databricks

---



Integrate Teams with Genie using Azure Serverless Framework in 30mins!

Databricksters

Subscribe

Sign in

Integrate Teams with Genie using Azure Serverless Framework in 30mins!

Ambarish

Nov 04, 2025

7

5

Share

A Teams Bot that integrates with Databricks Genie using conversational APIs, allowing users to query and interact with their data directly from Teams.

Features

🤖 

Conversational AI

: Ask questions about your data in natural language

💬 

Threaded Conversations

: Maintain context across multiple messages in Teams Channels

📊 

Rich Responses

: Get query results and reasoning directly in Slack

🔄 

Real-time Feedback

: Provide feedback to Genie directly from Teams

🔐 

Secure

: Uses Azure AD Authentication with OAuth 2.0 and service principals

Architecture

Prerequisites

Azure Access - with permissions to create resources

Azure CLI Setup

Azure Databricks workspace

Databricks Genie space

Setup Instructions

Azure Resources Setup

Prepare Application Code

Thanks for reading Databricksters! Subscribe for free to receive new posts and support my work.

Subscribe

Download application code from repo 

https://github.com/adgitdemo/ad_databricks/tree/main/genie-teams-app

 and make necessary updates to config values in .env based on reference in config.env. Execute

 install.sh

 script. This script will take care of everything for you as listed below.

Creates Resource Group

Creates Storage Account

Creates Function App

Creates App Registration &amp; Service Principal

Generates client secret

Adds Azure Databricks API permission

Configures Function App settings

Deploys code

Creates Bot Service

Enables the Teams channel

Creates Teams app package

The script will output all resource names along with a zip file to be used for the Teams application setup. Once the installation script is successfully completed,

1. Add service principal to Databricks (App ID: xxxxxx-xxx-xxx-xxxxx-xxxxxxx)

You will first need to add a service principal at the account level, and later same needs to be added at the workspace level

2. Grant Genie space access and catalog/schema/table access to the service principal

3. Upload Teams app: databricks-genie-teams-app-xxxxxx.zip (appears as: DB Genie xx)

4. Test in Teams!

Usage

In Teams

Direct Messages:

Send a DM to your bot

Ask questions like: “Show me sales data for last month”

Mentions in Teams:

Add the bot to a Team/channel

Mention the bot: @YourBotName, what are the top 10 customers?

Threaded Conversations within Teams/Channels:

Continue asking questions in a thread

The bot maintains conversation context within threads

Demo Time

Start chatting with 

Databricks Genie Bot

Let’s ask real question,

You should give feedback to Genie Bot, and it will get recorded in Databricks Genie, and the author of Genie Space will review it!

Thanks for reading Databricksters! Subscribe for free to receive new posts and support my work.

Subscribe

7

5

Share

Previous

Next

Discussion about this post

Comments

Restacks

Ambarish

Nov 4

Author

Teams users do not need direct access to Databricks. This setup utilizes an Azure service principal to connect to Databricks, and you must grant the necessary access to it on Genie Space to make it work. All interactions will be part of the service principal in Genie Space. So you do not need to add any users to Databricks directly to make this work. If you like to restrict this App to only selected Teams/channels, you can configure that while adding the App to Teams. You could certainly enhance this code to add OBO (On-Behalf-Of) user authorization if necessary.

Reply

Share

1 reply

Nidhi Gupta

Nov 4

Liked by Ambarish

Thnk u for sure my weekend task to implement this.

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
