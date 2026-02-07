---
name: when-data-engineering-met-ai
description: **TL;DR:**  
- **Key Idea 1:** Use serverless computing on Azure to avoid Azure's quota limits when integrating AI for data engineering tasks, as it a...
---

# When Data Engineering Met AI

## Overview
**TL;DR:**  
- **Key Idea 1:** Use serverless computing on Azure to avoid Azure's quota limits when integrating AI for data engineering tasks, as it allows for efficient scaling and avoids query speed issues with large models.

- **Key Idea 2:** Leverage Databricks' AI Query function to run custom queries with different LLM models. This allows flexibility in choosing the best model based on success rates and task requirements.

- **Key Idea 3:** Monitor query performance using metrics like completed vs. failed inferences to optimize efficiency and troubleshoot issues effectively.

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2025-04-26
- **URL:** https://www.youtube.com/watch?v=A6u31Q6M4yQ

## Tags
databricks, performance, ai

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** A6u31Q6M4yQ

### Summary

**TL;DR:**  
- **Key Idea 1:** Use serverless computing on Azure to avoid Azure's quota limits when integrating AI for data engineering tasks, as it allows for efficient scaling and avoids query speed issues with large models.

- **Key Idea 2:** Leverage Databricks' AI Query function to run custom queries with different LLM models. This allows flexibility in choosing the best model based on success rates and task requirements.

- **Key Idea 3:** Monitor query performance using metrics like completed vs. failed inferences to optimize efficiency and troubleshoot issues effectively.

### Full Transcript

Hey folks, I wanted to quickly cover a topic for you. So lately people have been trying to call LMS within data engineering workloads meaning they are doing some data transformation and in between they want to also use an LLM model. So in this quick video we are going to do that. I usually like to decouple the LLM call with the rest of the transformation because LLM calls are very slow.

So what I would realistically do is whenever I have a data engineering workload, I'll decouple the data engineering part and then calling the LLM part into a separate piece. So here I'll do a demo of it. So first what you need to know is inside databicks if you click playground inside machine learning you'll see a couple of models here. So you can choose in between these models and we going to use paper token choice.

And then basically what you do is you can click choose endpoint and just chat with it. Example in my case I want to say who is the parent company of Google just as an example and give me the parent company name and if you don't know just say I do not know. You can say anything what you want and you can see which LLM model works for you just for an example.

So I have a table with me which has a bunch of company names. By the way I'm running this on serverless. Even LLM will use the serverless one. That way whatever constraints you have been facing with Azure quotas won't be there.

The problem which this customer has is this data is not always accurate. And what they're trying to do is look at the company page, look at the company tail and then try to predict. Basically, we going to ask LLM, hey, what is the parent company name for these? Example, if you have RBC, which is Royal Bank of Canada, there is also RBC Financial Market. There is also RBC capital markets. What we really want is and to tell us, hey, who is the parent entity, which is probably just RBC.

Inside datab bricks we have this function called AI query and in this AI query function you pass which query needs to be called. So you can create your own custom query. In my case I created this query which I need to pass to the model and then I'll get a response back.

And only thing to know it's like it's very slow. LLMs are very slow and the bigger model you use, the slower you would be. So you need to figure out what is the cheapest model which meets your requirements.

---






---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
