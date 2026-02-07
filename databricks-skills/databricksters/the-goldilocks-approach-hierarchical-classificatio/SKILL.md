---
name: the-goldilocks-approach-hierarchical-classificatio
description: 
---

# The Goldilocks Approach: Hierarchical Classification with AI_QUERY in Databricks

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/the-goldilocks-approach-hierarchical

## Tags
streaming, joins, ai, performance, databricks

## Full Content

# The Goldilocks Approach: Hierarchical Classification with AI_QUERY in Databricks

**Source:** https://www.databricksters.com/p/the-goldilocks-approach-hierarchical

**Blog:** Databricks

---



The Goldilocks Approach: Hierarchical Classification with AI_QUERY in Databricks

Databricksters

Subscribe

Sign in

The Goldilocks Approach: Hierarchical Classification with AI_QUERY in Databricks

Leverage Databricks `AI_QUERY` to tackle complex, context-dependent hierarchical classification problems that traditional ML and simple LLM prompts cannot solve.

Mandy Baker

Nov 11, 2025

7

Share

Classification Machine Learning problems have been around for a long time. Traditional approaches like decision trees and SVMs have dominated the landscape, and with the rise of NLP, we gained powerful tools for text classification using techniques like TF-IDF and word embeddings. Now, we have Large Language Models that understand context and nuance in ways that traditional models often can’t match. This opens up opportunities to automate classification processes, especially in complex scenarios where rigid classification rules break down, such as hierarchical classification.

How can you take advantage of the latest frontier in classification? This blog will show you how to use AI_QUERY in Databricks to run batch inference for complex hierarchical classifications with LLMs.

Thanks for reading Databricksters! Subscribe for free to receive new posts and support my work.

Subscribe

As always, choose the best approach for your problem! There are many occasions when a classical approach is a better choice. However, if:

Your historical data is not trustworthy

 (e.g. it may be inconsistent, difficult to validate, or labeled by multiple people with different interpretations; you can’t trust that the patterns held therein are going to be useful in a classical machine learning setup)

Your categories are flexible 

(e.g. you need to be able to add a new category on the whims of your business partners without retraining an entire model)

You’re dealing with nuanced, context-dependent classifications

 (e.g. rigid rules fail; maybe “billing dispute” vs “billing inquiry” depends on subtle linguistic cues that are difficult to encode as features)

… then read on to learn how to use AI_QUERY for hierarchical classification!

The Goal

Let’s say that we’re a telecommunications company and we have a lot of customer call transcripts coming in that we want to classify along four levels: 

Domain (Level 1) → Category (Level 2) → Problem Type (Level 3) → Root Cause (Level 4)

. This hierarchy creates a comprehensive support taxonomy covering everything from network outages and billing disputes to device issues and order management, so our business team can gain a lot of insights once all of these transcripts are classified correctly. The only obstacle is actually classifying these transcripts. How should we do that?

If you’re already familiar with the power of 

AI_QUERY

, which allows you to query LLM endpoints via SQL for batch workloads, you could jump right into the Databricks SQL editor or a notebook and use the SQL function right away, relying on a monster prompt that attempts to take each record and assign the right hierarchy in one shot.

The Problem with the “Everything at Once” Approach

Unfortunately, if we have 12 Domains, and each Domain contains 5-6 Categories, and each Category has 3+ Problem Types, and each Problem Type has 5+ Root Causes, we have at minimum 900 hierarchical paths that a single transcript could take.

Combinatorial explosion! From a statistical perspective, we’re asking an LLM to perform a 900-class classification problem—the kind of task where even specialized neural networks start sweating.

There are a couple of issues with treating each of the 900 options as its own unique value:

More choices = worse outcomes:

 Imagine a 900x900 confusion matrix; a classification problem of this size will very likely result in poor precision and poor recall.

The “needle in a haystack” problem:

 When you present an LLM with a massive list of options, accuracy degrades significantly. False Negatives are easy when there are 899 other options, some of which sound very similar.

Prompt complexity becomes unmaintainable (and more expensive):

 Your prompt becomes a short story, making it difficult to debug, version control, and understand what instructions the model is actually following - and on top of that you’re paying for all those tokens each time you send a request!

What About Individual Level Classification?

On the other hand, if we look at each level individually, we greatly reduce the number of options per record. We only need to predict across 12 Domains, for example, and once we have the Domain, we only need to predict across 5 or 6 Categories, and so on down the levels. This approach will improve shrink the prediction space and likely improve accuracy, precision, and recall. The downside here is that we’re now running four sequential SQL queries. Depending on the model we’re using, this could get expensive and/or slow.

By now you might be thinking, “So that porridge is too cold, and this one is too hot… How do I use AI_QUERY for hierarchical classification???”

The Goldilocks Solution: Hierarchically-Aware Classification

To get just-right porridge, we can find a balance of the two approaches above by running two queries: the first will use a simple prompt to classify Level 1, and then the second will use a dynamic prompt to classify Levels 2-4. Here’s our recipe:

Create a hierarchies table

Build a gold-standard evaluation dataset

Execute your Domain (Level 1) prompt

Execute your Levels 2-4 prompt (with dynamic hierarchy filtering)

Step 1: Build Your Hierarchy Table

First, we need to build a table of our classification hierarchies. This table will be the reference for our second dynamic AI_QUERY prompt. It should look something like the schema here:

transcript_classification_hierarchy_table
├── level_1 (string) -- Domain
├── level_2_map (string) -- Category
├── level_3_map (string) -- Problem Type
└── level_4_map (string) -- Root Cause

Each row represents a valid path through your hierarchy. This is your source of truth that ensures we only provide valid paths to the LLM.

Here’s a sample record:

Step 2: Build Your Evaluation Dataset

Next, we need to make sure we have a solid evaluation dataset. If possible, grab an unsuspecting nearby SME and beg them to help you build this dataset.

The number of examples depends on how many categories you have in your dataset, but aim for at least 50-100 labeled examples covering your major categories. Try to avoid a scenario where you spend a lot of time prompt engineering, only to realize your ground truths are not all that accurate to begin with and you’ve been tuning your prompt in the wrong direction (speaking from personal experience, this is not fun).

Bonus:

 Having a gold-standard evaluation dataset allows you to continually try new models and model versions as they’re released and use the cheapest, fastest version that passes your evaluation metrics. This is deeply unsexy work, but it is possibly the most valuable.

Step 3: Classify Level 1 (Domain)

Once you have your hierarchical table and evaluation dataset, we’re ready to run our first AI_QUERY SQL statement: classifying Level 1. In this first batch inference round, we focus only on classifying the Domain. Since all the hierarchies stem from this first choice, getting it right is critical—but with only 12 options, our LLM has a much better chance of getting the classifications right. Here’s a sample query, using Llama 3.3 70b. We concatenate the prompt, which consists of the 12 Domain options, as well as the transcript itself and an instruction to only return the category name.

%sql
CREATE OR REPLACE TABLE catalog.schema.call_transcripts_l1_predictions AS
SELECT
 call_id,
 transcript_text,
 AI_QUERY(‘databricks-meta-llama-3-3-70b-ins



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
