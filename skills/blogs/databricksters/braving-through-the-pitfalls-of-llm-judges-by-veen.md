---
name: braving-through-the-pitfalls-of-llm-judges-by-veen
description: 
---

# Braving through the pitfalls of LLM judges - by Veena

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/braving-through-the-pitfalls-of-llm

## Tags
joins, ai, spark, data-engineering, databricks, clustering

## Full Content

# Braving through the pitfalls of LLM judges - by Veena

**Source:** https://www.databricksters.com/p/braving-through-the-pitfalls-of-llm

---



Braving through the pitfalls of LLM judges - by Veena

Databricksters

Subscribe

Sign in

AI &amp; ML

Braving through the pitfalls of LLM judges

A guide on improving your LLM evaluations.

Veena

Apr 15, 2025

4

Share

a picture of Judge Judy (the original judge). 

LLM judges are the de facto standard for evaluating anything related to LLMs. Human evaluations are too expensive and difficult to scale, so LLMs are a practical alternative.

But judges aren’t perfect. Here, we will examine some of the common problems with LLM judges and explore different ways to deal with them.

Please note that this blog will not cover non-LLM evaluations!

Let us first review what Agent Evaluation looks like in Databricks.

LLM-as-a-judge is a common evaluation technique; instead of using a human to evaluate a text response, we use an LLM. 

Mosaic AI Agent Evaluation

 allows you to systematically assess the quality of your agentic applications. This includes the use of LLM judges.

There are several 

built-in judges

 that can be used, including:

Correctness judge: assesses whether response is accurate

Helpfulness judge: assesses if response satisfies the user

Harmlessness judge: assesses if response avoids harmful content

Coherence judge: assesses if response is logical

Relevance judge: assesses whether response addresses the query

Given an evaluation set, you can use these judges to evaluate. Each judge takes a different set of inputs; for example, the Correctness judge requires a request, a response, and an expected response, but the Harmlessness judge only requires a request and a response. You can take a look at 

what judges are available and how to use them

 here.

LLMs have a hard time with numbers.

Let me show you what a basic implementation looks like. Using the databricks callable judge SDK, you can use the correctness judge like:

from databricks.agents.evals import judges

assessment = judges.correctness(
 request=&quot;What is the difference between reduceByKey and groupByKey in Spark?&quot;,
 response=&quot;reduceByKey aggregates data before shuffling, whereas groupByKey shuffles all data, making reduceByKey more efficient.&quot;,
 expected_facts=[
 &quot;reduceByKey aggregates data before shuffling&quot;,
 &quot;groupByKey shuffles all data&quot;,
 ]
)

We can see that an assessment contains information something like this:

Assessment: 
error_code=None
error_message=None
metadata={}
name='correctness'
rationale=&quot;...&quot; 
value=CategoricalRating.YES

The value that the assessment returned is categorical. LLMs notably struggle quite a lot with numerical scoring. Some studies show that they have preferences for certain values. Other studies often show them clustering around the highest and lowest values, instead of utilizing the full range.

Let us assume you have already created a judge to output scores from 1 to 10. When graphing the scores with the “ideal” scores, you could see something like this: 

In this example, the LLM gives perfect scores until a certain threshold, where it drops to very low scores. Note: this graph was created manually via matplotlib. 

The built-in judge already includes a categorical value instead of a numerical one, but if you need numerical ratings, 

prompt the judge with an explanation for each of the scores.

 In MLFlow, you can include evaluation examples when defining different evaluation metrics.

average_example = EvaluationExample(
 input=&quot;What are the main types of horse breeds?&quot;,
 output=&quot;The main horse breeds include Arabian, Thoroughbred, Quarter Horse, Appaloosa, Morgan, Tennessee Walker, Clydesdale, and Mustang. Arabians are known for endurance, Thoroughbreds for racing, Quarter Horses for sprinting, and Clydesdales for their size and strength.&quot;,
 score=5,
 justification=&quot;This response correctly lists 8 common horse breeds and provides brief descriptions for 4 of them, but the descriptions are very basic and only cover half of the breeds mentioned. It lacks depth about breed characteristics, historical origins, or typical uses.&quot;,
 grading_context={
 &quot;targets&quot;: &quot;There are numerous horse breeds worldwide, with common breeds including Arabian, Thoroughbred, Quarter Horse, Appaloosa, Morgan, Tennessee Walker, Andalusian, Friesian, Clydesdale, Percheron, Mustang, and Shetland Pony. Each breed has distinctive physical traits, temperaments, and was developed for specific purposes like racing, work, or riding.&quot;
 },
)

horse_breed_similarity_metric = answer_similarity(
 examples=[poor_example, average_example, excellent_example])

LLMs like long answers.

In my last example regarding horses, you can see that the average example was scored lower than it would have been because it lacked ‘depth.’ Unfortunately, lots of LLMs equate depth with a lot of unnecessary chatter. When graphing scores of responses of equal quality, you may see long responses rewarded more than short responses, like: 

In this example, the LLM judge scores rise with response length, even when the actual quality of the content remains constant. Note: this graph was generated manually via matplotlib. 

LLM judges tend to prefer longer outputs

. This makes sense if you have ever used one of these chat bots. Depending on your use case, this might not be preferable. When I am talking to a customer service chatbot, for example, I get frustrated when it responds with paragraph long responses to my simple questions. Conciseness is incredibly important. This problem could also mean that more accurate responses are drowned out by rambling, semi-accurate responses. Observe what is getting approved by your judge to make sure the LLM is not avoiding brevity.

If you are noticing that only long answers are getting approved, you can instead simply adjust scores based on the length, penalizing answers that are ‘too’ long.

Let us assume that you have extracted the base score from the judge. You can have a simple function that penalizes a longer answer, if it surpasses a hardcoded threshold.

length_ratio = response_length / max(1, request_length)

def linear_verbosity_adjustment(length_ratio, base_score):
 threshold = 3.0
 if length_ratio &lt;= threshold:
 return 0
 else:
 return min(base_score * 0.3, (length_ratio - threshold) * 0.5)

But you can also approach this in a more sophisticated manner. 

In this paper

, they fit a regression model to predict “what would the score be if the responses all had the same length?” This improved correlation with human preferences from 0.94 to 0.98, but in most cases, however, this is overkill.

LLMs are biased towards themselves.

We have also seen that

 LLM judges have a preference for text with lower perplexity

. This suggests that LLMs prefer language similar to language they were trained on. This can lead to your evaluators assigning higher scores to outputs generated by their own kind. For example, you can no longer trust a GPT model to evaluate a Llama 8B model against a GPT 4o-mini model without bias.

You can mitigate this bias by using a jury-- a collection of LLM judges instead. The goal here is to use LLMs from different families, so one LLM’s bias towards the answer does not prevent you from understanding the quality of the response. You can create a custom metric in Databricks to do this. Here, I am defining three different judges using different LLMs with the same prompt.

import mlflow
from mlflow.metrics.genai import make_genai_metric_from_prompt
from databricks.agents.evals import metric
from databricks.agents.evals import judges
from mlflow.evaluation import Assessment

judge_prompt = &quot;&quot;&quot;
Determine if this response accurately covers all expected facts.

Request: '{inputs}'
Response: '{response}'
&quot;&quot;&quot;

llama_judge = make_genai_metric_from_prompt(
 name=&quot;a



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
