---
name: "llm-judges-pitfalls-mitigation"
description: "Identify and mitigate common pitfalls in LLM-as-a-judge evaluations: numerical scoring bias, length bias, self-bias, and inconsistency. Includes Databricks Agent Evaluation patterns."
---

# Braving the Pitfalls of LLM Judges: Evaluation Best Practices

## Overview

This skill covers identifying and mitigating common pitfalls when using LLMs as judges for evaluation. Learn about numerical scoring bias, length bias, self-bias (familiarity bias), and inconsistency issues. Includes patterns for using Databricks Agent Evaluation, creating LLM juries, implementing verbosity adjustments, and improving evaluation consistency through chain-of-thought reasoning.

## Quick Start

### Use Built-in Judges
Leverage Databricks built-in judges:

```python
from databricks.agents.evals import judges

# Correctness judge
assessment = judges.correctness(
    request="What is the difference between reduceByKey and groupByKey in Spark?",
    response="reduceByKey aggregates data before shuffling, whereas groupByKey shuffles all data, making reduceByKey more efficient.",
    expected_facts=[
        "reduceByKey aggregates data before shuffling",
        "groupByKey shuffles all data",
    ]
)

print(f"Assessment: {assessment.value}")  # CategoricalRating.YES
print(f"Rationale: {assessment.rationale}")

# Other built-in judges
helpfulness_assessment = judges.helpfulness(request, response)
harmlessness_assessment = judges.harmlessness(request, response)
coherence_assessment = judges.coherence(request, response)
relevance_assessment = judges.relevance(request, response)
```

### Create LLM Jury
Mitigate self-bias with multiple judges:

```python
import mlflow
from mlflow.metrics.genai import make_genai_metric_from_prompt
from databricks.agents.evals import metric
from mlflow.evaluation import Assessment

judge_prompt = """
Determine if this response accurately covers all expected facts.

Request: '{inputs}'
Response: '{response}'
"""

# Create judges from different model families
llama_judge = make_genai_metric_from_prompt(
    name="accuracy_judge1",
    judge_prompt=judge_prompt,
    model="endpoints:/databricks-meta-llama-3-1-405b-instruct",
    metric_metadata={"assessment_type": "ANSWER"}
)

claude_judge = make_genai_metric_from_prompt(
    name="accuracy_judge2",
    judge_prompt=judge_prompt,
    model="endpoints:/databricks-claude-3-7-sonnet",
    metric_metadata={"assessment_type": "ANSWER"}
)

gpt_judge = make_genai_metric_from_prompt(
    name="accuracy_judge3",
    judge_prompt=judge_prompt,
    model="endpoints:/test-gpt-endpoint",
    metric_metadata={"assessment_type": "ANSWER"}
)

@metric
def llm_jury(request, response):
    """Combine multiple judges to reduce bias"""
    inputs = request['messages'][0]['content']
    
    llama_score = llama_judge(inputs=inputs, response=response).scores[0]
    claude_score = claude_judge(inputs=inputs, response=response).scores[0]
    gpt_score = gpt_judge(inputs=inputs, response=response).scores[0]
    
    avg_score = (llama_score + claude_score + gpt_score) / 3
    
    return [
        Assessment(
            name="llm_jury_score",
            value=avg_score,
            rationale=f"LLAMA: {llama_score:.2f}, Claude: {claude_score:.2f}, GPT: {gpt_score:.2f}"
        )
    ]

# Usage
jury_assessment = llm_jury(request, response)
```

## Common Patterns

### Pattern 1: Mitigate Numerical Scoring Bias
Provide examples for each score level:

```python
from mlflow.evaluation import EvaluationExample

# Create examples for each score level
poor_example = EvaluationExample(
    input="What are the main types of horse breeds?",
    output="Horses.",
    score=1,
    justification="Response is too brief and lacks any useful information.",
    grading_context={
        "targets": "Should list multiple breeds with descriptions"
    }
)

average_example = EvaluationExample(
    input="What are the main types of horse breeds?",
    output="The main horse breeds include Arabian, Thoroughbred, Quarter Horse, Appaloosa, Morgan, Tennessee Walker, Clydesdale, and Mustang. Arabians are known for endurance, Thoroughbreds for racing, Quarter Horses for sprinting, and Clydesdales for their size and strength.",
    score=5,
    justification="This response correctly lists 8 common horse breeds and provides brief descriptions for 4 of them, but the descriptions are very basic and only cover half of the breeds mentioned. It lacks depth about breed characteristics, historical origins, or typical uses.",
    grading_context={
        "targets": "There are numerous horse breeds worldwide, with common breeds including Arabian, Thoroughbred, Quarter Horse, Appaloosa, Morgan, Tennessee Walker, Andalusian, Friesian, Clydesdale, Percheron, Mustang, and Shetland Pony. Each breed has distinctive physical traits, temperaments, and was developed for specific purposes like racing, work, or riding."
    }
)

excellent_example = EvaluationExample(
    input="What are the main types of horse breeds?",
    output="Comprehensive response with detailed breed information...",
    score=10,
    justification="Thorough response covering multiple breeds with detailed characteristics.",
    grading_context={
        "targets": "Should provide comprehensive breed information"
    }
)

# Use examples in metric definition
from mlflow.metrics.genai import answer_similarity

horse_breed_metric = answer_similarity(
    examples=[poor_example, average_example, excellent_example]
)
```

### Pattern 2: Address Length Bias
Penalize overly verbose responses:

```python
def linear_verbosity_adjustment(length_ratio, base_score, threshold=3.0):
    """
    Adjust score based on response length.
    
    Args:
        length_ratio: response_length / request_length
        base_score: Original judge score
        threshold: Length ratio threshold before penalizing
    """
    if length_ratio <= threshold:
        return 0  # No penalty
    else:
        # Penalize scores that exceed threshold
        penalty = min(base_score * 0.3, (length_ratio - threshold) * 0.5)
        return penalty

def adjust_score_for_length(request, response, base_score):
    """Apply verbosity adjustment to judge score"""
    request_length = len(request.split())
    response_length = len(response.split())
    length_ratio = response_length / max(1, request_length)
    
    penalty = linear_verbosity_adjustment(length_ratio, base_score)
    adjusted_score = base_score - penalty
    
    return max(0, adjusted_score)

# Usage
base_assessment = judges.helpfulness(request, response)
base_score = 8.0  # Example score

adjusted_score = adjust_score_for_length(request, response, base_score)
print(f"Base score: {base_score}, Adjusted: {adjusted_score}")
```

### Pattern 3: Improve Consistency with Chain-of-Thought
Force deliberate reasoning:

```python
def create_cot_judge_prompt(base_prompt):
    """Add chain-of-thought reasoning to judge prompt"""
    
    cot_prompt = f"""
{base_prompt}

Before providing your final assessment, please:
1. Analyze the request carefully
2. Evaluate the response against each criterion
3. Consider edge cases and nuances
4. Provide your reasoning step-by-step
5. Finally, provide your assessment

Reasoning:
"""
    return cot_prompt

# Usage
base_prompt = """
Determine if this response accurately covers all expected facts.

Request: '{inputs}'
Response: '{response}'
"""

cot_prompt = create_cot_judge_prompt(base_prompt)

cot_judge = make_genai_metric_from_prompt(
    name="cot_accuracy_judge",
    judge_prompt=cot_prompt,
    model="endpoints:/databricks-meta-llama-3-1-405b-instruct",
    metric_metadata={"assessment_type": "ANSWER"}
)
```

### Pattern 4: Majority Voting for Consistency
Reduce random variation:

```python
def majority_vote_judge(request, response, judge_func, num_votes=5):
    """
    Get multiple assessments and use majority vote.
    
    Args:
        request: Input request
        response: Model response
        judge_func: Judge function to call
        num_votes: Number of times to query judge
    """
    assessments = []
    
    for i in range(num_votes):
        assessment = judge_func(request, response)
        assessments.append(assessment.value)
    
    # Count votes
    vote_counts = {}
    for vote in assessments:
        vote_counts[vote] = vote_counts.get(vote, 0) + 1
    
    # Return majority vote
    majority_vote = max(vote_counts.items(), key=lambda x: x[1])[0]
    confidence = vote_counts[majority_vote] / num_votes
    
    return {
        "assessment": majority_vote,
        "confidence": confidence,
        "all_votes": assessments
    }

# Usage
majority_result = majority_vote_judge(
    request,
    response,
    lambda rq, rs: judges.correctness(rq, rs, expected_facts=expected_facts),
    num_votes=5
)

print(f"Majority assessment: {majority_result['assessment']}")
print(f"Confidence: {majority_result['confidence']:.2%}")
```

## Reference Files

- [Databricks Agent Evaluation](https://docs.databricks.com/en/generative-ai/agent-evaluation/) - Evaluation framework
- [LLM Judge Reference](https://docs.databricks.com/en/generative-ai/agent-evaluation/llm-judge-reference.html) - Built-in judges
- [Improving LLM-as-a-Judge](https://arxiv.org/abs/2310.08491) - Research paper on judge improvements

## Common Issues

| Issue | Solution |
|-------|----------|
| **Numerical scoring bias** | Provide examples for each score level, use categorical ratings |
| **Length bias** | Apply verbosity adjustments, penalize overly long responses |
| **Self-bias** | Use LLM jury with different model families |
| **Inconsistency** | Use chain-of-thought reasoning, majority voting |
| **Clustering at extremes** | Provide scoring examples, use categorical ratings |

## Key Takeaways

1. **Numerical Bias**: LLMs struggle with numerical scores - use categorical ratings or provide examples
2. **Length Bias**: LLMs prefer longer outputs - apply verbosity adjustments
3. **Self-Bias**: LLMs favor their own outputs - use jury of different model families
4. **Inconsistency**: Same prompt can yield different scores - use CoT reasoning or majority voting
5. **Built-in Judges**: Databricks provides correctness, helpfulness, harmlessness, coherence, relevance
6. **Targeted Solutions**: Address specific biases affecting your use case

## Complete Evaluation Workflow

```python
def comprehensive_evaluation_workflow(request, response, expected_facts=None):
    """Complete evaluation workflow addressing common pitfalls"""
    
    # Step 1: Use built-in judge
    base_assessment = judges.correctness(
        request=request,
        response=response,
        expected_facts=expected_facts or []
    )
    
    # Step 2: Apply length adjustment
    request_length = len(request.split())
    response_length = len(response.split())
    length_ratio = response_length / max(1, request_length)
    
    base_score = 8.0 if base_assessment.value == "YES" else 2.0
    adjusted_score = adjust_score_for_length(request, response, base_score)
    
    # Step 3: Get jury assessment (if needed)
    jury_assessment = llm_jury(request, response)
    
    # Step 4: Combine results
    final_assessment = {
        "base_assessment": base_assessment.value,
        "base_rationale": base_assessment.rationale,
        "length_adjusted_score": adjusted_score,
        "jury_score": jury_assessment[0].value if jury_assessment else None,
        "length_ratio": length_ratio
    }
    
    return final_assessment

# Usage
evaluation = comprehensive_evaluation_workflow(
    request="Explain Spark reduceByKey",
    response="reduceByKey aggregates data before shuffling...",
    expected_facts=["aggregates before shuffle", "more efficient"]
)

print("Evaluation Results:")
for key, value in evaluation.items():
    print(f"  {key}: {value}")
```

## When to Use This Skill

- Building LLM evaluation systems
- Using Databricks Agent Evaluation
- Mitigating judge biases
- Improving evaluation consistency
- Creating custom evaluation metrics
- Debugging evaluation issues

## Related Skills

- agent-evaluation-patterns
- llm-evaluation-metrics
- custom-judge-implementation
- evaluation-bias-mitigation