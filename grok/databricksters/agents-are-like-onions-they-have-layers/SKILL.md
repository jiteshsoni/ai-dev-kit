---
name: "mlflow-custom-scorers-agents"
description: "Implement custom MLflow scorers to evaluate AI agent tool usage by analyzing trace spans and LLM decision-making patterns."
---

# Custom MLflow Scorers for AI Agents

## Overview

This skill covers implementing custom MLflow scorers to evaluate AI agent performance by analyzing trace spans and tool usage patterns. Learn how to create sophisticated evaluation metrics that go beyond simple end-to-end scoring, enabling deep analysis of agent decision-making, tool selection appropriateness, and execution quality. Includes practical implementations for tool usage validation, span analysis, and integration with MLflow evaluation pipelines.

## Quick Start

### Basic Custom Scorer Implementation
Create a scorer to evaluate agent tool usage decisions:

```python
from mlflow.genai.scorers import Scorer
from mlflow.genai.judges import custom_prompt_judge
from mlflow.entities import Feedback, AssessmentSource, SpanType, SpanAttributeKey
import json

class AgentToolUsageScorer(Scorer):
    def __init__(self, tools_config: dict):
        super().__init__()
        self.tools_config = tools_config
        self.tool_requirement_prompt = """
        Given the user input: {inputs}
        And the tool: {tool_name} with description: {tool_description}

        Should this tool be used to answer the query?
        Answer with only "required" or "not_required".
        """

    def __call__(self, *, inputs=None, outputs=None, expectations=None, trace=None):
        if not trace:
            return Feedback(
                value=False,
                rationale="No trace available for analysis",
                source=AssessmentSource("LLM_JUDGE", "tool_usage_scorer")
            )

        # Determine which tools should be used
        required_tools = self._determine_required_tools(inputs)
        
        # Extract actual tool usage from trace
        used_tools = self._extract_used_tools_from_trace(trace)
        
        # Compare expected vs actual usage
        analysis = self._compare_tool_usage(required_tools, used_tools)
        
        return self._generate_feedback(analysis)

    def _determine_required_tools(self, user_input: str) -> dict:
        """Use LLM judge to determine which tools should be used"""
        required_tools = {}
        
        for tool_name, tool_description in self.tools_config.items():
            judge = custom_prompt_judge(
                name=f"{tool_name}_requirement_judge",
                prompt_template=self.tool_requirement_prompt,
                numeric_values={"required": 1.0, "not_required": 0.0}
            )
            
            result = judge(
                inputs=user_input,
                tool_name=tool_name,
                tool_description=tool_description
            )
            
            required_tools[tool_name] = result.value == 1.0
        
        return required_tools

    def _extract_used_tools_from_trace(self, trace) -> list:
        """Extract tool calls from MLflow trace spans"""
        tools_used = []
        tool_spans = trace.search_spans(span_type=SpanType.TOOL)
        
        for span in tool_spans:
            outputs = span.get_attribute(SpanAttributeKey.OUTPUTS)
            if outputs and 'content' in outputs:
                content = json.loads(outputs['content'])
                tool_info = {
                    'tool_call_id': outputs.get('tool_call_id'),
                    'tool_name': outputs.get('name'),
                    'tool_response': content.get('value'),
                    'tool_status': outputs.get('status')
                }
                tools_used.append(tool_info)
        
        return tools_used

    def _compare_tool_usage(self, required_tools: dict, used_tools: list) -> dict:
        """Compare expected vs actual tool usage"""
        required_tool_names = [name for name, required in required_tools.items() if required]
        used_tool_names = [tool['tool_name'] for tool in used_tools]
        
        correctly_used = []
        incorrectly_used = []
        failed_required = []
        missing_required = required_tool_names.copy()
        
        for tool in used_tools:
            tool_name = tool['tool_name']
            if tool_name in required_tool_names:
                correctly_used.append(tool)
                if tool_name in missing_required:
                    missing_required.remove(tool_name)
                
                # Check if tool execution was successful
                if not self._is_tool_successful(tool):
                    failed_required.append(tool)
            else:
                incorrectly_used.append(tool)
        
        return {
            'correctly_used_tools': correctly_used,
            'incorrectly_used_tools': incorrectly_used,
            'failed_required_tools': failed_required,
            'missing_required_tools': missing_required
        }

    def _is_tool_successful(self, tool: dict) -> bool:
        """Determine if a tool call was successful"""
        # Custom logic based on your tool's success criteria
        status = tool.get('tool_status', '').lower()
        response = tool.get('tool_response', '')
        
        # Example success criteria - customize for your tools
        if status == 'error':
            return False
        if 'error' in response.lower():
            return False
        if not response or response.strip() == '':
            return False
        
        return True

    def _generate_feedback(self, analysis: dict) -> Feedback:
        """Generate feedback based on tool usage analysis"""
        total_issues = (
            len(analysis['incorrectly_used_tools']) +
            len(analysis['failed_required_tools']) +
            len(analysis['missing_required_tools'])
        )
        
        if total_issues == 0:
            return Feedback(
                value=True,
                rationale="Agent used tools appropriately with no issues detected",
                source=AssessmentSource("LLM_JUDGE", "tool_usage_scorer")
            )
        else:
            rationale = f"Found {total_issues} tool usage issues: "
            issues = []
            
            if analysis['missing_required_tools']:
                issues.append(f"Missing required tools: {analysis['missing_required_tools']}")
            if analysis['failed_required_tools']:
                issues.append(f"Failed required tools: {len(analysis['failed_required_tools'])}")
            if analysis['incorrectly_used_tools']:
                issues.append(f"Incorrectly used tools: {len(analysis['incorrectly_used_tools'])}")
            
            rationale += "; ".join(issues)
            
            return Feedback(
                value=False,
                rationale=rationale,
                source=AssessmentSource("LLM_JUDGE", "tool_usage_scorer")
            )
```

### Using the Custom Scorer
Integrate with MLflow evaluation:

```python
# Define your agent's tools
tools_config = {
    "python_executor": "Execute Python code and return results",
    "doc_retriever": "Search Databricks documentation for relevant information",
    "translator": "Translate text between languages",
    "summarizer": "Create concise summaries of long text"
}

# Create scorer instance
tool_scorer = AgentToolUsageScorer(tools_config)

# Use in evaluation
import mlflow.genai

results = mlflow.genai.evaluate(
    data=evaluation_dataset,
    scorers=[tool_scorer, "correctness", "safety"],
    model=your_agent_model
)

print(results)
```

## Common Patterns

### Pattern 1: Tool Execution Quality Scorer
Evaluate not just tool selection, but execution success:

```python
class ToolExecutionQualityScorer(Scorer):
    def __init__(self, quality_criteria: dict):
        super().__init__()
        self.quality_criteria = quality_criteria  # tool_name -> quality_checks

    def __call__(self, *, inputs=None, outputs=None, expectations=None, trace=None):
        tool_spans = trace.search_spans(span_type=SpanType.TOOL)
        quality_scores = {}
        
        for span in tool_spans:
            tool_name = span.get_attribute(SpanAttributeKey.OUTPUTS).get('name')
            tool_response = span.get_attribute(SpanAttributeKey.OUTPUTS).get('content')
            
            if tool_name in self.quality_criteria:
                score = self._evaluate_tool_quality(tool_name, tool_response)
                quality_scores[tool_name] = score
        
        # Return aggregate quality score
        avg_quality = sum(quality_scores.values()) / len(quality_scores) if quality_scores else 0
        
        return Feedback(
            value=avg_quality,
            rationale=f"Average tool execution quality: {avg_quality:.2f}",
            source=AssessmentSource("LLM_JUDGE", "tool_quality_scorer")
        )

    def _evaluate_tool_quality(self, tool_name: str, response: str) -> float:
        """Custom quality evaluation logic"""
        criteria = self.quality_criteria[tool_name]
        
        score = 1.0  # Start with perfect score
        
        for check in criteria.get('checks', []):
            if check['type'] == 'contains' and check['value'] not in response:
                score -= check.get('penalty', 0.2)
            elif check['type'] == 'not_contains' and check['value'] in response:
                score -= check.get('penalty', 0.2)
        
        return max(0.0, min(1.0, score))  # Clamp between 0 and 1
```

### Pattern 2: Agent Reasoning Quality Scorer
Evaluate the quality of agent decision-making before tool calls:

```python
class AgentReasoningScorer(Scorer):
    def __init__(self):
        super().__init__()
        self.reasoning_judge = custom_prompt_judge(
            name="reasoning_quality_judge",
            prompt_template="""
            Evaluate the quality of this agent's reasoning for tool selection:
            
            User Input: {inputs}
            Agent Reasoning: {agent_reasoning}
            
            Rate the reasoning quality from 0.0 to 1.0, where:
            1.0 = Perfect reasoning, logical tool selection
            0.0 = Poor reasoning, inappropriate tool choices
            """,
            numeric_values={"range": [0.0, 1.0]}
        )

    def __call__(self, *, inputs=None, outputs=None, expectations=None, trace=None):
        # Extract agent reasoning from spans
        reasoning_spans = trace.search_spans(span_type=SpanType.LLM)
        
        best_reasoning = ""
        for span in reasoning_spans:
            inputs = span.get_attribute(SpanAttributeKey.INPUTS)
            if inputs and 'messages' in inputs:
                # Extract reasoning from LLM call
                reasoning = self._extract_reasoning_from_messages(inputs['messages'])
                if reasoning:
                    best_reasoning = reasoning
                    break
        
        if not best_reasoning:
            return Feedback(
                value=0.5,
                rationale="No clear reasoning found in trace",
                source=AssessmentSource("LLM_JUDGE", "reasoning_scorer")
            )
        
        # Evaluate reasoning quality
        result = self.reasoning_judge(
            inputs=inputs,
            agent_reasoning=best_reasoning
        )
        
        return Feedback(
            value=result.value,
            rationale=f"Reasoning quality: {result.value:.2f}",
            source=AssessmentSource("LLM_JUDGE", "reasoning_scorer")
        )

    def _extract_reasoning_from_messages(self, messages):
        """Extract agent reasoning from chat messages"""
        for message in messages:
            if message.get('role') == 'assistant':
                content = message.get('content', '')
                # Look for reasoning patterns
                if 'think' in content.lower() or 'reason' in content.lower():
                    return content
        return None
```

### Pattern 3: Multi-Turn Conversation Scorer
Evaluate tool usage across multi-turn conversations:

```python
class ConversationFlowScorer(Scorer):
    def __init__(self):
        super().__init__()

    def __call__(self, *, inputs=None, outputs=None, expectations=None, trace=None):
        # Analyze tool usage patterns across the conversation
        tool_spans = trace.search_spans(span_type=SpanType.TOOL)
        
        # Group tools by conversation turn
        turns = self._group_tools_by_turn(tool_spans)
        
        # Evaluate conversation flow
        flow_score = self._evaluate_conversation_flow(turns)
        
        return Feedback(
            value=flow_score,
            rationale=f"Conversation flow score: {flow_score:.2f}",
            source=AssessmentSource("LLM_JUDGE", "conversation_scorer")
        )

    def _group_tools_by_turn(self, tool_spans):
        """Group tool calls by conversation turn"""
        turns = {}
        for span in tool_spans:
            turn_id = span.get_attribute('conversation_turn', 0)
            if turn_id not in turns:
                turns[turn_id] = []
            turns[turn_id].append(span)
        return turns

    def _evaluate_conversation_flow(self, turns: dict) -> float:
        """Evaluate how well tools are used across conversation"""
        if not turns:
            return 1.0  # No tools used, neutral score
        
        scores = []
        for turn_id, turn_tools in turns.items():
            turn_score = self._evaluate_turn_quality(turn_tools)
            scores.append(turn_score)
        
        return sum(scores) / len(scores)

    def _evaluate_turn_quality(self, turn_tools: list) -> float:
        """Evaluate tool usage quality for a single turn"""
        if len(turn_tools) == 0:
            return 1.0  # No tools, assume good
        elif len(turn_tools) == 1:
            return 0.9  # Single tool usage typically good
        else:
            # Multiple tools - check if they're complementary
            tool_names = [span.get_attribute(SpanAttributeKey.OUTPUTS).get('name') 
                         for span in turn_tools]
            return self._evaluate_tool_complementarity(tool_names)

    def _evaluate_tool_complementarity(self, tool_names: list) -> float:
        """Check if tools complement each other"""
        # Define complementary tool pairs
        complementary_pairs = [
            {'python_executor', 'doc_retriever'},  # Code + docs
            {'translator', 'summarizer'},  # Translation + summary
        ]
        
        tool_set = set(tool_names)
        for pair in complementary_pairs:
            if pair.issubset(tool_set):
                return 0.9  # Good complementarity
        
        # Penalize redundant or conflicting tools
        if len(set(tool_names)) < len(tool_names):
            return 0.6  # Duplicate tools
        
        return 0.7  # Neutral score
```

## Reference Files

- [MLflow Custom Scorers](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/concepts/scorers) - Official scorer documentation
- [MLflow Tracing](https://docs.databricks.com/aws/en/mlflow3/genai/tracing) - Trace collection and analysis
- [LLM Judges](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/concepts/judges) - Judge implementation patterns

## Common Issues

| Issue | Solution |
|-------|----------|
| **Spans not captured** | Ensure `mlflow.langchain.autolog()` is called before agent execution |
| **Tool response parsing fails** | Verify tool output format matches expected JSON structure |
| **LLM judge inconsistent** | Use more specific prompts and provide examples in judge definition |
| **Performance impact** | Run custom scorers on sample data first, optimize judge prompts |
| **Complex span analysis** | Break complex scorers into smaller, focused evaluation functions |

## Key Takeaways

1. **Span-Level Analysis** - Go beyond end-to-end evaluation by analyzing individual trace spans
2. **Tool Usage Validation** - Compare expected vs actual tool usage using LLM judges
3. **Custom Scorer Classes** - Extend the Scorer base class for complex evaluation logic
4. **Feedback Objects** - Return structured feedback with rationale and assessment sources
5. **Iterative Improvement** - Start with simple scorers and gradually add complexity

## Advanced Implementation Patterns

### Composite Scorer Pattern
Combine multiple scorers for comprehensive evaluation:

```python
class CompositeAgentScorer(Scorer):
    def __init__(self, scorers: list, weights: list = None):
        super().__init__()
        self.scorers = scorers
        self.weights = weights or [1.0] * len(scorers)

    def __call__(self, *, inputs=None, outputs=None, expectations=None, trace=None):
        scores = []
        rationales = []
        
        for scorer in self.scorers:
            feedback = scorer(inputs=inputs, outputs=outputs, 
                            expectations=expectations, trace=trace)
            scores.append(feedback.value if isinstance(feedback.value, (int, float)) else 0.5)
            rationales.append(feedback.rationale)
        
        # Weighted average
        weighted_score = sum(s * w for s, w in zip(scores, self.weights)) / sum(self.weights)
        
        return Feedback(
            value=weighted_score,
            rationale=f"Composite score: {weighted_score:.2f}. Details: {'; '.join(rationales)}",
            source=AssessmentSource("COMPOSITE", "agent_evaluation")
        )

# Usage
composite_scorer = CompositeAgentScorer([
    AgentToolUsageScorer(tools_config),
    ToolExecutionQualityScorer(quality_criteria),
    AgentReasoningScorer()
], weights=[0.4, 0.4, 0.2])
```

### Automated Retraining Trigger
Use scorer results to trigger model retraining:

```python
def evaluate_and_retrain_agent(agent_model, evaluation_data, threshold=0.7):
    """Evaluate agent and trigger retraining if performance drops"""
    
    # Run evaluation with custom scorers
    results = mlflow.genai.evaluate(
        data=evaluation_data,
        scorers=["correctness", tool_usage_scorer, reasoning_scorer],
        model=agent_model
    )
    
    # Check if performance is below threshold
    avg_score = results.overall_score()
    
    if avg_score < threshold:
        print(f"Agent performance {avg_score:.2f} below threshold {threshold}")
        
        # Trigger retraining workflow
        trigger_retraining_pipeline(agent_model, evaluation_data)
        
        return True  # Retraining triggered
    
    return False  # No retraining needed
```

## Performance Optimization

### Efficient Span Processing
Optimize span analysis for large traces:

```python
def process_spans_efficiently(trace, span_types=None):
    """Process spans with early filtering and caching"""
    
    # Cache frequently accessed attributes
    span_cache = {}
    
    # Filter spans early
    relevant_spans = []
    all_spans = trace.search_spans(span_type=span_types) if span_types else trace.spans
    
    for span in all_spans:
        span_id = span.span_id
        
        # Cache span attributes
        if span_id not in span_cache:
            span_cache[span_id] = {
                'inputs': span.get_attribute(SpanAttributeKey.INPUTS),
                'outputs': span.get_attribute(SpanAttributeKey.OUTPUTS),
                'attributes': span.attributes
            }
        
        relevant_spans.append(span_cache[span_id])
    
    return relevant_spans
```

### Batch Evaluation
Evaluate multiple traces efficiently:

```python
def batch_evaluate_traces(traces, scorer, batch_size=10):
    """Evaluate multiple traces in batches"""
    
    results = []
    for i in range(0, len(traces), batch_size):
        batch = traces[i:i + batch_size]
        
        # Process batch in parallel if possible
        batch_results = []
        for trace in batch:
            try:
                feedback = scorer(trace=trace)
                batch_results.append({
                    'trace_id': trace.trace_id,
                    'score': feedback.value,
                    'rationale': feedback.rationale
                })
            except Exception as e:
                batch_results.append({
                    'trace_id': trace.trace_id,
                    'error': str(e)
                })
        
        results.extend(batch_results)
        
        # Progress logging
        print(f"Processed {min(i + batch_size, len(traces))}/{len(traces)} traces")
    
    return results
```

## When to Use This Skill

- Building production AI agents with tool-calling capabilities
- Debugging agent tool selection and execution issues
- Implementing automated agent quality monitoring
- Creating custom evaluation metrics for specialized agents
- Setting up continuous improvement pipelines for agent performance

## Integration with CI/CD

### Automated Evaluation Pipeline
```yaml
# .github/workflows/evaluate-agent.yml
name: Evaluate Agent Performance

on:
  pull_request:
    branches: [ main ]

jobs:
  evaluate:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Setup Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'
    
    - name: Install dependencies
      run: pip install mlflow databricks-sdk
      
    - name: Run evaluation
      env:
        DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
      run: |
        python -c "
        import mlflow.genai
        from custom_scorers import AgentToolUsageScorer
        
        # Load evaluation data
        eval_data = mlflow.genai.load_evaluation_data('gs://eval-data/agent_eval.jsonl')
        
        # Run evaluation
        results = mlflow.genai.evaluate(
            data=eval_data,
            scorers=[AgentToolUsageScorer(tools_config), 'correctness'],
            model_type='databricks-agent'
        )
        
        # Fail PR if score too low
        if results.overall_score() < 0.8:
            exit(1)
        "
```

## Related Skills

- mlflow-tracing
- llm-evaluation-techniques
- agent-frameworks-langchain
- automated-testing-ai-systems
- model-monitoring-production