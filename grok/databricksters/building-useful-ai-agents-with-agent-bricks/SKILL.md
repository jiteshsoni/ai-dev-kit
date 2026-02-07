---
name: "databricks-agent-bricks"
description: "Complete guide to building AI agents with Databricks Agent Bricks, including Knowledge Assistants, RAG implementation, and human feedback loops."
---

# Databricks Agent Bricks: Building Production AI Agents

## Overview

This skill covers Databricks Agent Bricks, a low-code framework for building specialized AI agents with domain-specific capabilities. Learn about Knowledge Assistants for RAG applications, agent evaluation techniques, human feedback loops, and continuous improvement through Agent Learning from Human Feedback (ALHF). Includes practical implementations for document Q&A, custom knowledge bases, and production agent deployment.

## Quick Start

### Prerequisites Check
Ensure your workspace meets Agent Bricks requirements:

```python
# Required workspace settings verification
required_settings = {
    "mosaic_ai_agent_bricks_enabled": True,
    "production_monitoring_mlflow_enabled": True,
    "serverless_compute_enabled": True,
    "unity_catalog_enabled": True,
    "mosaic_ai_model_serving_access": True,
    "system_ai_schema_access": True,
    "serverless_budget_policy": "non-zero",
    "supported_region": True
}

def verify_agent_bricks_prerequisites():
    """Verify all prerequisites are met"""
    missing_prereqs = []
    
    # Check each prerequisite
    for setting, required in required_settings.items():
        if not required:
            missing_prereqs.append(setting)
    
    if missing_prereqs:
        raise EnvironmentError(f"Missing prerequisites: {missing_prereqs}")
    
    print("✅ All Agent Bricks prerequisites verified")
    return True
```

### Create Knowledge Assistant
Build a RAG agent for document Q&A:

```python
from databricks.sdk import WorkspaceClient
from databricks.agent_bricks import KnowledgeAssistant

def create_knowledge_assistant(name: str, data_sources: list, description: str):
    """
    Create a Knowledge Assistant for document-based Q&A
    """
    
    w = WorkspaceClient()
    
    # Define data sources (Unity Catalog volumes or vector search indexes)
    data_config = {
        "sources": [
            {
                "type": "unity_catalog_volume",
                "path": source_path,
                "format": "auto"  # Supports txt, pdf, md, ppt, docx
            } for source_path in data_sources
        ],
        "embedding_model": "databricks-gte-large-en",
        "vector_search_index": f"catalog.schema.{name}_index"
    }
    
    # Create Knowledge Assistant
    assistant = KnowledgeAssistant.create(
        name=name,
        description=description,
        data_sources=data_config,
        model="databricks-meta-llama-3-1-70b-instruct",  # Foundation model
        guardrails_enabled=True,
        rate_limiting_enabled=False  # Disable for development
    )
    
    # Deploy the assistant
    endpoint = assistant.deploy(
        endpoint_name=f"{name}_endpoint",
        scale_to_zero_enabled=True,
        workload_size="Small"
    )
    
    return assistant, endpoint

# Usage
assistant, endpoint = create_knowledge_assistant(
    name="databricks_docs_assistant",
    data_sources=["catalog.schema.docs_volume"],
    description="Answers questions about Databricks documentation and best practices"
)
```

### Query Your Agent
Test the deployed Knowledge Assistant:

```python
import requests

def query_knowledge_assistant(endpoint_url: str, question: str, token: str):
    """Query the deployed Knowledge Assistant"""
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "messages": [
            {
                "role": "user",
                "content": question
            }
        ],
        "max_tokens": 500,
        "temperature": 0.1  # Lower temperature for factual responses
    }
    
    response = requests.post(endpoint_url, json=payload, headers=headers)
    response.raise_for_status()
    
    result = response.json()
    
    # Extract response with citations
    answer = result["choices"][0]["message"]["content"]
    citations = result.get("citations", [])
    
    return {
        "answer": answer,
        "citations": citations,
        "usage": result.get("usage", {})
    }

# Example query
result = query_knowledge_assistant(
    endpoint_url="https://workspace.databricks.com/serving-endpoints/databricks_docs_assistant/invocations",
    question="How do I optimize Spark streaming jobs?",
    token=dbutils.secrets.get("scope", "databricks-token")
)

print(f"Answer: {result['answer']}")
print(f"Citations: {result['citations']}")
```

## Common Patterns

### Pattern 1: Custom Knowledge Base Agent
Build an agent for internal documentation and policies:

```python
from databricks.agent_bricks import KnowledgeAssistant
from databricks.vector_search import VectorSearchClient

def create_enterprise_knowledge_agent():
    """Create agent for enterprise knowledge management"""
    
    # Prepare data sources
    data_sources = [
        {
            "type": "unity_catalog_volume",
            "path": "catalog.knowledge_base.docs",
            "chunk_size": 500,
            "chunk_overlap": 50
        },
        {
            "type": "unity_catalog_volume", 
            "path": "catalog.knowledge_base.policies",
            "chunk_size": 300,
            "chunk_overlap": 30
        }
    ]
    
    # Create vector search index
    vs_client = VectorSearchClient()
    index = vs_client.create_index(
        name="enterprise_knowledge_index",
        primary_key="doc_id",
        embedding_source="databricks-gte-large-en",
        schema={
            "doc_id": "string",
            "content": "string",
            "title": "string",
            "category": "string",
            "last_updated": "timestamp"
        }
    )
    
    # Configure Knowledge Assistant
    assistant_config = {
        "name": "Enterprise Knowledge Assistant",
        "description": "Answers questions about company policies, procedures, and documentation",
        "data_sources": data_sources,
        "vector_index": index,
        "model": "databricks-meta-llama-3-1-70b-instruct",
        "temperature": 0.0,  # Deterministic responses for policies
        "max_context_length": 4000,
        "guardrails": {
            "sensitive_topics": ["salary", "personal_info"],
            "response_filtering": True
        }
    }
    
    assistant = KnowledgeAssistant.create(**assistant_config)
    return assistant
```

### Pattern 2: Multi-Document Q&A Agent
Handle complex queries across multiple document types:

```python
def create_multi_document_agent():
    """Agent that can answer questions requiring information from multiple sources"""
    
    # Define specialized data sources
    data_sources = [
        {
            "name": "technical_docs",
            "path": "catalog.docs.technical",
            "specialization": "technical_implementation",
            "priority": "high"
        },
        {
            "name": "user_guides",
            "path": "catalog.docs.user_guides", 
            "specialization": "user_experience",
            "priority": "medium"
        },
        {
            "name": "api_reference",
            "path": "catalog.docs.api",
            "specialization": "developer_tools",
            "priority": "high"
        }
    ]
    
    # Advanced RAG configuration
    rag_config = {
        "retrieval_strategy": "hybrid",  # Combine semantic and keyword search
        "reranking_enabled": True,
        "multi_source_synthesis": True,  # Combine info from multiple sources
        "citation_style": "comprehensive",  # Detailed source attribution
        "confidence_scoring": True  # Score answer confidence
    }
    
    assistant = KnowledgeAssistant.create(
        name="Multi-Document Assistant",
        data_sources=data_sources,
        rag_config=rag_config,
        model="databricks-meta-llama-3-1-405b-instruct"  # Larger model for complex synthesis
    )
    
    return assistant
```

### Pattern 3: Agent Learning from Human Feedback (ALHF)
Implement continuous improvement through human feedback:

```python
from databricks.agent_bricks import LabelingSession, FeedbackCollector

def implement_human_feedback_loop(assistant, evaluation_questions):
    """Set up human feedback collection and learning"""
    
    # Create evaluation questions
    questions = [
        "How do I set up Databricks Asset Bundles?",
        "What are the best practices for Delta Lake optimization?",
        "How do I implement real-time streaming with Spark?",
        "What are common issues with Databricks Jobs?",
        "How do I monitor Databricks SQL warehouse performance?"
    ]
    
    # Create labeling session
    session = LabelingSession.create(
        name=f"{assistant.name}_evaluation_{datetime.now().strftime('%Y%m%d')}",
        assistant=assistant,
        questions=questions,
        evaluators=["sme@databricks.com", "architect@databricks.com"]
    )
    
    # Generate initial responses
    session.generate_responses()
    
    # Share with SMEs for feedback
    feedback_url = session.get_feedback_url()
    print(f"Share this URL with SMEs for feedback: {feedback_url}")
    
    return session

def sync_feedback_and_improve(assistant, session):
    """Sync human feedback and update agent"""
    
    # End labeling session
    session.end_session()
    
    # Sync feedback for agent learning
    sync_result = assistant.sync_feedback(
        labeling_session=session,
        learning_method="alhf",  # Agent Learning from Human Feedback
        improvement_focus=["accuracy", "completeness", "tone"]
    )
    
    # Monitor improvement
    metrics = sync_result.get_improvement_metrics()
    print(f"Agent improved by: {metrics}")
    
    return sync_result

# Usage
assistant = create_knowledge_assistant("databricks_docs", [...])
session = implement_human_feedback_loop(assistant, evaluation_questions)
# Wait for SME feedback...
sync_result = sync_feedback_and_improve(assistant, session)
```

## Reference Files

- [Agent Bricks Documentation](https://docs.databricks.com/en/large-language-models/agent-bricks.html) - Official Agent Bricks guide
- [Knowledge Assistant](https://docs.databricks.com/en/large-language-models/knowledge-assistant.html) - RAG agent implementation
- [Agent Learning from Human Feedback](https://docs.databricks.com/en/large-language-models/alhf.html) - Continuous improvement

## Common Issues

| Issue | Solution |
|-------|----------|
| **Prerequisites not met** | Check workspace settings and enable required features |
| **Data source access denied** | Verify Unity Catalog permissions on volumes/indexes |
| **Poor retrieval quality** | Adjust chunk size, overlap, and embedding model |
| **Inconsistent responses** | Lower temperature, enable guardrails, use deterministic models |
| **Feedback not syncing** | Ensure labeling session is properly ended before sync |
| **High latency** | Use smaller models, enable caching, optimize index |

## Key Takeaways

1. **Low-Code Agent Development** - Agent Bricks provides no-code RAG agents with optimizations
2. **Knowledge Assistant Focus** - Specialized for document Q&A with citations and reliability
3. **RAG Optimization** - Automatic index optimization and LLM selection
4. **Human Feedback Loop** - ALHF enables continuous improvement without retraining
5. **Multi-Source Synthesis** - Combine information from multiple document types
6. **Production Ready** - Built-in monitoring, guardrails, and scaling

## Agent Types in Agent Bricks

| Agent Type | Use Case | Key Features |
|------------|----------|--------------|
| **Knowledge Assistant** | Document Q&A, RAG | Citations, multi-source synthesis |
| **Code Assistant** | Programming help | Code generation, debugging |
| **Data Assistant** | Analytics queries | SQL generation, data exploration |
| **Workflow Assistant** | Process automation | Task orchestration, API integration |

## Evaluation and Monitoring

### Automated Evaluation Setup
```python
from databricks.agent_bricks import EvaluationSuite

def setup_agent_evaluation(assistant, test_dataset):
    """Set up comprehensive agent evaluation"""
    
    evaluation_config = {
        "metrics": [
            "correctness",      # Factual accuracy
            "completeness",     # Answer thoroughness  
            "relevance",        # Query relevance
            "citation_quality", # Source attribution
            "response_time",    # Performance
            "safety"           # Content safety
        ],
        "test_dataset": test_dataset,
        "baseline_comparison": True,
        "automated_scoring": True
    }
    
    evaluator = EvaluationSuite.create(
        name=f"{assistant.name}_evaluation",
        assistant=assistant,
        config=evaluation_config
    )
    
    # Run evaluation
    results = evaluator.run_evaluation()
    
    # Generate report
    report = evaluator.generate_report(
        format="html",
        include_recommendations=True
    )
    
    return results, report
```

### Continuous Monitoring
```python
from databricks.agent_bricks import MonitoringDashboard

def setup_agent_monitoring(assistant):
    """Set up production monitoring for agent performance"""
    
    monitoring_config = {
        "metrics": {
            "response_quality": {
                "threshold": 0.8,
                "alert_on_drop": True
            },
            "usage_patterns": {
                "track_query_types": True,
                "anomaly_detection": True
            },
            "performance": {
                "latency_threshold_ms": 5000,
                "error_rate_threshold": 0.05
            }
        },
        "feedback_collection": {
            "enabled": True,
            "sampling_rate": 0.1  # Collect feedback on 10% of queries
        },
        "automated_retraining": {
            "enabled": True,
            "trigger_threshold": 0.75,  # Retrain if score drops below
            "min_samples": 100  # Minimum feedback samples before retraining
        }
    }
    
    dashboard = MonitoringDashboard.create(
        assistant=assistant,
        config=monitoring_config
    )
    
    return dashboard
```

## Advanced Customization

### Custom Retrieval Strategies
```python
from databricks.vector_search import VectorSearchIndex

def create_hybrid_search_assistant():
    """Agent with custom hybrid search capabilities"""
    
    # Create custom vector index with metadata filtering
    index_config = {
        "name": "hybrid_knowledge_index",
        "embedding_model": "databricks-gte-large-en",
        "schema": {
            "doc_id": "string",
            "content": "string", 
            "metadata": {
                "category": "string",
                "importance": "float",
                "last_updated": "timestamp"
            }
        },
        "search_config": {
            "hybrid_search": True,  # Combine semantic and keyword search
            "reranking": True,
            "filtering": True
        }
    }
    
    index = VectorSearchIndex.create(**index_config)
    
    # Custom retrieval function
    def hybrid_retrieve(query, filters=None, top_k=5):
        """Custom retrieval with filtering and reranking"""
        
        # Base semantic search
        semantic_results = index.search(
            query=query,
            filters=filters,
            limit=top_k * 2  # Get more for reranking
        )
        
        # Keyword search for exact matches
        keyword_results = index.search(
            query=f'"{query}"',  # Exact phrase search
            search_type="keyword",
            limit=top_k
        )
        
        # Combine and rerank
        combined_results = combine_results(
            semantic_results, 
            keyword_results,
            rerank_method="reciprocal_rank_fusion"
        )
        
        return combined_results[:top_k]
    
    # Create assistant with custom retrieval
    assistant = KnowledgeAssistant.create(
        name="Hybrid Search Assistant",
        retrieval_function=hybrid_retrieve,
        model="databricks-meta-llama-3-1-70b-instruct"
    )
    
    return assistant
```

## When to Use This Skill

- Building RAG applications for document Q&A
- Creating enterprise knowledge assistants
- Implementing customer support chatbots
- Developing internal documentation assistants
- Setting up continuous improvement through human feedback
- Deploying production-ready AI agents

## Cost Optimization

### Model Selection Strategy
```python
def select_optimal_model(query_complexity: str, cost_budget: float):
    """Select appropriate model based on requirements and budget"""
    
    model_options = {
        "simple_factual": {
            "model": "databricks-meta-llama-3-1-8b-instruct",
            "cost_per_1k_tokens": 0.0015,
            "quality_score": 0.8
        },
        "complex_reasoning": {
            "model": "databricks-meta-llama-3-1-70b-instruct", 
            "cost_per_1k_tokens": 0.009,
            "quality_score": 0.95
        },
        "enterprise_scale": {
            "model": "databricks-meta-llama-3-1-405b-instruct",
            "cost_per_1k_tokens": 0.027,
            "quality_score": 0.98
        }
    }
    
    if query_complexity == "high" and cost_budget > 0.01:
        return model_options["enterprise_scale"]
    elif query_complexity == "medium" or cost_budget > 0.005:
        return model_options["complex_reasoning"]
    else:
        return model_options["simple_factual"]
```

## Related Skills

- databricks-vector-search
- rag-implementation-patterns
- mlflow-model-evaluation
- production-ai-monitoring
- human-feedback-loops