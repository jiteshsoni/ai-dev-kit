---
name: "Vector Search Similarity Score Interpretation"
description: "Understand and optimize Vector Search similarity scores (cosine, dot product, euclidean) for accurate retrieval thresholds."
author: "Databricksters"
url: "https://www.databricksters.com/p/databricks-vector-search-similarity-scores-deep-di"
date: "2025"
tags: ["vector-search", "similarity", "embeddings", "rag", "databricks"]
---

# Vector Search Similarity Scores

## Overview

Vector Search returns similarity scores based on distance metric: cosine similarity (-1 to 1), dot product, or euclidean distance. Understanding score ranges and setting appropriate thresholds improves RAG relevance.

## Quick Start

```python
from databricks.vector_search.client import VectorSearchClient

client = VectorSearchClient()

# Query with score interpretation
results = client.get_index("catalog.schema.index").similarity_search(
    query_text="What is Databricks?",
    columns=["id", "text"],
    num_results=5
)

# Interpret scores (cosine similarity)
for result in results.get("result", {}).get("data_array", []):
    score = result["_distance"]  # Cosine similarity: 0-1 range
    if score > 0.8:
        print(f"Highly relevant: {result['text'][:100]}")
    elif score > 0.6:
        print(f"Moderately relevant: {result['text'][:100]}")
    else:
        print(f"Low relevance: {result['text'][:100]}")
```

## Common Patterns

### Pattern 1: Adaptive Thresholding

```python
def filter_by_relevance(results, min_score=0.7):
    """Only return results above threshold"""
    return [r for r in results if r["_distance"] >= min_score]

# Higher threshold for factual queries
factual_results = filter_by_relevance(results, min_score=0.85)

# Lower threshold for exploratory queries
exploratory_results = filter_by_relevance(results, min_score=0.6)
```

## FAQ

**Q: What's a good similarity threshold?**  
A: 0.7-0.8 for cosine similarity. Test with your data to find optimal cutoff.

**Q: Why are all my scores similar?**  
A: Embeddings may be poorly calibrated. Try different embedding model or query reformulation.
