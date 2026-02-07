---
name: "Embedding Model Cost Optimization"
description: "Optimize embedding costs by choosing right model size, caching embeddings, batch processing, and using Foundation Model APIs efficiently."
author: "Databricksters"
url: "https://www.databricksters.com/p/understanding-embedding-model-pricing-on-databrick"
date: "2025"
tags: ["embeddings", "cost-optimization", "foundation-models", "vector-search", "databricks"]
---

# Embedding Model Cost Optimization

## Overview

Embedding models charge per token processed. Optimize costs: cache embeddings in Delta tables, batch requests, choose appropriate model size (small vs large), and reuse embeddings across applications.

## Quick Start

```python
# Cost-efficient embedding pattern
from databricks_langchain import DatabricksEmbeddings

# Choose model based on use case
embeddings_small = DatabricksEmbeddings(endpoint="bge-small-en")  # Cheaper
embeddings_large = DatabricksEmbeddings(endpoint="bge-large-en")  # Better quality

# Cache embeddings in Delta table
df_with_embeddings = spark.table("documents").withColumn(
    "embedding",
    ai_query("bge-small-en", col("text"))
)
df_with_embeddings.write.mode("append").saveAsTable("cached_embeddings")

# Reuse cached embeddings
cached = spark.table("cached_embeddings")
```

## Common Patterns

### Pattern 1: Batch Processing

```python
# Batch embed to reduce API calls
texts = ["doc1", "doc2", "doc3", ...]
embeddings = embeddings_model.embed_documents(texts)  # Single API call

# vs inefficient per-document calls
# for text in texts:
#     embedding = embeddings_model.embed_query(text)  # Multiple API calls
```

## FAQ

**Q: How much do embeddings cost?**  
A: Varies by model. BGE-small: ~$0.0001/1K tokens. BGE-large: ~$0.0004/1K tokens.

**Q: Should I always cache?**  
A: Yes for static content. No for dynamic/changing content.
