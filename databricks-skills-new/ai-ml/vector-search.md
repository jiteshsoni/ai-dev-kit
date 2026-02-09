---
name: vector-search
description: "Semantic search and RAG with Databricks Vector Search."
tags: ["vector-search", "rag", "embeddings", "semantic-search"]
---

# Vector Search

## Overview

Build semantic search and RAG applications with Databricks Vector Search.

## Quick Start

### Create Vector Index

```python
# Create delta sync index
spark.sql("""
    CREATE VECTOR INDEX vector_index
    ON TABLE catalog.schema.documents
    COLUMNS embedding
    USING delta_sync
""")
```

### Query Index

```python
# Semantic search
results = spark.sql("""
    SELECT * FROM vector_index
    WHERE embedding_similarity(query_embedding, text_embedding) > 0.8
""")
```

## Common Patterns

### Pattern 1: RAG Pipeline

```python
# 1. Embed query
query_embedding = embed(query)

# 2. Retrieve relevant docs
relevant_docs = search_vector_index(query_embedding)

# 3. Generate response
response = llm.generate(context=relevant_docs, question=query)
```

### Pattern 2: Hybrid Search

```python
# Combine keyword + semantic
results = spark.sql("""
    SELECT * FROM documents
    WHERE text LIKE '%keyword%'
    OR embedding_similarity(query_vec, embedding) > 0.7
""")
```
