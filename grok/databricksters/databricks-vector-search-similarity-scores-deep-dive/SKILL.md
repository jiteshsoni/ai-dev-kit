---
name: "vector-search-similarity-scores"
description: "Deep dive into Databricks Vector Search similarity scoring: understanding L2 distance vs cosine similarity, normalization strategies, and converting between metrics."
---

# Databricks Vector Search Similarity Scores: Complete Guide

## Overview

This skill covers Databricks Vector Search similarity scoring mechanisms, explaining the differences between cosine similarity and Databricks' L2-based scoring, the importance of vector normalization, rank-order equivalence, and how to convert between similarity metrics. Includes practical examples for embedding models (BGE, GTE) and conversion formulas.

## Quick Start

### Normalize Vectors for Consistent Results
Ensure vectors are normalized before indexing:

```python
from sklearn.preprocessing import normalize
import numpy as np

def normalize_embeddings_for_indexing(embeddings):
    """Normalize embeddings before indexing in Databricks Vector Search"""
    
    # Convert to numpy array if needed
    if isinstance(embeddings, list):
        embeddings = np.array(embeddings)
    
    # L2 normalize (unit length)
    normalized = normalize(embeddings, norm='l2', axis=1)
    
    return normalized

# Usage
embeddings = np.array([
    [1.0, 2.0, 3.0],
    [4.0, 5.0, 6.0],
    [7.0, 8.0, 9.0]
])

normalized_embeddings = normalize_embeddings_for_indexing(embeddings)
print("Normalized embeddings shape:", normalized_embeddings.shape)
print("L2 norm of first vector:", np.linalg.norm(normalized_embeddings[0]))  # Should be ~1.0
```

### Convert Databricks Score to Cosine Similarity
Recover cosine similarity from Databricks scores:

```python
def cosine_from_databricks_score(databricks_score):
    """
    Convert Databricks similarity score to cosine similarity.
    
    Requires: Both query and indexed vectors must be L2-normalized.
    
    Args:
        databricks_score: Similarity score from Databricks Vector Search
        
    Returns:
        Cosine similarity value (range: -1 to 1)
    """
    return 1 - 0.5 * (1 / databricks_score - 1)

# Usage
db_score = 0.9517  # Example Databricks score
cosine_sim = cosine_from_databricks_score(db_score)
print(f"Databricks score: {db_score:.4f}")
print(f"Cosine similarity: {cosine_sim:.4f}")
```

## Common Patterns

### Pattern 1: Handle Model-Specific Normalization
Different embedding models have different normalization behavior:

```python
def prepare_embeddings_by_model(model_name: str, embeddings):
    """Prepare embeddings based on model normalization behavior"""
    
    # Models that produce normalized embeddings by default
    normalized_models = ['BGE', 'BAAI-General-Embedding', 'bge-large-en-v1.5']
    
    # Models that don't normalize by default
    non_normalized_models = ['GTE', 'General-Text-Embedding', 'gte-large']
    
    embeddings_array = np.array(embeddings) if isinstance(embeddings, list) else embeddings
    
    if model_name in normalized_models:
        print(f"{model_name} produces normalized embeddings - use as-is")
        return embeddings_array
    elif model_name in non_normalized_models:
        print(f"{model_name} does NOT normalize - normalizing now")
        return normalize(embeddings_array, norm='l2', axis=1)
    else:
        # Unknown model - normalize to be safe
        print(f"Unknown model {model_name} - normalizing to be safe")
        return normalize(embeddings_array, norm='l2', axis=1)

# Usage
bge_embeddings = prepare_embeddings_by_model('BGE', raw_embeddings)
gte_embeddings = prepare_embeddings_by_model('GTE', raw_embeddings)
```

### Pattern 2: Consistent Normalization at Index and Query Time
Ensure same normalization strategy at both times:

```python
from databricks.vector_search import VectorSearchIndex
from sklearn.preprocessing import normalize

class NormalizedVectorSearch:
    """Wrapper ensuring consistent normalization"""
    
    def __init__(self, index_name: str, normalize_vectors: bool = True):
        self.index_name = index_name
        self.normalize_vectors = normalize_vectors
        self.index = VectorSearchIndex(index_name)
    
    def index_vectors(self, vectors, metadata):
        """Index vectors with consistent normalization"""
        if self.normalize_vectors:
            vectors = normalize(np.array(vectors), norm='l2', axis=1)
        
        # Index vectors
        self.index.upsert(vectors, metadata)
    
    def search(self, query_vector, top_k=10):
        """Search with consistent normalization"""
        if self.normalize_vectors:
            query_vector = normalize(np.array([query_vector]), norm='l2', axis=1)[0]
        
        # Search
        results = self.index.similarity_search(
            query_vector,
            top_k=top_k
        )
        
        # Convert scores to cosine similarity if needed
        for result in results:
            result['cosine_similarity'] = cosine_from_databricks_score(result['score'])
        
        return results

# Usage
vs = NormalizedVectorSearch("my_index", normalize_vectors=True)

# Index with normalization
vs.index_vectors(embeddings, metadata)

# Search with normalization
results = vs.search(query_vector, top_k=10)
```

### Pattern 3: Verify Rank-Order Equivalence
Validate that normalization preserves ranking:

```python
from sklearn.metrics.pairwise import cosine_similarity

def verify_rank_order_equivalence(query_vector, candidate_vectors):
    """Verify that normalized vectors preserve rank order"""
    
    # Normalize all vectors
    query_norm = normalize(np.array([query_vector]), norm='l2', axis=1)[0]
    candidates_norm = normalize(np.array(candidate_vectors), norm='l2', axis=1)
    
    # Calculate cosine similarities
    cosine_scores = cosine_similarity(
        query_norm.reshape(1, -1),
        candidates_norm
    )[0]
    
    # Calculate Databricks scores
    def databricks_similarity(q, x):
        distance = np.linalg.norm(q - x)
        return 1 / (1 + distance ** 2)
    
    db_scores = [databricks_similarity(query_norm, cand) for cand in candidates_norm]
    
    # Get rankings
    cosine_ranking = np.argsort(cosine_scores)[::-1]
    db_ranking = np.argsort(db_scores)[::-1]
    
    # Verify rank order equivalence
    rank_order_match = np.array_equal(cosine_ranking, db_ranking)
    
    print("Rank Order Verification:")
    print(f"  Cosine ranking: {cosine_ranking}")
    print(f"  Databricks ranking: {db_ranking}")
    print(f"  Rankings match: {rank_order_match}")
    
    return rank_order_match

# Usage
query = np.array([1.0, 2.0, 3.0])
candidates = [
    [0.0, 1.0, 0.0],
    [1.0, 1.0, 0.0],
    [0.2, 0.8, 0.0]
]

verify_rank_order_equivalence(query, candidates)
```

## Reference Files

- [Vector Search Documentation](https://docs.databricks.com/en/generative-ai/vector-search.html) - Official guide
- [Similarity Algorithms](https://docs.databricks.com/en/generative-ai/vector-search.html#keyword-search-algorithm) - Scoring formulas
- [Hybrid Search](https://docs.databricks.com/en/generative-ai/vector-search.html#hybrid-search) - BM25 + Vector search

## Common Issues

| Issue | Solution |
|-------|----------|
| **Unexpected similarity scores** | Normalize vectors before indexing and querying |
| **Rankings don't match cosine** | Ensure consistent normalization (L2) |
| **GTE model scores seem off** | GTE doesn't normalize - normalize manually |
| **BGE scores work fine** | BGE normalizes by default - no action needed |
| **Hybrid search scores confusing** | Uses RRF (Reciprocal Rank Fusion) - different formula |

## Key Takeaways

1. **Databricks Formula**: `similarity = 1 / (1 + dist(q, x)^2)` where dist is Euclidean distance
2. **Normalization Required**: L2-normalize vectors for rank-order equivalence with cosine
3. **Model Differences**: BGE normalizes by default, GTE does not
4. **Rank-Order Equivalence**: Normalized L2 distance ranking = cosine similarity ranking
5. **Conversion Formula**: `cosine = 1 - 0.5 * (1/score - 1)` for normalized vectors
6. **Consistency**: Use same normalization at index time and query time

## Mathematical Background

### Databricks Similarity Score
```python
def databricks_similarity_score(query_vector, candidate_vector):
    """Calculate Databricks similarity score"""
    distance = np.linalg.norm(np.array(query_vector) - np.array(candidate_vector))
    return 1 / (1 + distance ** 2)

# Properties:
# - Range: (0, 1] (approaches 0 as distance increases)
# - Higher score = more similar
# - Based on Euclidean distance (positional)
```

### Cosine Similarity
```python
def cosine_similarity_custom(a, b):
    """Calculate cosine similarity"""
    a = np.array(a)
    b = np.array(b)
    dot_product = np.dot(a, b)
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    return dot_product / (norm_a * norm_b)

# Properties:
# - Range: [-1, 1]
# - Measures angle (direction), not position
# - For normalized vectors: cosine = dot product
```

### Relationship When Normalized
```python
# When vectors are L2-normalized:
# ||a|| = ||b|| = 1
# 
# Euclidean distance squared:
# ||a - b||^2 = ||a||^2 + ||b||^2 - 2*a·b
#             = 1 + 1 - 2*cosine(a, b)
#             = 2(1 - cosine(a, b))
#
# Databricks score:
# score = 1 / (1 + ||a - b||^2)
#       = 1 / (1 + 2(1 - cosine))
#       = 1 / (3 - 2*cosine)
#
# Solving for cosine:
# cosine = 1 - 0.5 * (1/score - 1)
```

## Complete Example: End-to-End Workflow

```python
from databricks.vector_search import VectorSearchIndex
from sklearn.preprocessing import normalize
import numpy as np

def complete_vector_search_workflow():
    """Complete workflow with proper normalization"""
    
    # Step 1: Generate embeddings (example)
    documents = [
        "Machine learning is fascinating",
        "Deep learning uses neural networks",
        "Python is a programming language"
    ]
    
    # Simulate embeddings (in practice, use actual model)
    embeddings = np.random.randn(len(documents), 384)
    
    # Step 2: Normalize before indexing
    normalized_embeddings = normalize(embeddings, norm='l2', axis=1)
    
    # Step 3: Create index and upsert
    index = VectorSearchIndex("my_index")
    metadata = [{"doc_id": i, "text": doc} for i, doc in enumerate(documents)]
    index.upsert(normalized_embeddings, metadata)
    
    # Step 4: Query with normalization
    query_text = "artificial intelligence"
    query_embedding = np.random.randn(384)  # In practice, use model
    query_normalized = normalize(np.array([query_embedding]), norm='l2', axis=1)[0]
    
    # Step 5: Search
    results = index.similarity_search(query_normalized, top_k=3)
    
    # Step 6: Convert scores to cosine similarity
    for result in results:
        db_score = result['score']
        cosine = cosine_from_databricks_score(db_score)
        result['cosine_similarity'] = cosine
        print(f"Doc: {result['metadata']['text']}")
        print(f"  Databricks score: {db_score:.4f}")
        print(f"  Cosine similarity: {cosine:.4f}")
    
    return results

# Usage
results = complete_vector_search_workflow()
```

## Hybrid Search Considerations

```python
def understand_hybrid_search_scoring():
    """Hybrid search uses RRF (Reciprocal Rank Fusion)"""
    
    # Hybrid search combines:
    # 1. BM25 keyword search ranking
    # 2. Vector similarity search ranking
    # 
    # Uses Reciprocal Rank Fusion to combine rankings
    # Formula: RRF_score = sum(1 / (k + rank)) for each ranking
    
    print("Hybrid Search Scoring:")
    print("  - Combines BM25 (keyword) + Vector (semantic)")
    print("  - Uses Reciprocal Rank Fusion (RRF)")
    print("  - Different scoring than pure vector search")
    print("  - Better for queries needing both keyword and semantic matching")

# Usage
understand_hybrid_search_scoring()
```

## When to Use This Skill

- Understanding why Databricks scores differ from cosine similarity
- Implementing proper vector normalization workflows
- Converting between similarity metrics
- Working with BGE, GTE, or other embedding models
- Debugging unexpected search results
- Ensuring consistent normalization across index and query

## Related Skills

- vector-search-implementation
- embedding-normalization
- similarity-metrics
- semantic-search-optimization