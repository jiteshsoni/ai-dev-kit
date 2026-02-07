---
name: databricks-vector-search-similarity-scores-deep-di
description: 
---

# Databricks Vector Search Similarity Scores Deep Dive

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/databricks-vector-search-similarity

## Tags
performance, databricks, clustering, ai

## Full Content

# Databricks Vector Search Similarity Scores Deep Dive

**Source:** https://www.databricksters.com/p/databricks-vector-search-similarity

---



Databricks Vector Search Similarity Scores Deep Dive 

Databricksters

Subscribe

Sign in

AI &amp; ML

Databricks Vector Search Similarity Scores Deep Dive 

Joshua Eason

Apr 09, 2025

3

Share

Have you noticed unexpected results from Databricks Vector Search’s similarity_search? If you’re coming from a cosine similarity background, the scores might seem puzzlingly misaligned with your expectations. In this blog, we’ll dive deep into why this happens, explain the key differences between similarity metrics, and provide a solution to bridge this gap.

Cosine Similarity

Thanks for reading Databricksters! Subscribe for free to receive new posts and support my work.

Subscribe

Let’s start with a quick refresher on cosine similarity. The cosine similarity between two vectors a and b defined as:

\(\text{cosine_sim}(a, b) = \frac{\vec{a} \cdot \vec{b}}{\|\vec{a}\| \cdot \|\vec{b}\|}
\)

Geometric Interpretation

Cosine similarity gives us a clear geometric intuition for vector similarity by measuring the angle between them in vector space. If two vectors point in the same direction, their cosine similarity is 1; if they are orthogonal (at 90°), it is 0; and if they point in opposite directions, it is –1. This works because cosine similarity is simply the cosine of the angle between them in their vector space.

Think of it like comparing the orientation of arrows: two arrows pointing in the same direction, no matter how long, are perfectly aligned (cosine = 1), while arrows at right angles share no alignment (cosine = 0), and arrows pointing in opposite directions are fully misaligned (cosine = –1). Unlike Euclidean distance, which can be influenced by the length of the vectors, cosine similarity purely reflects alignment, making it especially useful in high-dimensional spaces where magnitude may vary but direction carries semantic meaning.

Here is a sample implementation

import numpy as np

def cosine_similarity(a, b):
 a = np.array(a)
 b = np.array(b)
 dot_product = np.dot(a, b)
 norm_a = np.linalg.norm(a)
 norm_b = np.linalg.norm(b)
 return dot_product / (norm_a * norm_b)

In practice, many of us rely on the scikit-learn implementation.

import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

# Example vectors
a = np.array([1, 2, 3])
b = np.array([4, 5, 6])

# Reshape for sklearn (expects 2D arrays)
similarity = cosine_similarity(a.reshape(1, -1), b.reshape(1, -1))[0][0]

Interestingly, when the input vectors are normalized to unit length so that 

\(\|\vec{a}\| = \|\vec{b}\| = 1\)

cosine similarity reduces to a simple dot product:

\(\text{cosine_sim}(a, b) = \vec{a} \cdot \vec{b}\)

In cases where we want to set a bound, cosine_sim ∈

[0, 1], we may apply a linear transformation such as 

\(bounded\_cosine = \frac{cosine\_sim +1}{2}\)

which preserves order, and simply squashes the outputs of your cosine similarity function into the new range. This can be very beneficial in circumstances where your similarity scores are used as intermediate inputs to downstream models that have strict boundary requirements, or are sensitive to negative values.

Another variation that you may have seen before is the cosine distance, which is just 1 

− 

cosine_sim. This is often used in conjunction with the other transformations, but provides an interpretation based on the distance (smaller is closer) instead of the alignment (higher is closer).

Databricks’ Similarity Computation

It is possible that you have noticed that neither of these methods produce the scores that you see when you perform a similarity_search using the Databricks VectorSearchIndex class. In contrast, from the 

Official Databricks

Documentation,

 we can see that databricks uses the following formula to compute similarity:

\(\text{similarity} = \frac{1}{(1 + \text{dist(q, x)}^2)}\)

Where

\(\text{dist}(q, x) = \sqrt{(q_1 - x_1)^2 + (q_2 - x_2)^2 + \cdots + (q_d - x_d)^2}\)

While this formula relies on the euclidean distance, you can see that it is not just the euclidean distance. In contrast to cosine similarity, which compares 

direction

, this score relies on a linear transformation of Euclidean distance, which compares 

position

. If the vectors are not normalized, the algorithm sensitive to both the 

magnitude 

and 

alignment 

of the vectors. For example:

Two vectors pointing in the same direction but with very different lengths may have high cosine similarity but large Euclidean distance.

Conversely, two vectors that are numerically close (in terms of component values) but not aligned may have small Euclidean distance but low cosine similarity.

This positional sensitivity makes the score potentially misleading in semantic spaces (like embeddings) where 

direction 

is more meaningful than length.

For clarity, here’s the Databricks similarity scoring function referenced in our examples:

def euclidean_distance(q, x):
 &quot;&quot;&quot;Calculate Euclidean distance between vectors q and x&quot;&quot;&quot;
 q = np.array(q)
 x = np.array(x)
 return np.linalg.norm(q - x)

def databricks_similarity_score(q, x):
 &quot;&quot;&quot;similarity score based on the formula: 1 / (1 + dist(q, x)^2)&quot;&quot;&quot;
 distance = euclidean_distance(q, x)
 return 1 / (1 + distance ** 2)

Side Note - Hybrid Similarity Score

There is an additional step for providing the score if a hybrid search, if using the 

query_type='HYBRID'

 argument when calling the 

VectorSearchIndex.similarity_search

 method, a composite BM25 Keyword and Vector similarity is used. This score

relies on an algorithm called Reciprocal Rank Fusion (RRF), and aggregates rankings from several sources into a single, ranking. For more detailed information about this, please see 

Ref (1).

Vector Normalization

Vector normalization rescales a vector to have unit length (i.e., L2 norm of 1). This process preserves the vector’s direction while standardizing its magnitude, which is crucial for reliable similarity comparisons.

Given a vector a, its normalized form is:

\(\hat{a} = \frac{\vec{a}}{\|\vec{a}\|}\)

Normalization essentially projects all vectors onto the unit hypersphere in n-dimensional space, allowing us to compare them purely by their direction and eliminating magnitude differences that can distort similarity measures.

Here is the vanilla python implementation

import numpy as np

def l2_normalize(vector):
 vector = np.array(vector)
 norm = np.linalg.norm(vector)
 if norm == 0:
 return vector # Avoid division by zero
 return vector / norm

Though in practice, most of us rely on the scikit learn implementation

from sklearn.preprocessing import normalize
import numpy as np

# Each row will be treated as a separate vector
X = np.array([[1, 2, 3], [4, 5, 6]])

# Normalize along rows (axis=1)
X_normalized = normalize(X, norm='l2', axis=1)

Why Normalize?

So, why is normalization so important?

This sketch illustrates the geometric difference between cosine similarity and the Databricks similarity score, which is derived from squared Euclidean distance.

The green angles between vectors (e.g., cos(a, b) and cos(b, c)) represent cosine similarity, which depends purely on direction. In contrast, the orange segments represent Euclidean distances between vector tips — and since the Databricks score is computed as a linear transformation of the distance, these distances directly influence the similarity score.

When embeddings are not normalized, the magnitude of the vectors affects the result. Even though Vector b (raw) is directionally aligned with Vector a, its large magnitude causes the straight-line distance dist(a, b_raw) to be quite large — leading to a low Databricks score. At the same time, Vector c, which is closer in space but less aligned in direction, will have a smaller Euclidean distance to a, and therefore a higher similarity scor



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
