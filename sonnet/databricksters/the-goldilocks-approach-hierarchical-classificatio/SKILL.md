---
name: "Hierarchical Classification with AI_QUERY"
description: "Implement multi-level classification using AI_QUERY for cost-efficient routing: fast small models for easy cases, expensive large models for hard cases."
author: "Databricksters"
url: "https://www.databricksters.com/p/the-goldilocks-approach-hierarchical-classificatio"
date: "2025"
tags: ["classification", "ai-query", "cost-optimization", "databricks"]
---

# Hierarchical Classification

## Overview

Hierarchical classification routes tasks through models of increasing capability: small/fast models handle obvious cases, large/expensive models handle ambiguous cases. Reduces cost while maintaining accuracy using AI_QUERY function.

## Quick Start

```sql
-- Level 1: Fast small model
WITH level1 AS (
  SELECT 
    text,
    AI_QUERY(
      'databricks-meta-llama-3-1-8b-instruct',
      CONCAT('Classify sentiment as positive/negative/unsure: ', text)
    ) as classification
  FROM reviews
)

-- Level 2: Expensive large model for 'unsure' only
SELECT 
  text,
  CASE 
    WHEN classification != 'unsure' THEN classification
    ELSE AI_QUERY(
      'databricks-meta-llama-3-1-70b-instruct',
      CONCAT('Classify sentiment as positive/negative: ', text)
    )
  END as final_classification
FROM level1
```

## Common Patterns

### Pattern 1: Three-Tier Classification

```sql
-- Tier 1: Tiny model (cheapest)
-- Tier 2: Medium model
-- Tier 3: Large model (most expensive)

WITH tier1 AS (
  SELECT id, text,
    AI_QUERY('tiny-model', text) as result,
    CASE WHEN result LIKE '%unsure%' THEN 1 ELSE 0 END as escalate
  FROM data
),
tier2 AS (
  SELECT id, text,
    AI_QUERY('medium-model', text) as result,
    CASE WHEN result LIKE '%unsure%' THEN 1 ELSE 0 END as escalate
  FROM tier1
  WHERE escalate = 1
),
tier3 AS (
  SELECT id, text,
    AI_QUERY('large-model', text) as result
  FROM tier2
  WHERE escalate = 1
)
SELECT * FROM tier1 WHERE escalate = 0
UNION ALL
SELECT * FROM tier2 WHERE escalate = 0
UNION ALL
SELECT * FROM tier3
```

## FAQ

**Q: How much cost savings?**  
A: 60-80% reduction if 80% of cases handled by small model.
