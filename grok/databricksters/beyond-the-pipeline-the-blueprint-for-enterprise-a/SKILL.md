---
name: "databricks-enterprise-ai-platform"
description: "Complete blueprint for building scalable enterprise AI platforms on Databricks with Unity Catalog governance and three core model inference patterns."
---

# Enterprise AI Platform Blueprint on Databricks

## Overview

This skill provides a comprehensive blueprint for building enterprise-scale AI platforms on Databricks, moving beyond brittle pipelines to unified, governable architectures. Learn the three core model inference patterns, governance-first design principles, and trade-off analysis for choosing the right approach. Includes Unity Catalog foundation, AI_QUERY functions, and Spark UDF implementations for real-time and batch inference at scale.

## Quick Start

### Foundation: Unity Catalog Setup
Establish governance before building models:

```sql
-- Enable Unity Catalog for all AI assets
-- Create catalogs for different environments
CREATE CATALOG IF NOT EXISTS main;
CREATE SCHEMA IF NOT EXISTS main.production_models;
CREATE SCHEMA IF NOT EXISTS main.feature_store;

-- Grant appropriate permissions
GRANT USE_CATALOG ON CATALOG main TO `data-scientists`;
GRANT USE_SCHEMA ON SCHEMA main.production_models TO `ml-engineers`;
```

### Pattern Selection Decision Tree
Choose your inference pattern:

```python
def choose_inference_pattern(requirements):
    """
    Decision framework for selecting AI inference patterns
    """
    latency = requirements.get('latency_seconds', 60)
    volume = requirements.get('daily_predictions', 1000)
    complexity = requirements.get('model_complexity', 'simple')

    if latency < 1:  # Sub-second requirements
        return "Pattern 1: Real-Time Model Serving"
    elif volume > 100000 and complexity == 'simple':
        return "Pattern 3: Embedded Spark UDF"
    else:  # Balanced requirements
        return "Pattern 2: Serverless Batch Inference"
```

## Common Patterns

### Pattern 1: Real-Time Model Serving (AI_QUERY)
For low-latency, request-response interactions:

```sql
-- Deploy model to serving endpoint first
-- Then use AI_QUERY for real-time predictions

SELECT
    customer_id,
    ai_query(
        endpoint => 'prod_customer_churn_model',
        request => named_struct(
            'account_age', account_age,
            'monthly_spend', monthly_spend,
            'support_tickets', support_tickets
        ),
        returnType => 'DOUBLE'
    ) AS churn_probability
FROM main.gold.customer_features
WHERE customer_id = 'A-12345';
```

### Pattern 2: Serverless Batch Inference (AI_QUERY)
For scalable batch processing with simplicity:

```sql
-- Apply model to entire dataset
CREATE OR REPLACE TABLE main.gold.customer_churn_predictions AS
SELECT
    customer_id,
    account_age,
    monthly_spend,
    ai_query(
        endpoint => 'prod_customer_churn_model',
        request => named_struct(
            'account_age', c.account_age,
            'monthly_spend', c.monthly_spend,
            'support_tickets', c.support_tickets
        ),
        returnType => 'DOUBLE'
    ) AS churn_probability
FROM main.gold.customer_features AS c;

-- Add confidence intervals for decision-making
SELECT
    customer_id,
    churn_probability,
    CASE
        WHEN churn_probability > 0.8 THEN 'High Risk'
        WHEN churn_probability > 0.5 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS risk_category
FROM main.gold.customer_churn_predictions;
```

### Pattern 3: Embedded Spark UDF (Maximum Throughput)
For high-volume batch processing with co-located execution:

```python
import mlflow
from pyspark.sql.functions import col, struct

def create_embedded_inference_udf(model_uri, env_manager='virtualenv'):
    """
    Create high-performance Spark UDF for model inference
    """
    return mlflow.pyfunc.spark_udf(
        spark,
        model_uri=model_uri,
        env_manager=env_manager,
        result_type='double'
    )

# Load model from Unity Catalog
model_uri = "models:/main.production_models.customer_churn@champion"
predict_udf = create_embedded_inference_udf(model_uri)

# Read features and apply predictions
features_df = spark.read.table("main.gold.customer_features")

predictions_df = features_df.withColumn(
    "churn_probability",
    predict_udf(
        struct(
            col("account_age"),
            col("monthly_spend"),
            col("support_tickets")
        )
    )
)

# Write results with governance
predictions_df.write.mode("overwrite").saveAsTable("main.gold.customer_predictions")
```

### Advanced: Multi-Model Ensemble Pattern
Combine multiple models for improved accuracy:

```sql
-- Ensemble predictions using multiple models
CREATE OR REPLACE TABLE main.gold.customer_ensemble_predictions AS
SELECT
    customer_id,
    -- Average predictions from multiple models
    (model_a_score + model_b_score + model_c_score) / 3.0 AS ensemble_score,
    -- Confidence based on agreement
    CASE
        WHEN ABS(model_a_score - model_b_score) < 0.1
             AND ABS(model_b_score - model_c_score) < 0.1
        THEN 'High Confidence'
        ELSE 'Low Confidence'
    END AS prediction_confidence
FROM (
    SELECT
        customer_id,
        ai_query('model_a_endpoint', features_struct, 'DOUBLE') AS model_a_score,
        ai_query('model_b_endpoint', features_struct, 'DOUBLE') AS model_b_score,
        ai_query('model_c_endpoint', features_struct, 'DOUBLE') AS model_c_score
    FROM main.gold.customer_features
) predictions;
```

## Reference Files

- [Databricks Model Serving](https://docs.databricks.com/aws/en/machine-learning/model-serving/index.html) - Real-time model deployment
- [AI_QUERY Function](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_query.html) - SQL-based model inference
- [MLflow Spark UDF](https://mlflow.org/docs/latest/models.html#spark-udf) - Distributed model execution
- [Unity Catalog Governance](https://docs.databricks.com/aws/en/data-governance/unity-catalog/index.html) - AI asset governance

## Common Issues

| Issue | Solution |
|-------|----------|
| **AI_QUERY permission denied** | Grant `USE_FUNCTION` privilege on ai_query to workspace users |
| **Model endpoint not found** | Ensure model is deployed to serving endpoint with correct name |
| **Spark UDF environment conflicts** | Use virtualenv for simple models, conda for complex dependencies |
| **High latency in batch inference** | Switch to Pattern 3 (Spark UDF) for co-located execution |
| **Model versioning issues** | Use Unity Catalog aliases (@champion, @challenger) for stable references |
| **Cost optimization** | Pattern 3 typically lowest cost, Pattern 1 highest for batch workloads |

## Key Takeaways

1. **Governance First** - Unity Catalog provides unified management of data and AI assets
2. **Three Patterns, One Platform** - Choose based on latency vs throughput requirements
3. **Pattern 1 (Real-Time)** - Sub-second latency using Model Serving endpoints
4. **Pattern 2 (Serverless Batch)** - Simple scaling with AI_QUERY for regular batch jobs
5. **Pattern 3 (Spark UDF)** - Maximum throughput by co-locating models with data
6. **Trade-off Analysis** - Balance latency, cost, complexity, and scale requirements

## Pattern Selection Framework

### When to Use Each Pattern

| Pattern | Latency | Volume | Complexity | Best For |
|---------|---------|--------|------------|----------|
| **Real-Time Serving** | < 1 second | Low-Moderate | Low | APIs, real-time decisions, interactive apps |
| **Serverless Batch** | 10-100 seconds | High | Low | ETL pipelines, scheduled scoring, regular batch jobs |
| **Spark UDF** | 1-10 minutes | Very High | Medium | Massive datasets, cost-sensitive, complex models |

### Cost Optimization Strategies

```python
def optimize_ai_costs(pattern, volume, model_complexity):
    """
    Cost optimization recommendations
    """
    if volume > 1000000:  # Million+ predictions
        return "Pattern 3: Spark UDF for lowest cost per prediction"
    elif volume > 10000:  # Ten thousand+ predictions
        return "Pattern 2: Serverless batch for simplicity"
    else:  # Smaller volumes
        return "Pattern 1: Real-time for flexibility"
```

## Prerequisites Checklist

- [ ] **Databricks Runtime**: 15.4 LTS or above for AI_QUERY
- [ ] **Unity Catalog**: Enabled and configured
- [ ] **Model Format**: MLflow-packaged models in UC registry
- [ ] **Permissions**: USE_FUNCTION on AI_QUERY, model access grants
- [ ] **Compute**: Serverless SQL warehouses (Patterns 1&2), Spark clusters (Pattern 3)
- [ ] **Networking**: VPC configuration for model serving endpoints

## When to Use This Skill

- Designing enterprise ML platforms on Databricks
- Scaling from single models to model factories
- Choosing inference patterns for different workloads
- Implementing governance for AI assets
- Optimizing costs for ML production workloads

## Related Skills

- databricks-model-serving
- unity-catalog-model-governance
- mlflow-model-registry
- spark-ml-production-patterns
- ai-platform-architecture