---
name: batch-inference
description: "Scale model inference with batch processing patterns on Databricks."
tags: ["batch-inference", "model-serving", "scale", "predictions"]
---

# Batch Inference

## Overview

Apply ML models to large datasets efficiently using batch processing.

## Quick Start

### Spark UDF Pattern

```python
import mlflow
from pyspark.sql.functions import struct, col

# Create UDF
predict_udf = mlflow.pyfunc.spark_udf(
    spark,
    model_uri="models:/catalog.schema.model/1",
    result_type="double"
)

# Apply to DataFrame
predictions = (features_df
    .withColumn("prediction", predict_udf(struct(col("feature1"), col("feature2"))))
)
```

## Common Patterns

### Pattern 1: Pandas UDF

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("double")
def predict_batch(features: pd.DataFrame) -> pd.Series:
    return model.predict(features)

df.withColumn("prediction", predict_batch(struct("*"))).show()
```

### Pattern 2: ai_query SQL

```sql
-- Batch inference in SQL
CREATE TABLE predictions AS
SELECT *,
    ai_query(
        endpoint => 'model_endpoint',
        request => named_struct('features', feature_col)
    ) AS prediction
FROM input_table;
```
