---
name: "Pandas/PyArrow UDFs Performance Guide"
description: "Achieve 10-100x speedup using vectorized Pandas UDFs with Apache Arrow for batch processing instead of row-by-row Python UDFs."
author: "Databricksters"
url: "https://www.databricksters.com/p/everything-you-ever-wanted-to-know"
date: "2025"
tags: ["pandas", "pyarrow", "udf", "performance", "spark", "databricks"]
---

# Pandas/PyArrow UDFs Performance

## Overview

Pandas UDFs (vectorized UDFs) leverage Apache Arrow for zero-copy data transfer between JVM and Python, processing batches instead of single rows. Delivers 10-100x speedup over traditional Python UDFs by enabling pandas/NumPy vectorized operations. Critical for ML feature engineering and complex transformations at scale.

**Use this skill when:** Writing custom transformations in PySpark, optimizing slow Python UDFs, or processing large datasets with Python logic.

## Quick Start

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

# ❌ SLOW: Traditional Python UDF (row-by-row)
from pyspark.sql.functions import udf
@udf("double")
def slow_multiply(x):
    return x * 2
# Processes 1 row at a time, pickle overhead

# ✅ FAST: Pandas UDF (vectorized batches)
@pandas_udf("double")
def fast_multiply(series: pd.Series) -> pd.Series:
    return series * 2
# Processes 10,000 rows at once, Arrow transfer

df = spark.range(1000000)
result = df.select(fast_multiply("id"))

# Speedup: 10-100x faster
```

## Common Patterns

### Pattern 1: Series to Series (Element-wise Operations)

```python
@pandas_udf("string")
def clean_text(text: pd.Series) -> pd.Series:
    """Vectorized text cleaning"""
    return text.str.lower().str.strip()

# Apply to DataFrame
df_cleaned = df.withColumn("cleaned_text", clean_text("raw_text"))
```

### Pattern 2: Iterator of Series (Memory-Efficient)

```python
from typing import Iterator

@pandas_udf("double")
def process_batches(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """Process in batches, load model once"""
    # Load expensive resource once per partition
    model = load_ml_model()
    
    # Process each batch
    for batch in iterator:
        predictions = model.predict(batch.values.reshape(-1, 1))
        yield pd.Series(predictions)

df_predicted = df.withColumn("prediction", process_batches("feature"))
```

**Benefits:**
- Load model/resources once per partition
- Process batches to manage memory
- Amortize initialization cost

### Pattern 3: Multiple Columns Input

```python
from typing import Tuple

@pandas_udf("double")
def calculate_score(iterator: Iterator[Tuple[pd.Series, pd.Series]]) -> Iterator[pd.Series]:
    """Process multiple input columns"""
    for feature1, feature2 in iterator:
        # Vectorized operation on multiple columns
        score = (feature1 * 0.6) + (feature2 * 0.4)
        yield score

df_scored = df.withColumn(
    "score",
    calculate_score("feature1", "feature2")
)
```

### Pattern 4: GroupBy Aggregation

```python
@pandas_udf("double")
def weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    """Custom aggregation function"""
    return (values * weights).sum() / weights.sum()

# Use with groupBy
result = df.groupBy("category").agg(
    weighted_mean("value", "weight").alias("weighted_avg")
)
```

### Pattern 5: applyInPandas for Complex Group Operations

```python
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define output schema
schema = StructType([
    StructField("category", StringType()),
    StructField("metric1", DoubleType()),
    StructField("metric2", DoubleType())
])

def complex_group_logic(pdf: pd.DataFrame) -> pd.DataFrame:
    """Complex transformation on each group"""
    # Full pandas operations available
    pdf["metric1"] = pdf["value"].rolling(window=7).mean()
    pdf["metric2"] = pdf["value"].expanding().std()
    return pdf[["category", "metric1", "metric2"]]

# Apply to groups
result = df.groupBy("category").applyInPandas(complex_group_logic, schema)
```

**Warning:** Loads entire group into memory - ensure groups are manageable size.

### Pattern 6: mapInPandas for Partition Operations

```python
def process_partition(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Process entire partition with pandas"""
    # Load model once per partition
    model = load_model()
    
    for batch_df in iterator:
        # Complex pandas operations
        batch_df["features"] = create_features(batch_df)
        batch_df["prediction"] = model.predict(batch_df[["features"]])
        yield batch_df

result = df.mapInPandas(process_partition, schema)
```

**Use case:** File parsing, complex transformations, ML inference on partitions.

## Reference Files

- [Pandas UDF Documentation](https://docs.databricks.com/udf/pandas.html)
- [Apache Arrow in Spark](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html)
- [Vectorized UDFs Tutorial](https://www.databricks.com/blog/2020/05/20/new-pandas-udfs-and-python-type-hints-in-the-upcoming-release-of-apache-spark-3-0.html)

## Common Issues

| Issue | Solution |
|-------|----------|
| **Slower than expected** | Check batch size: `spark.sql.execution.arrow.maxRecordsPerBatch`. Default 10K. |
| **OutOfMemoryError** | Reduce batch size or increase executor memory. |
| **Type conversion errors** | Ensure return type matches declared schema exactly. |
| **Complex types unsupported** | Some nested types (ArrayType(TimestampType)) need recent PyArrow. Upgrade or simplify. |
| **Overhead for small data** | Use native Spark functions for small datasets (<1M rows). |
| **Groups too large (applyInPandas)** | Partition data better or use Series-based UDFs instead. |

## Advanced Tips

### Tune Arrow Batch Size

```python
# Default: 10,000 rows per batch
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")

# Larger batches: Better throughput, more memory
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "50000")

# Smaller batches: Less memory, more overhead
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "5000")

# Tune based on:
# - Available executor memory
# - Data complexity
# - UDF computation intensity
```

### Performance Hierarchy

```python
# Fastest to slowest:
# 1. Native Spark functions (best)
df.withColumn("result", col("value") * 2)

# 2. Pandas UDFs (10-100x faster than traditional)
@pandas_udf("double")
def pandas_multiply(s: pd.Series) -> pd.Series:
    return s * 2

# 3. Arrow-optimized Python UDFs (better than pickle)
@udf("double")
def arrow_multiply(x):
    return x * 2

# 4. Traditional Python UDFs (slowest, avoid)
# (same as #3 but without Arrow optimization)
```

**Rule:** Prefer native Spark → Pandas UDF → Arrow UDF → Traditional UDF.

### Benchmark Your UDF

```python
# Measure impact
# Step 1: Baseline without UDF
df.write.format("noop").mode("overwrite").save()

# Step 2: With UDF
df.withColumn("result", my_udf("col")).write.format("noop").mode("overwrite").save()

# Compare Spark UI execution times
```

### Type Hints Best Practices

```python
# Always use type hints for clarity and performance
from typing import Iterator

# Good: Clear types
@pandas_udf("double")
def good_udf(s: pd.Series) -> pd.Series:
    return s * 2

# Better: With iterator for memory efficiency
@pandas_udf("double")
def better_udf(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    for batch in iterator:
        yield batch * 2

# Best: Multiple inputs with type hints
@pandas_udf("double")
def best_udf(iterator: Iterator[Tuple[pd.Series, pd.Series]]) -> Iterator[pd.Series]:
    for col1, col2 in iterator:
        yield col1 + col2
```

## FAQ

**Q: When should I use Pandas UDFs?**  
A: When native Spark functions insufficient and you need pandas/NumPy logic. For <1M rows, overhead may not be worth it.

**Q: What's the performance difference?**  
A: Typically 10-100x faster than traditional Python UDFs. Benchmark your specific use case.

**Q: Can I use scikit-learn in Pandas UDFs?**  
A: Yes! Load model once in iterator pattern, apply to batches.

**Q: What about Spark 3.5 Arrow-optimized UDFs?**  
A: Arrow-optimized UDFs faster than pickle, but Pandas UDFs still faster due to vectorization.

**Q: How do I debug Pandas UDF failures?**  
A: Check: (1) type hints match return types, (2) batch size reasonable, (3) executor memory sufficient.

**Q: Should I always use Pandas UDFs?**  
A: No. Use native Spark functions when possible. Pandas UDFs for complex logic requiring pandas/NumPy.
