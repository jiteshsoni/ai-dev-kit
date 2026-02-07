---
name: "pandas-pyarrow-udfs-spark"
description: "Complete guide to Pandas and PyArrow UDFs in Apache Spark: vectorized operations, Arrow batch processing, performance optimization, and best practices for 10-100x speedups."
---

# Pandas & PyArrow UDFs in Apache Spark: Vectorized Performance

## Overview

This skill covers Pandas and PyArrow UDFs in Apache Spark, which enable 10-100x performance improvements over traditional Python UDFs through vectorized batch processing and zero-copy Arrow data transfer. Learn about different UDF types (Series-to-Series, Iterator patterns, applyInPandas, mapInPandas), batch size tuning, memory management, and when to use each pattern for optimal performance.

## Quick Start

### Basic Pandas UDF: Series to Series
Simple vectorized transformation:

```python
from pyspark.sql.functions import pandas_udf
from typing import Iterator
import pandas as pd

@pandas_udf('double')
def pandas_plus_one(series: pd.Series) -> pd.Series:
    """Vectorized operation on a Series"""
    return series + 1

# Usage
df = spark.range(10)
result = df.select(pandas_plus_one("id").alias("id_plus_one"))
result.show()
```

### Iterator Pattern for Memory Efficiency
Process large partitions in batches:

```python
from typing import Iterator
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf('double')
def process_in_batches(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """Process data in batches to manage memory"""
    for series in iterator:
        # Process each batch
        yield series * 2

# Usage
df = spark.range(1000000)
result = df.select(process_in_batches("id"))
```

## Common Patterns

### Pattern 1: Series-to-Series Transformation
Most common pattern for column transformations:

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd
import numpy as np

@pandas_udf('double')
def calculate_distance(lat1: pd.Series, lon1: pd.Series, 
                       lat2: pd.Series, lon2: pd.Series) -> pd.Series:
    """Calculate Haversine distance between two points"""
    R = 6371  # Earth radius in km
    
    lat1_rad = np.radians(lat1)
    lat2_rad = np.radians(lat2)
    delta_lat = np.radians(lat2 - lat1)
    delta_lon = np.radians(lon2 - lon1)
    
    a = (np.sin(delta_lat / 2) ** 2 +
         np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(delta_lon / 2) ** 2)
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    
    return R * c

# Usage
df = spark.createDataFrame([
    (40.7128, -74.0060, 34.0522, -118.2437),  # NYC to LA
    (51.5074, -0.1278, 48.8566, 2.3522)      # London to Paris
], ["lat1", "lon1", "lat2", "lon2"])

result = df.withColumn("distance_km", 
    calculate_distance("lat1", "lon1", "lat2", "lon2"))
result.show()
```

### Pattern 2: Iterator Pattern with State Initialization
Load expensive resources once per partition:

```python
from typing import Iterator
import pandas as pd
import pickle

@pandas_udf('double')
def predict_with_model(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """Load model once per partition, use for all batches"""
    # Load model once per partition (not per batch)
    with open('/path/to/model.pkl', 'rb') as f:
        model = pickle.load(f)
    
    try:
        for series in iterator:
            # Use model for prediction
            predictions = model.predict(series.values.reshape(-1, 1))
            yield pd.Series(predictions)
    finally:
        # Cleanup if needed
        del model

# Usage
df = spark.range(100000)
result = df.select(predict_with_model("id").alias("prediction"))
```

### Pattern 3: Multiple Series Input
Process multiple columns together:

```python
from typing import Iterator, Tuple
import pandas as pd

@pandas_udf('double')
def complex_calculation(iterator: Iterator[Tuple[pd.Series, pd.Series]]) -> Iterator[pd.Series]:
    """Process multiple input columns"""
    for price, quantity in iterator:
        # Vectorized calculation across columns
        result = price * quantity * 1.1  # Add 10% tax
        yield result

# Usage
df = spark.createDataFrame([
    (10.0, 5),
    (20.0, 3),
    (15.0, 4)
], ["price", "quantity"])

result = df.withColumn("total_with_tax", 
    complex_calculation("price", "quantity"))
result.show()
```

### Pattern 4: applyInPandas for Grouped Operations
Split-apply-combine pattern:

```python
import pandas as pd

def calculate_group_stats(pdf: pd.DataFrame) -> pd.DataFrame:
    """Calculate statistics per group"""
    return pd.DataFrame({
        'group_id': [pdf['group_id'].iloc[0]],
        'mean_value': [pdf['value'].mean()],
        'max_value': [pdf['value'].max()],
        'min_value': [pdf['value'].min()],
        'count': [len(pdf)]
    })

# Usage
df = spark.createDataFrame([
    ('A', 10), ('A', 20), ('A', 30),
    ('B', 15), ('B', 25)
], ["group_id", "value"])

result = df.groupBy("group_id").applyInPandas(
    calculate_group_stats,
    schema="group_id string, mean_value double, max_value double, min_value double, count long"
)
result.show()
```

### Pattern 5: mapInPandas for Complex Transformations
Transform entire partitions with flexible output:

```python
from typing import Iterator
import pandas as pd

def expand_records(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Expand records - output size may differ from input"""
    for pdf in iterator:
        # Example: explode a column
        expanded_rows = []
        for _, row in pdf.iterrows():
            items = row['items'].split(',')
            for item in items:
                expanded_rows.append({
                    'id': row['id'],
                    'item': item.strip()
                })
        yield pd.DataFrame(expanded_rows)

# Usage
df = spark.createDataFrame([
    (1, 'apple,banana,cherry'),
    (2, 'dog,cat')
], ["id", "items"])

result = df.mapInPandas(
    expand_records,
    schema="id int, item string"
)
result.show()
```

## Reference Files

- [Pandas UDFs Documentation](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html) - Official Spark guide
- [Arrow-Optimized UDFs](https://www.databricks.com/blog/arrow-optimized-python-udfs-apache-sparktm-35) - Databricks blog
- [Pandas Function APIs](https://learn.microsoft.com/en-us/azure/databricks/pandas/pandas-function-apis) - Azure Databricks guide

## Common Issues

| Issue | Solution |
|-------|----------|
| **Out of memory** | Use Iterator pattern, reduce batch size, increase executor memory |
| **Slow performance** | Ensure vectorized operations, avoid Python loops |
| **Type errors** | Add proper type hints, check Arrow type compatibility |
| **Model loading overhead** | Use Iterator pattern to load once per partition |
| **2GB Arrow limit** | Reduce batch size via `spark.sql.execution.arrow.maxRecordsPerBatch` |

## Key Takeaways

1. **Performance Hierarchy**: Native Spark > Pandas UDFs > Arrow-Optimized UDFs > Traditional UDFs
2. **Vectorization**: Use pandas/NumPy operations, avoid Python loops
3. **Batch Size**: Default 10k rows; tune based on memory constraints
4. **Iterator Pattern**: Use for memory efficiency and expensive initialization
5. **Type Hints**: Required for proper Arrow conversion
6. **Memory Management**: Size executors for worst-case batch size

## Performance Tuning

### Configure Arrow Batch Size
```python
# Default: 10,000 records per batch
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "5000")

# Smaller batches: less memory, more overhead
# Larger batches: more memory, less overhead
```

### Enable Arrow Optimization
```python
# Enable Arrow-based columnar data transfer
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Use Arrow for Pandas conversion
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
```

### Memory Configuration
```python
# Size executors for Pandas UDF workloads
# Rule: executor memory should handle 2-3x batch size
# Example: 10k rows × 1KB per row = 10MB per batch
# Allocate at least 2-3GB per executor for safety

spark.conf.set("spark.executor.memory", "4g")
spark.conf.set("spark.executor.memoryFraction", "0.8")
```

## Best Practices

### 1. Prefer Native Spark First
```python
# Use native Spark functions when possible
df.select(col("value") * 1.1)  # Native Spark - fastest

# Only use Pandas UDF when necessary
@pandas_udf('double')
def custom_logic(series: pd.Series) -> pd.Series:
    # Complex logic not available in Spark
    return series.apply(lambda x: complex_function(x))
```

### 2. Vectorize Operations
```python
# Good: Vectorized
@pandas_udf('double')
def good_udf(series: pd.Series) -> pd.Series:
    return series * 2 + 1  # Vectorized

# Bad: Row-by-row
@pandas_udf('double')
def bad_udf(series: pd.Series) -> pd.Series:
    return series.apply(lambda x: x * 2 + 1)  # Slower
```

### 3. Handle Timestamps Correctly
```python
@pandas_udf('timestamp')
def process_timestamps(series: pd.Series) -> pd.Series:
    """Process timestamps - keep in UTC"""
    # Convert to UTC if needed
    return pd.to_datetime(series, utc=True)
```

### 4. Resource Cleanup
```python
from typing import Iterator
import pandas as pd

@pandas_udf('double')
def udf_with_resources(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """Properly manage resources"""
    resource = expensive_initialization()
    
    try:
        for series in iterator:
            result = process_with_resource(series, resource)
            yield result
    finally:
        # Always cleanup
        cleanup_resource(resource)
```

## Performance Benchmarking

### Measure UDF Performance
```python
def benchmark_udf(df, udf_function, udf_name):
    """Benchmark UDF performance"""
    import time
    
    start = time.time()
    result = df.select(udf_function("id"))
    result.write.format("noop").mode("overwrite").save()  # No-op write for timing
    end = time.time()
    
    duration = end - start
    row_count = df.count()
    throughput = row_count / duration
    
    print(f"{udf_name}:")
    print(f"  Duration: {duration:.2f}s")
    print(f"  Rows: {row_count:,}")
    print(f"  Throughput: {throughput:,.0f} rows/sec")
    
    return duration

# Usage
df = spark.range(1000000)

# Benchmark different approaches
native_time = benchmark_udf(df, col("id") * 2, "Native Spark")
pandas_time = benchmark_udf(df, pandas_multiply_two("id"), "Pandas UDF")

print(f"Speedup: {native_time / pandas_time:.2f}x")
```

## When to Use Pandas UDFs

### Use Pandas UDFs When:
- Complex calculations not available in Spark SQL
- Need to leverage pandas/NumPy ecosystem
- Working with time series data
- Custom ML model inference
- Text processing with pandas

### Use Native Spark When:
- Simple transformations
- Standard aggregations
- Built-in functions available
- Performance is critical

## Related Skills

- spark-performance-optimization
- arrow-data-format
- python-udfs-spark
- vectorized-operations