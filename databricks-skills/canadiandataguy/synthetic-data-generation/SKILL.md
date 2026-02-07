---
name: "Spark Streaming: Generating Synthetic Data at Scale"
description: "Generate terabytes of test data quickly and cheaply using Spark for load testing and development."
author: "Canadian Data Guy"
url: "https://www.youtube.com/watch?v=[from transcript]"
date: "2025-11-29"
tags: ["spark", "synthetic-data", "testing", "data-generation", "performance"]
---

# Generating Synthetic Data at Scale

## Overview

Testing streaming applications requires realistic data volumes. Generate terabytes of synthetic data quickly and cheaply (<$5/TB) using Spark's built-in data generation capabilities.

**Key Tool**: `dbldatagen` (Databricks Labs Data Generator)

## Quick Start

### Install Data Generator

```python
# Install if not available
%pip install dbldatagen
```

### Basic Data Generation

```python
import dbldatagen as dg

# Define dataset
ds = (dg.DataGenerator(spark, name="test_data", rows=1000000, partitions=20)
    .withIdOutput()
    .withColumn("name", "string", template=r"\w{5,10}")
    .withColumn("amount", "decimal", minValue=1, maxValue=1000, random=True)
    .withColumn("timestamp", "timestamp", begin="2024-01-01", end="2024-12-31")
    .withColumn("category", "string", values=["A", "B", "C"])
)

# Generate
df = ds.build()
df.write.format("delta").saveAsTable("test_table")
```

### Generate Streaming Data

```python
# For streaming tests, use rate source
stream_df = (spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 1000)
    .option("numPartitions", 10)
    .load()
    .withColumn("user_id", (rand() * 1000).cast("int"))
    .withColumn("event_type", expr("CASE WHEN rand() < 0.5 THEN 'click' ELSE 'view' END"))
)

# Write to test topic
test_stream = (stream_df
    .select(to_json(struct("*")).alias("value"))
    .writeStream
    .format("kafka")
    .option("topic", "test-events")
    .option("checkpointLocation", "/tmp/test-checkpoint")
    .start()
)
```

## Common Patterns

### Pattern 1: IoT Device Data

```python
# Generate realistic IoT data
iot_data = (dg.DataGenerator(spark, rows=10000000, partitions=50)
    .withColumn("device_id", "string", template=r"device-\d{5}")
    .withColumn("sensor_type", "string", values=["temp", "pressure", "humidity"])
    .withColumn("reading", "float", minValue=0.0, maxValue=100.0, random=True)
    .withColumn("timestamp", "timestamp", expr="current_timestamp() - rand() * 86400")
    .withColumn("location", "string", values=["factory-1", "factory-2", "warehouse"])
)

df = iot_data.build()
df.write.format("delta").partitionBy("location").saveAsTable("iot_sensors")
```

### Pattern 2: E-commerce Transactions

```python
# Generate e-commerce data with realistic distributions
ecom_data = (dg.DataGenerator(spark, rows=5000000, partitions=20)
    .withColumn("order_id", "string", template=r"ORD-\d{8}")
    .withColumn("customer_id", "int", minValue=1, maxValue=100000, random=True)
    .withColumn("product_id", "int", minValue=1, maxValue=50000, random=True)
    .withColumn("quantity", "int", minValue=1, maxValue=10, random=True, 
                distribution=dg.distributions.Gamma(2.0, 2.0))
    .withColumn("price", "decimal", minValue=10, maxValue=1000, random=True)
    .withColumn("order_date", "date", begin="2024-01-01", end="2024-12-31")
)

df = ecom_data.build()
```

### Pattern 3: Load Testing Dataset

```python
# Generate 1TB for load testing
# ~$5 in compute costs

# Configuration for large dataset
tb_generator = (dg.DataGenerator(spark, 
    rows=10_000_000_000,  # 10B rows
    partitions=1000
)
    .withColumn("id", "long", uniqueValues=10_000_000_000)
    .withColumn("data", "string", template=r"\w{100}")  # Large string
    .withColumn("metadata", "map<string,string>", 
                expr="map('key1', 'value1', 'key2', 'value2')")
)

# Write to S3/ADLS
tb_generator.build().write \
    .format("parquet") \
    .option("compression", "zstd") \
    .save("s3://bucket/load-test-data/")
```

## Reference Files

### Data Types and Options

| Type | Options | Example |
|------|---------|---------|
| `string` | template, values | `template=r"\w{5,10}"` |
| `int` | minValue, maxValue, random | `minValue=1, maxValue=100` |
| `decimal` | minValue, maxValue, scale | `minValue=0.0, maxValue=1000.0` |
| `timestamp` | begin, end | `begin="2024-01-01"` |
| `date` | begin, end | `begin="2024-01-01"` |

### Distributions

```python
# Built-in distributions
dg.distributions.Normal(mean, stddev)
dg.distributions.Gamma(shape, scale)
dg.distributions.Beta(alpha, beta)
dg.distributions.Exponential(rate)
```

## Common Issues

| Issue | Solution |
|-------|----------|
| **Out of memory** | Increase partitions; reduce row count per partition |
| **Too slow** | Add more cluster nodes; increase parallelism |
| **Unrealistic data** | Use distributions; add correlations between columns |
| **Storage costs** | Use compression (zstd, snappy); delete after testing |

## Advanced Tips

### Cost Estimation

```python
# ~$5 per TB generated
# Time: ~15 minutes per TB on modest cluster

# Formula:
# Cost = (cluster_cost_per_hour * generation_time)
# Example: $2/hour * 2.5 hours = $5
```

### Data Realism

```python
# Make data more realistic with:
# 1. Correlations
generator.withColumn("premium_customer", "boolean", expr="amount > 500")

# 2. Nulls
generator.withColumn("optional_field", "string", 
                    percentNulls=0.1, template=r"\w{5}")

# 3. Skew
generator.withColumn("customer_id", "int", 
                    distribution=dg.distributions.Exponential(0.1))
```

### Reproducibility

```python
# Set seed for reproducible datasets
generator = dg.DataGenerator(spark, rows=1000000, seed=42)
# Same seed = same data every time
```

## FAQ

**Q: How much data can I generate?**
A: Limited by storage and time. 10TB+ possible with large clusters.

**Q: Can I generate streaming data continuously?**
A: Yes, use `spark.readStream.format("rate")` for continuous generation.

**Q: Is the data really random?**
A: Pseudo-random with seed option for reproducibility.

**Q: Can I use this for production?**
A: For testing only. Don't use synthetic data in production pipelines.

**Q: How do I validate the generated data?**
A: Use standard Spark/DataFrame operations to check distributions, nulls, ranges.