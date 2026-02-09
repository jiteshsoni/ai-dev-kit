---
name: synthetic-data
description: "Generate realistic synthetic test data for development and testing."
author: "Canadian Data Guy"
source: "https://www.youtube.com/watch?v=XXXXXXXXXXX"
tags: ["synthetic-data", "testing", "faker", "data-generation"]
---

# Synthetic Data Generation

## Overview

Generate realistic test data using Faker and other libraries.

## Quick Start

```python
from faker import Faker
import pandas as pd

fake = Faker()

# Generate users
data = [{
    "id": fake.uuid4(),
    "name": fake.name(),
    "email": fake.email(),
    "address": fake.address(),
    "created_at": fake.date_time_this_year()
} for _ in range(1000)]

df = spark.createDataFrame(pd.DataFrame(data))
df.write.format("delta").saveAsTable("synthetic.users")
```

## Common Patterns

### Pattern 1: Large-Scale Generation

```python
# Generate millions of rows
from pyspark.sql.functions import udf
from pyspark.sql.types import *

@udf(StringType())
def random_name():
    return fake.name()

# Generate with Spark
df = spark.range(1000000).select(
    random_name().alias("name"),
    # ... more columns
)
```

### Pattern 2: Referential Integrity

```python
# Generate related tables
customers = generate_customers(1000)
orders = generate_orders(customers, 10000)  # Reference valid customer IDs
```
