---
name: everything-you-ever-wanted-to-know-about-pandas-py
description: 
---

# Everything You Ever Wanted to Know about Pandas / PyArrow UDFs in Apache Spark

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/everything-you-ever-wanted-to-know

## Tags
streaming, ai, spark, data-engineering, performance, databricks

## Full Content

# Everything You Ever Wanted to Know about Pandas / PyArrow UDFs in Apache Spark

**Source:** https://www.databricksters.com/p/everything-you-ever-wanted-to-know

---



Everything You Ever Wanted to Know about Pandas / PyArrow UDFs in Apache Spark 

Databricksters

Subscribe

Sign in

Data Engineering

Everything You Ever Wanted to Know about Pandas / PyArrow UDFs in Apache Spark 

Vectorized UDFs, Zero-Copy Arrows &amp; 100× Speed-Ups

Canadian Data Guy

Apr 25, 2025

3

Share

TLDR

Vectorized

 (Pandas) UDFs marry Spark’s scale with Pandas &amp; NumPy’s speed by streaming Arrow column batches across the JVM ↔ Python boundary. They are 10-100× faster than classic Python UDFs, but they still have sharp edges—batch sizing, unsupported types, 2 GB limits, executor memory, SafeSpark jail-sandboxes, etc. 

In the world of big data processing, Apache Spark stands as the preeminent framework for distributed computation. One of its most powerful features for Python users is the ability to create User-Defined Functions (UDFs). However, traditional Python UDFs often face significant performance limitations. Enter Pandas PyArrow UDFs: a revolutionary approach that combines the analytical capabilities of pandas with the efficiency of Apache Arrow to deliver exceptional performance in distributed environments.

The Evolution of Python UDFs in Spark

Traditional Python UDFs in Spark suffer from three fundamental limitations that impact performance:

Serialization overhead

: Data must be serialized between JVM and Python processes using pickle, which is computationally expensive

2

.

Row-by-row processing

: Functions operate on individual rows rather than batches, resulting in millions of function calls for large datasets

4

.

Lack of vectorization

: Operations can't leverage the optimized C/Cython implementations in pandas and NumPy libraries

4

.

Pandas UDFs were introduced in Spark 2.3 to address these limitations, with significant improvements in Spark 3.0 and beyond. These vectorized UDFs use Apache Arrow to efficiently transfer data and pandas to process it in a vectorized manner, delivering performance increases of up to 100x compared to traditional UDFs

3

.

Apache Arrow: The Backbone of High-Performance Data Exchange

Apache Arrow is the critical technology that enables the exceptional performance of Pandas UDFs in Spark. As an open-source columnar in-memory data format, Arrow was specifically designed to facilitate efficient data exchange between different programming environments

2

. For Pandas UDFs, Arrow eliminates the costly serialization/deserialization overhead that plagues traditional Python UDFs when transferring data between JVM and Python processes

5

.

Before Arrow

Arrow achieves this efficiency through its columnar memory layout, which stores data contiguously by column rather than by row. This approach provides numerous benefits for analytical workloads: better memory compression, improved CPU cache utilization, and support for SIMD (Single Instruction, Multiple Data) vector operations

4

. Most importantly, Arrow enables a &quot;

zero-copy&quot;

 shared memory model where both JVM and Python processes can access the same data without duplicating it, dramatically reducing the cost of data transfer

6

.

When a Pandas UDF executes, Spark converts data to Arrow format, splits it into batches, transfers these batches to Python workers as Arrow structures, processes them using pandas, and then returns the results via the same efficient Arrow pathway

5

. This entire pipeline is optimized for high-throughput, parallel processing across a distributed cluster. The result is performance gains that can transform previously impractical Python processing into viable production workflows

3

.

The Data Flow Process

The process of executing a Pandas UDF involves several steps that highlight how data flows through the Spark execution environment:

Spark converts the data into Arrow format

The data is split into batches (configured by 

spark.sql.execution.arrow.maxRecordsPerBatch

)

Arrow batches are transferred to Python workers

Python workers convert Arrow batches to pandas Series or DataFrames

The UDF function processes these pandas objects

Results are converted back to Arrow format

Arrow data is transferred back to Spark

Spark converts Arrow data back to its internal format

This entire process happens in parallel across the Spark cluster, leveraging the distributed nature of Spark while maintaining the efficiency of vectorized operations.

Pandas UDFs Defined

You define a pandas UDF by decorating a Python function with 

@pandas_udf

and

 adding 

type hints

 for the input and output:

from typing import Iterator
import pandas as pd
from pyspark.sql.functions import pandas_udf 

@pandas_udf('long')
def pandas_plus_one(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
 return map(lambda s: s + 1, iterator)

display(spark.range(10).select(pandas_plus_one(&quot;id&quot;)))

The 

signature

 (

pd.Series → pd.Series

) tells Spark which UDF flavor to pick.

Under the hood, Spark uses 

Apache Arrow

 for zero-copy (de)serialization.

Vectorized

: your code gets whole batches as 

pd.Series

/

pd.DataFrame

, not single cells.

Different flavours of Pandas UDF

Series to Series (

pandas.Series -> pandas.Series

)

:

 This pattern exists to provide a clear, Pythonic, and type-hinted way to define vectorized UDFs that transform one Spark column into another, operating row-by-row (conceptually, applied batch-wise). It directly replaces the need to explicitly specify the older 

SCALAR

 Pandas UDF type, making the function's intent (operating on a Series and returning a Series of the same size) self-evident from the type hints.

Iterator of Series to Iterator of Series (

Iterator[pandas.Series] -> Iterator[pandas.Series]

)

:

 This pattern was introduced to offer more flexibility and optimization for Series-to-Series transformations. It allows processing data in batches (iterators) rather than loading the entire column partition at once. The 

why

 is twofold: 1) It helps manage memory usage for very large data partitions, and 2) It enables expensive state initialization (e.g., loading a model) to be done once per batch iterator, improving performance.

Iterator of Multiple Series to Iterator of Series (

Iterator[Tuple[pandas.Series, ...]] -> Iterator[pandas.Series]

):

 This extends the previous pattern because many operations require logic based on 

multiple

 input columns simultaneously. This type hint signature allows users to define UDFs that take batches of multiple input Series, perform calculations using them together, and return a single output Series batch, offering the same memory and initialization benefits for multi-column logic.

Series to Scalar (

pandas.Series -> Any

):

 This pattern, often used with 

groupBy().agg()

 or window functions, provides a type-hinted way to define aggregations. It takes a Pandas Series representing a group or partition and returns a single scalar value. The 

why

 is to replace the older 

GROUPED_AGG

 Pandas UDF type with a more standard Python type hint signature, making the aggregation intent clear.

applyInPandas

 (on GroupedData):

 This function exists specifically to implement the &quot;split-apply-combine&quot; pattern on grouped data

6

. 

Why?

 It allows applying a custom Python function, operating on a 

full Pandas DataFrame

 for each group, to perform complex, group-specific transformations or aggregations that are difficult or inefficient with standard Spark functions

2

6

. It expects one Pandas DataFrame (representing a group) as input and requires a Pandas DataFrame as output, effectively transforming each group

2

5

6

. Note: It loads the entire group into memory, which can be demanding for large groups

6

.

mapInPandas

 (on DataFrame):

 This function exists to apply a Python function to an 

iterator of Pandas DataFrames




---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
