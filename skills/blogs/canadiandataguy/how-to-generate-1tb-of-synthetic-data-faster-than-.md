---
name: how-to-generate-1tb-of-synthetic-data-faster-than-
description: 
---

# How to Generate 1TB of Synthetic Data Faster Than a Coffee Break

## Overview


## Source
- **Author:** Canadian Data Guy
- **URL:** https://www.canadiandataguy.com/p/how-to-generate-1tb-of-synthetic

## Tags
streaming, joins, ai, spark, performance, data-engineering, deltalake, databricks

## Full Content

# How to Generate 1TB of Synthetic Data Faster Than a Coffee Break

**Source:** https://www.canadiandataguy.com/p/how-to-generate-1tb-of-synthetic

**Blog:** Canadian Data Guy

---



How to Generate 1TB of Synthetic Data Faster Than a Coffee Break

Canadian Data Guy Unfiltered

Subscribe

Sign in

How to Generate 1TB of Synthetic Data Faster Than a Coffee Break

And Cheaper Than Your Starbucks Coffee

Canadian Data Guy

Jan 01, 2025

1

Share

Imagine creating a massive 1 Terabyte dataset of IoT data in less time than it takes to enjoy your coffee break. With synthetic data generation techniques and a bit more computing power, this becomes a reality. By leveraging an 4-core machine, we can process an astounding 1 million rows per second, with each row containing 1 KB of data. Let's break down what this means:

1 billion rows of 1 KB each equates to 1000 GB or 1 TB of data.

At a rate of 1 million rows per second, it takes approximately 1000 seconds (about 16.66 minutes) to generate 1 billion rows.

This means you can create a full terabyte of synthetic IoT data in under 10 minutes on 8 core machine; I have used 4 in my example. Such rapid data generation opens up exciting possibilities for developers, data scientists, and researchers working on big data projects, IoT applications, or machine learning models that require extensive datasets for training and testing.

ScreenShot Of Spark Streaming UI

Why create Synthetic datasets?

Privacy and Compliance

: Synthetic data allows developers to work with realistic data without risking exposure of sensitive information, helping to meet data protection regulations.

Scalability and Control

: You can generate virtually unlimited amounts of data with precise control over its characteristics, enabling thorough testing of systems at scale and creation of edge cases that might be rare or impossible to capture in real-world data.

Development Acceleration:

 By removing dependency on upstream teams for data, developers can build end-to-end pipelines, set up DevOps processes, and address architectural concerns before actual data becomes available, significantly speeding up the development process.

Cost-Effectiveness and Efficiency

: Generating synthetic data is often faster and more economical than collecting and processing real-world data, especially for large-scale testing and development.

Hardware

We used a single machine with 4 cores and 32 GB of memory

Let’s get into the code

Install 

Databricks Data Generator

The 

dbldatagen

 is a Python library for generating synthetic data within the Databricks environment using Spark. The generated data may be used for testing, benchmarking, demos, and many other uses.

It operates by defining a data generation specification in code that controls how the synthetic data is generated. The specification may incorporate the use of existing schemas or create data in an ad-hoc fashion. You can use it from Scala, R or other languages by defining a view over the generated data.

%pip install dbldatagen

 Setup and Imports 

import dbldatagen as dg
import uuid

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import expr

 Parameters 

# Parameterize partitions and rows per second
PARTITIONS = 4 # Match with number of cores on your cluster
ROWS_PER_SECOND = 1 * 1000 * 1000 # 1 Million rows per second

Schema Definition

iot_data_schema = StructType([
 StructField(&quot;device_id&quot;, StringType(), False),
 StructField(&quot;event_timestamp&quot;, TimestampType(), False),
 StructField(&quot;temperature&quot;, DoubleType(), False),
 StructField(&quot;humidity&quot;, DoubleType(), False),
 StructField(&quot;pressure&quot;, DoubleType(), False),
 StructField(&quot;battery_level&quot;, IntegerType(), False),
 StructField(&quot;device_type&quot;, StringType(), False),
 StructField(&quot;error_code&quot;, IntegerType(), True),
 StructField(&quot;signal_strength&quot;, IntegerType(), False)
])

Here, we define the schema for our IoT data. Each 

StructField

 represents a column in our dataset, specifying the name, data type, and whether it can contain null values. This schema mimics real-world IoT device data, including device identifiers, sensor readings, and status information.

Why use Databricks Data Generator- 

dbldatagen

?

Using 

dbldatagen

 for synthetic data generation offers several significant benefits that align closely with the characteristics of your actual data. 

The ability to specify parameters like 

minValue

, 

maxValue

, 

random

, and 

percentNulls

 allows you to create datasets that closely mimic real-world scenarios.

 This means you can generate realistic variations in your data, such as different temperature ranges or device IDs, while also controlling for missing values. By tailoring these specifications, you ensure that the synthetic data is not only large in volume but also rich in diversity, making it a valuable resource for testing and training machine learning models effectively.

dataspec = (
 dg.DataGenerator(spark, name=&quot;iot_data&quot;, partitions=PARTITIONS)
 .withSchema(iot_data_schema)
 .withColumnSpec(&quot;device_id&quot;, percentNulls=0.1, minValue=1000, maxValue=9999, prefix=&quot;DEV_&quot;, random=True)
 .withColumnSpec(&quot;event_timestamp&quot;, begin=&quot;2023-01-01 00:00:00&quot;, end=&quot;2023-12-31 23:59:59&quot;, random=True)
 .withColumnSpec(&quot;temperature&quot;, minValue=-10.0, maxValue=40.0, random=True)
 .withColumnSpec(&quot;humidity&quot;, minValue=0.0, maxValue=100.0, random=True)
 .withColumnSpec(&quot;pressure&quot;, minValue=900.0, maxValue=1100.0, random=True)
 .withColumnSpec(&quot;battery_level&quot;, minValue=0, maxValue=100, random=True)
 .withColumnSpec(&quot;device_type&quot;, values=[&quot;Sensor&quot;, &quot;Actuator&quot;, &quot;Gateway&quot;, &quot;Controller&quot;], random=True)
 .withColumnSpec(&quot;error_code&quot;, minValue=0, maxValue=999, random=True, percentNulls=0.2)
 .withColumnSpec(&quot;signal_strength&quot;, minValue=-100, maxValue=0, random=True)
)

This section creates a data generator specification using 

dbldatagen

. For each column, we define the data generation rules, including value ranges, randomness, and special formatting (like the &quot;DEV_&quot; prefix for device IDs). This ensures our synthetic data closely resembles real IoT data patterns.

Streaming DataFrame Creation

streaming_df = (
 dataspec.build(
 withStreaming=True,
 options={
 'rowsPerSecond': ROWS_PER_SECOND,
 }
 )
 .withColumn(
 &quot;firmware_version&quot;,
 expr(
 &quot;concat('v', cast(floor(rand() * 10) as string), '.', &quot;
 &quot;cast(floor(rand() * 10) as string), '.', &quot;
 &quot;cast(floor(rand() * 10) as string))&quot;
 )
 )
 .withColumn(
 &quot;location&quot;,
 expr(
 &quot;concat(cast(rand() * 180 - 90 as decimal(8,6)), ',', &quot;
 &quot;cast(rand() * 360 - 180 as decimal(9,6)))&quot;
 )
 )
 .withColumn(
 &quot;data_payload&quot;,
 expr(&quot;repeat(uuid(), 22)&quot;) # Add approx. 800 Bytes to construct 1 KB row
 )
)

streaming_df = ( dataspec.build( withStreaming=True, options={ 'rowsPerSecond': ROWS_PER_SECOND, } ) .withColumn( &quot;firmware_version&quot;, expr( &quot;concat('v', cast(floor(rand() * 10) as string), '.', &quot; &quot;cast(floor(rand() * 10) as string), '.', &quot; &quot;cast(floor(rand() * 10) as string))&quot; ) ) .withColumn( &quot;location&quot;, expr( &quot;concat(cast(rand() * 180 - 90 as decimal(8,6)), ',', &quot; &quot;cast(rand() * 360 - 180 as decimal(9,6)))&quot; ) ) .withColumn( &quot;data_payload&quot;, expr(&quot;repeat(uuid(), 22)&quot;) # Add approx. 800 Bytes to construct 1 KB row ) )

Here, we build the streaming DataFrame using our data specification. We enable streaming with `

withStreaming=True`

and set the rows per second. We also add additional columns:

firmware_version

: A randomly generated version number.

location

: Random latitude and long



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
