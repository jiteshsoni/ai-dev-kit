---
name: how-to-write-your-first-spark-application-with-str
description: 
---

# How to write your first Spark application with Stream-Stream Joins with working code

## Overview


## Source
- **Author:** Canadian Data Guy
- **URL:** https://www.canadiandataguy.com/p/how-to-write-your-first-spark-application-c23

## Tags
streaming, joins, ai, spark, data-engineering, deltalake, databricks

## Full Content

# How to write your first Spark application with Stream-Stream Joins with working code

**Source:** https://www.canadiandataguy.com/p/how-to-write-your-first-spark-application-c23

**Blog:** Canadian Data Guy

---



How to write your first Spark application with Stream-Stream Joins with working code

Canadian Data Guy Unfiltered

Subscribe

Sign in

Deep Dive

How to write your first Spark application with Stream-Stream Joins with working code

A Practical, Hands-On Guide to Joining Real-Time Data Streams in Spark Structured Streaming

Canadian Data Guy

Oct 15, 2025

6

Share

Have you been waiting to try Streaming but cannot take the plunge?

In a single blog, we will teach you whatever needs to be understood about Streaming Joins. We will give you a working code which you can use for your next Streaming Pipeline.

Thanks for reading CanadianDataGuy’s No Fluff Newsletter! Subscribe for free to receive new posts and support my work.

Subscribe

The steps involved:

Create a fake dataset at scale

Set a baseline using traditional SQL

Define Temporary Streaming Views

Inner Joins with optional Watermarking

Left Joins with Watermarking

The cold start edge case: withEventTimeOrder

Cleanup

What is Stream-Stream Join?

Stream-stream join is a widely used operation in stream processing where two or more data streams are joined based on some common attributes or keys. It is essential in several use cases, such as real-time analytics, fraud detection, and IoT data processing.

Concept of Stream-Stream Join

Stream-stream join combines two or more streams based on a common attribute or key. The join operation is performed on an ongoing basis, with each new data item from the stream triggering a join operation. In stream-stream join, each data item in the stream is treated as an event, and it is matched with the corresponding event from the other stream based on matching criteria. This matching criterion could be a common attribute or key in both streams.

When it comes to joining data streams, there are a few key challenges that must be addressed to ensure successful results. One of the biggest hurdles is the fact that, at any given moment, neither stream has a complete view of the dataset. This can make it difficult to find matches between inputs and generate accurate join results.

To overcome this challenge, it’s important to buffer past input as a streaming state for both input streams. This allows for every future input to be matched with past input, which can help to generate more accurate join results. Additionally, this buffering process can help to automatically handle late or out-of-order data, which can be common in streaming environments.

To further optimize the join process, it’s also important to use watermarks to limit the state. This can help to ensure that only the most relevant data is being used to generate join results, which can help to improve accuracy and reduce processing times.

Types of Stream-Stream Join

Depending on the nature of the join and the matching criteria, there are several types of stream-stream join operations. Some of the popular types of stream-stream join are:

Inner Join

In inner join, only those events are returned where there is a match in both the input streams. This type of join is useful when combining the data from two streams with a common key or attribute.

Outer Join

In outer join, all events from both the input streams are included in the joined stream, whether or not there is a match between them. This type of join is useful when we need to combine data from two streams, and there may be missing or incomplete data in either stream.

Left Join

In left join, all events from the left input stream are included in the joined stream, and only the matching events from the right input stream are included. This type of join is useful when we need to combine data from two streams and keep all the data from the left stream, even if there is no matching data in the right stream.

1. The Setup: Create a fake dataset at scale

Most people do not have 2 streams just hanging around for one to experiment with Stream Steam Joins. Thus I used Faker to mock 2 different streams which we will use for this example.

The name of the library being used is Faker and faker_vehicle to create Datasets.

!pip install faker_vehicle
!pip install faker

Imports

from faker import Faker
from faker_vehicle import VehicleProvider
from pyspark.sql import functions as F
import uuid
from utils import logger

Parameters

# define schema name and where should the table be stored
schema_name = “test_streaming_joins”
schema_storage_location = “/tmp/CHOOSE_A_PERMANENT_LOCATION/”

Create the Target Schema/Database

Create a Schema and set location. This way, all tables would inherit the base location.

create_schema_sql = f”””
 CREATE SCHEMA IF NOT EXISTS {schema_name}
 COMMENT ‘This is {schema_name} schema’
 LOCATION ‘{schema_storage_location}’
 WITH DBPROPERTIES ( Owner=’Jitesh’);
 “””
print(f”create_schema_sql: {create_schema_sql}”)
spark.sql(create_schema_sql)

Use Faker to define functions to help generate fake column values

fake = Faker()
fake.add_provider(VehicleProvider)

event_id = F.udf(lambda: str(uuid.uuid4()))
vehicle_year_make_model = F.udf(fake.vehicle_year_make_model)
vehicle_year_make_model_cat = F.udf(fake.vehicle_year_make_model_cat)
vehicle_make_model = F.udf(fake.vehicle_make_model)
vehicle_make = F.udf(fake.vehicle_make)
vehicle_model = F.udf(fake.vehicle_model)
vehicle_year = F.udf(fake.vehicle_year)
vehicle_category = F.udf(fake.vehicle_category)
vehicle_object = F.udf(fake.vehicle_object)

latitude = F.udf(fake.latitude)
longitude = F.udf(fake.longitude)
location_on_land = F.udf(fake.location_on_land)
local_latlng = F.udf(fake.local_latlng)
zipcode = F.udf(fake.zipcode)

Generate Streaming source data at your desired rate

def generated_vehicle_and_geo_df (rowsPerSecond:int , numPartitions :int ):
 return (
 spark.readStream.format(”rate”)
 .option(”numPartitions”, numPartitions)
 .option(”rowsPerSecond”, rowsPerSecond)
 .load()
 .withColumn(”event_id”, event_id())
 .withColumn(”vehicle_year_make_model”, vehicle_year_make_model())
 .withColumn(”vehicle_year_make_model_cat”, vehicle_year_make_model_cat())
 .withColumn(”vehicle_make_model”, vehicle_make_model())
 .withColumn(”vehicle_make”, vehicle_make())
 .withColumn(”vehicle_year”, vehicle_year())
 .withColumn(”vehicle_category”, vehicle_category())
 .withColumn(”vehicle_object”, vehicle_object())
 .withColumn(”latitude”, latitude())
 .withColumn(”longitude”, longitude())
 .withColumn(”location_on_land”, location_on_land())
 .withColumn(”local_latlng”, local_latlng())
 .withColumn(”zipcode”, zipcode())
 )

# You can uncomment the below display command to check if the code in this cell works
#display(generated_vehicle_and_geo_df)

# You can uncomment the below display command to check if the code in this cell works
#display(generated_vehicle_and_geo_df)

Now let’s generate the base source table and let’s call it Vehicle_Geo

def stream_write_to_vehicle_geo_table(rowsPerSecond: int = 1000, numPartitions: int = 10):
 table_name_vehicle_geo= “vehicle_geo”
 (
 generated_vehicle_and_geo_df(rowsPerSecond, numPartitions)
 .writeStream
 .queryName(f”write_to_delta_table: {table_name_vehicle_geo}”)
 .option(”checkpointLocation”, f”{schema_storage_location}/{table_name_vehicle_geo}/_checkpoint”)
 .format(”delta”)
 .toTable(f”{schema_name}.{table_name_vehicle_geo}”)
 )
stream_write_to_vehicle_geo_table(rowsPerSecond = 1000, numPartitions = 10)

Let the above code run for a few iterations, and you can play with rowsPerSecond and numPartitions to control how much data you would like to generate. Once you have generated enough data, kill the above stream and get a base line for row count.

spark.read.table(f”{schema_name}.{table_name_vehicle_geo}”).count()

display(
 spark.sql(f”“”
 SELECT * 
 FROM {schema_name}.{table_name_vehicle_geo}
“”“)
)

Let’s also get a min &amp; max 



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
