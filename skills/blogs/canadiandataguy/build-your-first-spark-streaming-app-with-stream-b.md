---
name: build-your-first-spark-streaming-app-with-stream-b
description: 
---

# Build Your First Spark Streaming App with Stream-Batch Joins

## Overview


## Source
- **Author:** Canadian Data Guy
- **URL:** https://www.canadiandataguy.com/p/how-to-write-your-first-spark-stream

## Tags
streaming, joins, ai, spark, data-engineering, deltalake, clustering

## Full Content

# Build Your First Spark Streaming App with Stream-Batch Joins

**Source:** https://www.canadiandataguy.com/p/how-to-write-your-first-spark-stream

---



Build Your First Spark Streaming App with Stream-Batch Joins

Canadian Data Guy Unfiltered

Subscribe

Sign in

Build Your First Spark Streaming App with Stream-Batch Joins

Start your Spark Streaming journey with this beginner-friendly guide, featuring code for stream-batch joins and Delta table operations.

Canadian Data Guy

Sep 04, 2024

Share

When I started learning about Spark Streaming, I could not find enough code/material which could kick-start my journey and build my confidence. I wrote this blog to fill this gap which could help beginners understand how

 simple streaming 

is and build their first application.

In this blog, I will explain most things by first principles to increase your understanding and confidence and you walk away with code for your first Streaming application.

Photo by 

Ian Schneider

 on 

Unsplash

Scenario:

Let’s assume we have a streaming source with data arriving all the time. We want to add more attributes from another table( Think lookup table/ dimension table). Thus we will stream the data and join with the lookup table via Stream-Batch join. The result would be written as a Delta table, which could be used downstream for analytics or streaming.

Imports &amp; Parameters

from pyspark.sql import functions as F
from faker import Faker
import uuid

# define schema name and where should the table be stored
schema_name = &quot;test_streaming_joins&quot;
schema_storage_location = &quot;/tmp/CHOOSE_A_PERMANENT_LOCATION/&quot;

# Please download this file from https://simplemaps.com/data/us-zips then download and place it at a location of your choice and then change the value for the variable below
static_table_csv_file = &quot;/FileStore/jitesh.soni/data/us_zip_code_and_its_attributes.csv&quot;

# Static table specification
static_table_name = &quot;static_zip_codes&quot;

# Target Stareaming Table specification
target_table_name = &quot;joined_datasets&quot;

# Recommend you to keep the checkpoint next to the Delta table so that you do have to notion about where the checkpoint is
checkpoint_location = f&quot;{schema_storage_location}/{target_table_name}/_checkpoints/&quot;Create Target Database

The below code will help create a schema/database with comments and storage locations for tables

create_schema_sql = f&quot;&quot;&quot;
 CREATE SCHEMA IF NOT EXISTS {schema_name}
 COMMENT 'This is {schema_name} schema'
 LOCATION '{schema_storage_location}'
 WITH DBPROPERTIES ( Owner='Jitesh');
 &quot;&quot;&quot;
print(f&quot;create_schema_sql: {create_schema_sql}&quot;)

Generate Static Or a lookup Dataset

We will use a public dataset 

source

 with attributes about a zip code. This could be any other static source or a Delta table being updated in parallel.

Note

: If you pick a static source and start streaming, Spark Streaming will only read it once. If you have a few updates to the static source, you will have to restart the Spark Stream so it rereads the static source.

Meanwhile, if you have the Delta table as a source, then Spark Streaming will identify the update automatically, and nothing extra needs to be done.

csv_df = (
 spark.read.option(&quot;header&quot;, True)
 .option(&quot;inferSchema&quot;, True)
 .csv(static_table_csv_file)
)
display(csv_df)
csv_df.write.saveAsTable(f&quot;{schema_name}.{static_table_name}&quot;)

Next, we will Z-order the table on the key, which would be used in joins. This will help Spark Streaming do efficient joins because the Delta table is sorted by join key with statistics about which file contains which key value.

spark.sql(
 f&quot;&quot;&quot;
 OPTIMIZE {schema_name}.{static_table_name} ZORDER BY (zip);
 &quot;&quot;&quot;
)

Generate Streaming Dataset

We will generate a Streaming dataset using the Faker library. In the below code, we will define a few user-defined functions.

fake = Faker()
fake_id = F.udf(lambda: str(uuid.uuid4()))
fake_firstname = F.udf(fake.first_name)
fake_lastname = F.udf(fake.last_name)
fake_email = F.udf(fake.ascii_company_email)
# fake_date = F.udf(lambda:fake.date_time_this_month().strftime(&quot;%Y-%m-%d %H:%M:%S&quot;))
fake_address = F.udf(fake.address)
fake_zipcode = F.udf(fake.zipcode)

Now, we will use 

spark.readStream.format(“rate”) 

to generate data at your desired rate.

streaming_df = (
 spark.readStream.format(&quot;rate&quot;)
 .option(&quot;numPartitions&quot;, 10)
 .option(&quot;rowsPerSecond&quot;, 1 * 1000)
 .load()
 .withColumn(&quot;fake_id&quot;, fake_id())
 .withColumn(&quot;fake_firstname&quot;, fake_firstname())
 .withColumn(&quot;fake_lastname&quot;, fake_lastname())
 .withColumn(&quot;fake_email&quot;, fake_email())
 .withColumn(&quot;fake_address&quot;, fake_address())
 .withColumn(&quot;fake_zipcode&quot;, fake_zipcode())
)

# You can uncomment the below display command to check if the code in this cell works
# display(streaming_df)

Stream- Static Join or Stream -Delta Join

Structured Streaming supports joins (inner join and left join) between a streaming and a static DataFrame or a Delta Table. 

However, a few types of stream-static outer Joins are not supported yet.

lookup_delta_df = spark.read.table(static_table_name)

joined_streaming_df = streaming_df.join(
 lookup_delta_df,
 streaming_df[&quot;fake_zipcode&quot;] == lookup_delta_df[&quot;zip&quot;],
 &quot;left_outer&quot;,
).drop(&quot;fake_zipcode&quot;)
# display(joined_streaming_df)

Orchestrate the pipeline and write Spark Stream to Delta Table

Some Tips:

Give your streaming query a name. It’s good because this name will appear on Spark UI and help you monitor the stream.

If you are not planning to run the Stream continuously then use 

trigger(availableNow=True)

. This helps process all pending data and then stops the stream automatically.

(
 joined_streaming_df.writeStream
 # .trigger(availableNow=True)
 .queryName(&quot;do_a_stream_join_with_the_delta_table&quot;)
 .option(&quot;checkpointLocation&quot;, checkpoint_location)
 .format(&quot;delta&quot;)
 .toTable(f&quot;{schema_name}.{target_table_name}&quot;)
)

Download the code

Share

Previous

Next

Discussion about this post

Comments

Restacks

Top

Latest

Discussions

No posts

Ready for more?

Subscribe

© 2026 Canadian Data Guy

 · 

Privacy

 ∙ 

Terms

 ∙ 

Collection notice

 Start your Substack

Get the app

Substack
 is the home for great culture

 This site requires JavaScript to run correctly. Please 
turn on JavaScript
 or unblock scripts





---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
