---
name: how-to-read-delta-log-statistics-and-why-you-should
description: #
---

# How to Read Delta Log Statistics (and Why You Should)

## Overview
#

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2025-05-02
- **URL:** https://www.youtube.com/watch?v=g-aaFE1dCRQ

## Tags
deltalake

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** g-aaFE1dCRQ

### Summary

#### **TL;DR**
1. Delta Logs are parsed from JSON files in park files.
2. The 'stats column' contains time stamps and categorical data (pressure device type, error code).
3. Statistics include max, min, average, standard deviation, and null counts per column.

#### **Key Ideas**
- Delta logs are structured as JSON schema with a 'stats column'.
- Columns include numeric values like pressure device ID and strings like error codes.
- The script analyzes each park file to collect statistics on columns (max, min, null counts).
- Statistics help validate if specified parameters (e.g., subcolumns within structs) are enforced.

### Full Transcript

Hey, this is a quick video on how to read your data log and specifically how to read the statistics of the data log. Let's run this from scratch just for you folks to see. So in the top it's just imports. Here is a dynamic schema parsing. So we are passing I think the top row to this function and this is going to basically read the JSON and actually parse the schema.

And this function basically you give it a table name. We figure out the path. We figure read the JSON files. JSON files depict the data log. So we go read the data log and then we have a particular column which has the statistics. So that column is called stats column. And yeah all of this is just schema parsing and reading and flattening.

So let me show you the output. We are just going to give a table name and as an input. Then we figure out automatic automatically what is the delta path or what is the path where all the JSON files are sitting. Again JSON files represent your delta table. And now we are going with reading those JSON files meaning reading the delta log. And this is the inferred schema of the delta log.

And then what I have for you is the path of the park file. Inside the delta log what we are getting. Hey this is the park file. This has the number of records. This these are the number of records inside the park file. And this is what is of most significant value which is for each column. We have the max value. So these are the column names given time stamp string. Pressure device type which is a string. Error code. So a mix of integers strings and time stamp.

So we have the max value for all of the columns and min values for each of the columns. We also have the null count on how many nulls does this thing have. And yeah, that's about it which is this script will help you read your data log. You can read parts of the code. It will figure out the table path for you automatically and then it will show you what statistics have been collected for each park file.

You may also ask why is this important? So what we are trying to identify here is for which column statistics have been collected. We set parameters but this is opening the delta log and checking is a way for us to verify that the parameters which were specified have gone into effect. For example, in a delta log by default the first 32 column statistics are collected. But sometimes people want to collect statistics of certain other columns. So once you specify that parameter you actually want to see is it being enforced?

---





---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
