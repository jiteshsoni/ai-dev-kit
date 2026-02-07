---
name: ensuring-data-quality-in-the-hybrid-world-of-strea
description: 
---

# Ensuring Data Quality in the Hybrid World of Streaming and Scheduled ETL

## Overview


## Source
- **Author:** Canadian Data Guy
- **URL:** https://www.canadiandataguy.com/p/ensuring-data-quality-in-the-hybrid

## Tags
streaming, kafka, ai, spark, performance, data-engineering, deltalake, databricks

## Full Content

# Ensuring Data Quality in the Hybrid World of Streaming and Scheduled ETL

**Source:** https://www.canadiandataguy.com/p/ensuring-data-quality-in-the-hybrid

---



Ensuring Data Quality in the Hybrid World of Streaming and Scheduled ETL

Canadian Data Guy Unfiltered

Subscribe

Sign in

Ensuring Data Quality in the Hybrid World of Streaming and Scheduled ETL

How to Schedule Downstream ETL Jobs When Upstream Is a Streaming Job

Canadian Data Guy

Nov 05, 2024

1

Share

Introduction

In modern data architectures, it's increasingly common to have upstream data pipelines ingesting data continuously through streaming processes, while downstream systems rely on scheduled Extract, Transform, Load (ETL) jobs to process and analyze this data. Coordinating these two paradigms—

streaming ingestion

 and 

scheduled batch processing

—poses a significant challenge. The critical question is:

How do you decide when to run downstream ETL jobs when the upstream data is continuously streaming in?

This article explores various strategies to address this challenge, ensuring data consistency, accuracy, and timeliness in your downstream processes. We will delve into methods involving observable metrics, buffer-based scheduling, dbt (Data Build Tool) data quality checks, and more, providing practical guidance for data engineers and architects.

The Challenge

The primary difficulty lies in synchronizing the downstream ETL jobs with the upstream streaming data. Without proper coordination, downstream jobs might start processing incomplete or inconsistent data, leading to inaccurate results, data quality issues, or even system failures. The goal is to determine an optimal time or condition under which the downstream ETL jobs can safely execute, knowing that they have all the necessary data from the upstream streaming source.

Observable Metrics in Structured Streaming

Monitoring the health and progress of your streaming applications is crucial for making informed decisions regarding downstream ETL processes. Apache Spark Structured Streaming, commonly used in platforms like Databricks, provides observable metrics that can be leveraged for this purpose. These metrics vary based on the source of the streaming data:

1. Kafka Metrics

When using Apache Kafka as the source, you can monitor the following metrics:

avgOffsetsBehindLatest

: The average number of offsets that the streaming query is behind the latest available offset across all subscribed topics.

maxOffsetsBehindLatest

: The maximum number of offsets that the streaming query is behind the latest available offset across all subscribed topics.

minOffsetsBehindLatest

: The minimum number of offsets that the streaming query is behind the latest available offset across all subscribed topics.

estimatedTotalBytesBehindLatest

: The estimated total number of bytes that the streaming query has yet to consume from the subscribed topics.

2. Delta Lake Metrics

For Delta Lake as the streaming source, the relevant metrics include:

numInputRows

: The number of rows read in each micro-batch.

inputRowsPerSecond

: The rate at which data is being ingested.

processedRowsPerSecond

: The rate at which data is being processed.

batchId

: The unique identifier for each micro-batch, which can be useful for tracking progress.

3. Kinesis Metrics

When using Amazon Kinesis as the source, important metrics are:

avgMsBehindLatest

: The average number of milliseconds the consumer is behind the latest data in the stream.

maxMsBehindLatest

: The maximum number of milliseconds the consumer is behind the latest data.

minMsBehindLatest

: The minimum number of milliseconds the consumer is behind the latest data.

totalPrefetchedBytes

: The total number of bytes prefetched but not yet processed, indicating the backlog.

These metrics help you understand the lag in your streaming job and make informed decisions about when the downstream ETL jobs should run.

Decision Flow

To visualize the flow and interactions between the upstream streaming job and downstream ETL processes, here is a Mermaid diagram:

Explanation:

The 

Upstream Streaming Job

 writes data continuously to the 

Data Lake

, typically in Delta tables.

It also writes metrics to a 

Metrics Table

, which is used to monitor the streaming job's progress.

A 

Data Completeness Check

 is performed by querying the 

Metrics Table

.

If the lag is within acceptable thresholds, it 

Triggers the Downstream ETL Job

.

If the lag exceeds thresholds, it may 

Wait or Alert

 teams to handle the delay.

The 

Downstream ETL Job

 processes the data and produces 

Processed Data

 for consumption.

Notifications or alerts are sent to teams to 

Handle Delays

 if necessary.

Solutions

Let's explore various strategies to synchronize downstream ETL jobs with upstream streaming data.

1. Monitoring Streaming Metrics via 

StreamingQueryListener

Apache Spark provides the 

StreamingQueryListener

 interface, allowing you to monitor the progress and status of your streaming queries in real time. By attaching a 

StreamingQueryListener

 to your streaming job, you can access the metrics critical for determining downstream ETL execution.

Implementation Steps:

Attach a 

StreamingQueryListener

:

spark.streams.addListener(new StreamingQueryListener() {
 override def onQueryStarted(event: QueryStartedEvent): Unit = {
 // Handle query started events
 }
 override def onQueryProgress(event: QueryProgressEvent): Unit = {
 // Extract and store metrics
 val progress = event.progress
 val sources = progress.sources
 sources.foreach { source =>
 val metrics = source.metrics
 // Access metrics like avgOffsetsBehindLatest, etc.
 }
 }
 override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
 // Handle query termination
 }
})

Store Metrics for Access:

Push metrics to third-party observability tools

 like Datadog, Prometheus, or Grafana using their respective APIs or exporters.

Write metrics to a Delta table

 for easy querying and dashboarding within your data lake.

Determine Acceptable Lag:

Define thresholds for acceptable lag or backlog based on your business requirements.

Downstream ETL jobs can query the stored metrics to check if the lag is within acceptable limits before starting.

Benefits:

Data Accuracy:

 Ensures that downstream jobs process data only when it's sufficiently up-to-date.

Flexibility:

 Downstream teams can define their own thresholds for data freshness.

Real-Time Monitoring:

 Provides up-to-date information about the streaming job's progress.

Considerations:

Complexity:

 Requires additional infrastructure to store and query metrics.

Maintenance Overhead:

 The monitoring system needs to be maintained and monitored itself.

2. Buffer-Based Scheduling with Service Level Agreements (SLAs)

In some scenarios, the upstream team can provide a Service Level Agreement (SLA) guaranteeing that the streaming job will not fall behind by more than a certain amount of time (e.g., 2 hours). Downstream teams can then schedule their ETL jobs with an additional buffer to account for any unforeseen delays.

Implementation Steps:

Establish SLAs with the Upstream Team:

Agree on maximum allowable lag times.

Document these SLAs and communicate them clearly to all stakeholders.

Schedule Downstream Jobs with a Buffer:

If the SLA is 2 hours, downstream jobs can add an additional buffer (e.g., 1 hour) and schedule their jobs to run 3 hours after data publication.

Use scheduling tools like Apache Airflow, Cron, or the scheduling features in your ETL platform.

Benefits:

Simplicity:

 No need for complex monitoring or coordination mechanisms.

Predictability:

 Downstream jobs run at fixed times, making planning easier.

Considerations:

Risk of Inaccuracy:

 If the upstream job falls behind beyond the SLA, downstream jobs may process incomplete data.

Lack of Real-Time Adaptation:

 This method doesn't account for real-time variations in data ava



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
