---
name: unlocking-sub-second-latency-with-databricks
description: 
---

# Unlocking Sub-Second Latency with Databricks

## Overview


## Source
- **Author:** Canadian Data Guy
- **URL:** https://www.canadiandataguy.com/p/unlocking-sub-second-latency-with

## Tags
streaming, kafka, joins, ai, spark, data-engineering, deltalake, databricks

## Full Content

# Unlocking Sub-Second Latency with Databricks

**Source:** https://www.canadiandataguy.com/p/unlocking-sub-second-latency-with

**Blog:** Canadian Data Guy

---



Unlocking Sub-Second Latency with Databricks 

Canadian Data Guy Unfiltered

Subscribe

Sign in

Playback speed

×

Share post

Share post at current time

Share from 0:00

0:00

/

0:00

Transcript

1

Unlocking Sub-Second Latency with Databricks 

How Spark Real Time Mode Achieving Millisecond Latency with a Simple Trigger Switch

Canadian Data Guy

Jan 14, 2026

1

Share

Transcript

I spent a whole month trying to write the “perfect” single blog on Spark Structured Streaming Real-Time Mode… and then I accepted reality: it’s too much to cram into one post without turning it into a textbook. So this is a 

series

.

In this first post, I’m not building a crypto demo. I’m building a pattern you can reuse for things that actually move the needle: fraud detection, IoT sensor monitoring, real-time offers, security signals—

anything where you need to respond to events ASAP.

The goal is simple:

When an event looks suspicious or invalid, flag it immediately and route it differently.

That “suspicious or invalid” could be:

Fraud detection:

 a transaction looks off → trigger a downstream

IoT:

 a sensor reading is impossible → trigger an action

Security:

 payload contains secrets/PII patterns → quarantine in real time

Offers/personalization:

 Respond to specific events

For the dataset, I’m using Ethereum blocks because they’re high volume and behave like real production traffic. But the point isn’t crypto. The point is the operational pattern: 

real-time guardrails

.

Concretely, I’m doing two checks on every block event:

Payload hygiene:

 flag suspicious strings in 

extra_data

 (think accidental secrets/PII-style patterns)

Data quality:

gas_used > gas_limit

 (This should not happen—if it does, something is wrong)

If any check trips, the event gets tagged 

QUARANTINE

. Otherwise 

ALLOW

. The output is just an enriched Kafka event that downstream consumers can act on immediately.

Also, I used 

Redpanda

 to run Kafka because they make it ridiculously easy to spin up a cluster, and new signups get 

$100 in credits for 14 days

. Not sponsored.

Redpanda, if you’re reading this: give me more credits. I have too many experiments.

If you know me, you know I always test things at scale; if it does not scale, then I don’t write about it. I uploaded the full Ethereum chain into Kafka—about 

95 GB

 into 

4 partitions

—roughly 

23 million messages

. If you want my notebook that dumps data into Redpanda, drop a comment and I’ll share it.

Why am I doing this?

Because I’m tired of hearing:

“Spark isn’t fast enough like Flink, so we need a whole new stack for this one use case.”

After 10+ years in data engineering, one lesson keeps paying rent: 

maintainability beats shiny tools

 more often than people want to admit. Even if the new tool is 20% faster and I have the energy to learn it, that does not automatically mean my whole team should learn it too.

I’ve benchmarked Spark streaming enough to be confident about this: i

f you can tolerate ~1–2 seconds

, Spark micro-batch will happily land data into Delta all day. I should redo that benchmark—last time it cost me 

$1,300

 (Confluent waived it, bless them). I’m here for sub-second latency, not 

sub-second “your card has been charged” notifications

.

Now Spark is stepping into the 

sub-second

 territory with 

Real-Time Mode

—a new trigger type designed for operational workloads that need immediate response, with end-to-end latency advertised as low as 

5 ms

 (Public Preview, DBR 16.4 LTS+). 

Databricks Documentation

I don’t buy marketing. So I tested it.

What we’re building: Operational Guardrail Stream 

Every incoming event gets evaluated immediately and we emit an enriched event downstream with:

a 

decision

: 

ALLOW

 vs 

QUARANTINE

reasons

: why we flagged it (data quality, payload hygiene, etc.)

This is the operational pattern that shows up everywhere:

“Do I quarantine it?”

“Do I enrich it so downstream can react instantly?”

In my dataset, the “event” is an Ethereum block. In your world, it could be a transaction, sensor reading, auth log, API call—same idea.

The dataset and assumptions

Source topic: 

ethereum-blocks-ordered-global

Target topic: 

topic-with-4-partitions

Assumptions:

Kafka 

value

 is JSON

We parse it into a hardcoded schema so we have typed columns like 

gas_used

, 

gas_limit

, 

timestamp

, etc.

We also keep 

kafka_ts

 (Kafka append timestamp) because for operational monitoring, arrival time matters.

What makes an event “bad” in this post

I’m keeping the rules intentionally simple and high-signal:

Rule 1: Payload hygiene check

Scan 

extra_data

 for obvious “this shouldn’t be here” patterns.

In the blog code, I show basic examples (email/JWT/AWS key shapes). Replace these with your real rules (PII patterns, internal IDs, API keys, etc.).

The point isn’t regex perfection. The point is: 

real-time guardrails belong in the pipeline, not in a postmortem.

Rule 2: Bad data check

gas_used > gas_limit

This should not happen. If it happens, either:

the data is corrupted,

the producer is wrong,

you’re parsing incorrectly,

or something upstream is broken.

Operationally, that’s exactly what we want: 

flag it immediately.

Real-Time Mode: the tiny bit you need to know

Real-Time Mode is enabled by using the real-time trigger and runs under update mode. In PySpark you specify an interval like 

&quot;5 minutes&quot;

.

Two important “don’t skip this” notes:

Cluster config matters

 (Databricks documents the required job cluster settings and the RTM enablement flag).

Output mode must be 

update

 with RTM triggers.

That’s all I’m going to say here, because this post is about the operational pattern. I’ll do a deeper “RTM setup checklist” in the next post.

The code: Real-time guardrail (Kafka → Spark RTM → Kafka)

This is a single-pass pipeline:

Connect to Kafka

Parse kafka input

Compute 

decision

, 

reasons

write JSON back to Kafka 

as strings

 (no binary needed)

Imports &amp; Configuration

import json
import re
import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import (
 StructType, StructField, StringType, LongType, 
 DoubleType, TimestampType, DateType
)

# -------------------------------------------------------------------------
# 1. CONFIGURATION
# -------------------------------------------------------------------------

# --- Kafka Connection Details ---
# Ideally, fetch these from secrets (e.g., dbutils.secrets.get)
BOOTSTRAP_SERVERS = &quot;d5deqhbrcoacstishppg.any.us-west-2.mpx.prd.cloud.redpanda.com:9092&quot;
SASL_MECHANISM = &quot;SCRAM-SHA-256&quot;
RP_USERNAME = &quot;redpanda&quot;
RP_PASSWORD = &quot;&quot;

# --- Kafka Options ---
RP_KAFKA_OPTIONS = {
 &quot;kafka.bootstrap.servers&quot;: BOOTSTRAP_SERVERS,
 &quot;kafka.security.protocol&quot;: &quot;SASL_SSL&quot;,
 &quot;kafka.sasl.mechanism&quot;: SASL_MECHANISM,
 &quot;kafka.sasl.jaas.config&quot;: (
 'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required '
 f'username=&quot;{RP_USERNAME}&quot; password=&quot;{RP_PASSWORD}&quot;;'
 ),
 &quot;kafka.ssl.endpoint.identification.algorithm&quot;: &quot;https&quot;,
}

# --- Job Settings ---
INPUT_TOPIC = &quot;ethereum-blocks-ordered-global&quot;
OUTPUT_TOPIC = &quot;topic-with-4-partitions&quot;
CHECKPOINT_LOCATION = f&quot;/tmp/chk_rtm_stateless_guardrail_{uuid.uuid4()}&quot;

# Set shuffle partitions for purposes (default 200 which is too high in low latency use cases)
spark.conf.set(&quot;spark.sql.shuffle.partitions&quot;, &quot;8&quot;)

Connect to Kafka

# --- Step A: Read from Kafka ---
df_raw = (
 spark.readStream
 .format(&quot;kafka&quot;)
 .options(**RP_KAFKA_OPTIONS)
 .option(&quot;subscribe&quot;, INPUT_TOPIC)
 .option(&quot;startingOffsets&quot;, &quot;ear



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
