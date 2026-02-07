---
name: "Unlocking Sub-Second Latency with Databricks"
description: "Use Spark Real-Time Mode (RTM) to achieve sub-second latency for low-latency streaming use cases like fraud detection and personalized offers."
author: "Canadian Data Guy"
url: "https://www.youtube.com/watch?v=w6R0aUlZaAQ"
date: "2026-01-23"
tags: ["spark-streaming", "real-time-mode", "rtm", "low-latency", "sub-second", "kafka"]
---

# Unlocking Sub-Second Latency with Databricks

## Overview

Spark Structured Streaming now supports **Real-Time Mode (RTM)** for sub-second latency processing. This enables Spark to handle use cases previously requiring Flink or other specialized stream processors.

**Latency Guidance**:
- < 800ms: Consider Flink (or Spark RTM)
- > 800ms: Spark Microbatch (default) is cost-effective and simpler

## Quick Start

### Enable Real-Time Mode

```python
# Only difference from microbatch: trigger.realTime()
query = (df
    .writeStream
    .format("kafka")  # or delta
    .trigger(realTime=True)  # Enable RTM
    .option("minLatencyMs", 100)  # Target latency
    .start()
)
```

### RTM vs Microbatch Comparison

```python
# Microbatch mode (default)
.trigger(processingTime="1 second")
# - Schedules tasks every interval
# - Higher latency due to scheduling overhead

# Real-Time Mode
.trigger(realTime=True)
# - Pre-allocates resources
# - Data flows through immediately
# - Sub-second latency achievable
```

## Common Patterns

### Pattern 1: Fraud Detection Pipeline

```python
# Use case: Detect fraud in < 100ms
# Architecture: Kafka → Spark RTM → Kafka

fraud_detection = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "transactions")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("is_fraud", fraud_detection_udf(col("amount"), col("location")))
    .filter(col("is_fraud") == True)
    .select(to_json(struct("*")).alias("value"))
    .writeStream
    .format("kafka")
    .option("topic", "fraud_alerts")
    .trigger(realTime=True)
    .start()
)
```

### Pattern 2: Personalized Offers

```python
# Use case: Real-time offer when user quits app
# Latency requirement: < 500ms

offers = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "user_events")
    .load()
    .filter(col("event_type") == "app_quit")
    .join(offer_rules, "user_segment")
    .select(
        col("user_id"),
        col("offer_id"),
        current_timestamp().alias("generated_at")
    )
    .writeStream
    .format("kafka")
    .option("topic", "personalized_offers")
    .trigger(realTime=True)
    .start()
)
```

### Pattern 3: Hybrid Latency Architecture

```python
# Most jobs: Microbatch (cost-effective)
# Critical jobs: Real-Time Mode

# 90% of workloads - microbatch
standard_stream = (df
    .writeStream
    .trigger(processingTime="30 seconds")
    .start("standard_table")
)

# 10% of workloads - RTM for low latency
critical_stream = (df
    .writeStream
    .trigger(realTime=True)
    .start("critical_table")
)
```

## Reference Files

### RTM Architecture

**Microbatch Mode**:
```
Data arrives → Wait for trigger → Schedule tasks → Execute → Commit
                    ↑
            Scheduling overhead
```

**Real-Time Mode**:
```
Data arrives → Immediate processing → Commit
                    ↑
            Pre-allocated resources
```

### Monitoring RTM

```python
# Check driver logs for latency metrics
# Look for: "spark.streaming made progress"
# Contains:
# - Input rows per second
# - Start/end offsets
# - P99 latency
# - End-to-end latency

# Example log output:
# P99 latency: 1ms
# End-to-end latency: 50ms
```

## Common Issues

| Issue | Solution |
|-------|----------|
| **Higher resource usage** | RTM pre-allocates resources; expected trade-off for latency |
| **Not seeing latency improvement** | Check if bottleneck is source/sink, not Spark |
| **P99 spikes** | Look for garbage collection pauses; tune JVM settings |
| **Can't achieve < 100ms** | Consider if use case truly needs it; network latency may dominate |

## Advanced Tips

### When to Use RTM vs Microbatch

| Factor | Use RTM | Use Microbatch |
|--------|---------|----------------|
| Latency requirement | < 1 second | > 1 second |
| Revenue impact | High (fraud, offers) | Low (reporting) |
| Cost sensitivity | Low | High |
| Complexity tolerance | Higher | Lower |

### Cost vs Latency Trade-off

```python
# RTM costs more due to pre-allocation
# Only use when latency directly impacts revenue

# Example decision matrix:
fraud_detection = RTM        # $$$ but prevents fraud losses
user_analytics = microbatch  # $   hourly reports fine
```

### Benchmark Results

```
Configuration: 4 cores, Kafka source/sink
Dataset: 23M Ethereum blockchain records (112 GB)

RTM Results:
- P99 latency: 1ms (Spark processing)
- Throughput: 60,000+ rows/second
- Processing time: ~6 minutes for 23M rows

Microbatch (1.5s trigger) Results:
- Latency: ~1.5 seconds
- Same throughput achievable
- Lower cost (no pre-allocation)
```

## FAQ

**Q: Is RTM production-ready?**
A: Yes, though newer than microbatch. Monitor releases for improvements.

**Q: Can I mix RTM and microbatch in same job?**
A: No, choose per stream. Different streams in same cluster can use different modes.

**Q: What's the minimum latency achievable?**
A: Sub-100ms possible depending on processing complexity and network.

**Q: Does RTM work with all sources/sinks?**
A: Works with Kafka, Delta. Check documentation for latest supported connectors.

**Q: How do I migrate from microbatch to RTM?**
A: Change trigger from `processingTime` to `realTime`. Same code, one line change.