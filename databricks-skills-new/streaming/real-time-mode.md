---
name: real-time-mode
description: "Sub-second latency streaming with Real-Time Mode (RTM) for time-critical applications."
tags: ["spark-streaming", "real-time-mode", "rtm", "low-latency", "photon"]
---

# Real-Time Mode (RTM)

## Overview

Real-Time Mode (RTM) provides sub-second latency for streaming pipelines requiring immediate processing.

## When to Use

| Latency | Mode | Photon Required |
|---------|------|-----------------|
| < 800ms | RTM | Yes |
| 1-30 seconds | Microbatch | Optional |
| > 30 seconds | Microbatch | No |

## Quick Start

```python
# Enable RTM
query = (df
    .writeStream
    .format("delta")
    .trigger(realTime=True)  # Enable RTM
    .option("checkpointLocation", checkpoint_path)
    .start(table_path)
)
```

## Cluster Requirements

```python
# Required configurations
spark.conf.set("spark.databricks.photon.enabled", "true")
spark.conf.set("spark.sql.streaming.stateStore.providerClass", 
               "com.databricks.sql.streaming.state.RocksDBStateProvider")
```

**Requirements:**
- Photon enabled
- Fixed-size cluster (no autoscaling)
- Driver: Minimum 4 cores
- Higher resource usage than microbatch

## Trade-offs

| Aspect | Microbatch | RTM |
|--------|-----------|-----|
| Latency | 1-30s | <800ms |
| Cost | Lower | Higher |
| Resource Usage | Lower | Higher |
| Use Case | Most workloads | Time-critical only |
