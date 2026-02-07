---
name: "databricks-zerobus-ingest"
description: "Complete guide to Databricks Zerobus Ingest - zero-configuration streaming ingestion directly to Delta tables without message bus complexity."
---

# Databricks Zerobus Ingest: Zero-Configuration Streaming

## Overview

This skill covers Databricks Zerobus Ingest, a fully managed, zero-configuration service for record-by-record data ingestion directly into Delta tables. Learn how to eliminate message bus complexity (Kafka, Event Hubs) while achieving reliable, high-throughput streaming ingestion. Includes SDK usage, performance optimization, cost comparisons, and architectural patterns for when Zerobus replaces or complements traditional message buses.

## Quick Start

### Basic Setup and Ingestion
Create a Delta table and start ingesting data immediately:

```sql
-- Create target Delta table first
CREATE TABLE main.ingest.sensor_data (
    sensor_id STRING,
    timestamp TIMESTAMP,
    temperature DOUBLE,
    humidity DOUBLE,
    location STRING
) USING DELTA;

-- Grant permissions for Zerobus Ingest
GRANT MODIFY ON TABLE main.ingest.sensor_data TO `zerobus-service-principal`;
```

### Python SDK Quick Start
Install and use the Python SDK:

```python
pip install zerobus-ingest-sdk

from zerobus_ingest import ZerobusIngestClient
import json

# Initialize client
client = ZerobusIngestClient(
    endpoint_url="https://your-workspace.cloud.databricks.com/zerobus",
    table_path="main.ingest.sensor_data",
    token="your-databricks-token"
)

# Send data
data = {
    "sensor_id": "sensor-001",
    "timestamp": "2024-01-01T10:00:00Z",
    "temperature": 23.5,
    "humidity": 65.2,
    "location": "warehouse-a"
}

# Synchronous send
client.send(data)

# Batch send for higher throughput
batch_data = [data1, data2, data3]  # List of records
client.send_batch(batch_data)
```

### Go SDK Example (High Performance)
For Go applications with automatic recovery:

```go
package main

import (
    "context"
    "log"
    zerobus "github.com/databricks/zerobus-ingest-sdk-go"
)

func main() {
    client, err := zerobus.NewClient(&zerobus.ClientConfig{
        EndpointURL: "https://your-workspace.cloud.databricks.com/zerobus",
        TablePath:   "main.ingest.events",
        Token:       "your-databricks-token",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Send individual records
    record := map[string]interface{}{
        "event_id":   "evt-123",
        "user_id":    "user-456",
        "event_type": "click",
        "timestamp":  "2024-01-01T10:00:00Z",
    }

    err = client.Send(context.Background(), record)
    if err != nil {
        log.Printf("Send failed: %v", err)
    }
}
```

## Common Patterns

### Pattern 1: IoT Device Data Ingestion
High-frequency sensor data with automatic recovery:

```python
from zerobus_ingest import ZerobusIngestClient
import time
import random

class IoTDataProducer:
    def __init__(self, table_path):
        self.client = ZerobusIngestClient(
            endpoint_url="https://workspace.databricks.com/zerobus",
            table_path=table_path,
            token=dbutils.secrets.get("scope", "databricks-token")
        )

    def generate_sensor_reading(self, sensor_id):
        return {
            "sensor_id": sensor_id,
            "timestamp": time.time(),
            "temperature": 20 + random.uniform(-5, 5),
            "humidity": 50 + random.uniform(-10, 10),
            "battery_level": random.uniform(0, 100)
        }

    def run_continuous_ingestion(self, sensor_ids, interval_seconds=1):
        """Continuously send data from multiple sensors"""
        while True:
            for sensor_id in sensor_ids:
                try:
                    data = self.generate_sensor_reading(sensor_id)
                    self.client.send(data)
                except Exception as e:
                    print(f"Failed to send data for {sensor_id}: {e}")
                    # SDK handles automatic recovery

            time.sleep(interval_seconds)
```

### Pattern 2: Application Event Streaming
Real-time user events with schema validation:

```python
from zerobus_ingest import ZerobusIngestClient
from typing import Dict, Any
import json

class EventStreamer:
    def __init__(self, table_path: str):
        self.client = ZerobusIngestClient(
            endpoint_url="https://workspace.databricks.com/zerobus",
            table_path=table_path,
            token=dbutils.secrets.get("scope", "databricks-token")
        )

    def track_user_event(self, user_id: str, event_type: str,
                        properties: Dict[str, Any] = None):
        """Track user interactions"""
        event = {
            "event_id": f"{user_id}-{int(time.time())}",
            "user_id": user_id,
            "event_type": event_type,
            "timestamp": time.time(),
            "properties": json.dumps(properties or {}),
            "user_agent": properties.get("user_agent", ""),
            "ip_address": properties.get("ip_address", "")
        }

        self.client.send(event)

    def track_page_view(self, user_id: str, page_url: str,
                       referrer: str = None):
        """Track page views"""
        self.track_user_event(user_id, "page_view", {
            "page_url": page_url,
            "referrer": referrer
        })
```

### Pattern 3: High-Throughput Batch Ingestion
Optimizing for maximum throughput:

```python
import asyncio
from zerobus_ingest import ZerobusIngestClient
import concurrent.futures

class HighThroughputIngester:
    def __init__(self, table_path: str, num_workers: int = 4):
        self.table_path = table_path
        self.num_workers = num_workers
        self.clients = []

        # Create multiple clients for parallel ingestion
        for _ in range(num_workers):
            client = ZerobusIngestClient(
                endpoint_url="https://workspace.databricks.com/zerobus",
                table_path=table_path,
                token=dbutils.secrets.get("scope", "databricks-token")
            )
            self.clients.append(client)

    def ingest_batch_parallel(self, data_batch: list, batch_size: int = 100):
        """Distribute batch across multiple clients"""
        batches = [data_batch[i:i + batch_size]
                  for i in range(0, len(data_batch), batch_size)]

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = []
            for i, batch in enumerate(batches):
                client_idx = i % self.num_workers
                future = executor.submit(self.clients[client_idx].send_batch, batch)
                futures.append(future)

            # Wait for all batches to complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Batch failed: {e}")
```

## Reference Files

- [Zerobus Ingest Documentation](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest.html) - Official setup and configuration
- [Python SDK](https://github.com/databricks/zerobus-sdk-py) - Python client library
- [Go SDK](https://github.com/databricks/zerobus-sdk-go) - High-performance Go client
- [Java SDK](https://github.com/databricks/zerobus-sdk-java) - Java client library
- [Rust SDK](https://github.com/databricks/zerobus-sdk-rs) - Rust client library

## Common Issues

| Issue | Solution |
|-------|----------|
| **Schema validation errors** | Ensure Delta table schema matches incoming data structure |
| **Network connectivity issues** | SDK handles automatic recovery; monitor for NonRetriableException |
| **Performance bottlenecks** | Use same region for client and endpoint; consider batching |
| **Duplicate records** | Handle at-least-once semantics with deduplication logic |
| **Table not found errors** | Verify table path and permissions; table must be Unity Catalog managed |
| **Message size limits** | Keep individual messages under 10MB limit |

## Performance Optimization

### Throughput Maximization
Achieve maximum performance with these patterns:

```python
# Optimal configuration for high throughput
client_config = {
    "batch_size": 1000,  # Send in larger batches
    "compression": "gzip",  # Enable compression
    "parallel_clients": 4,  # Multiple clients for parallelism
    "same_region": True,   # Client and endpoint in same region
}

# Expected performance:
# - 100 MB/second per stream (1KB messages)
# - 15,000 rows/second per stream
# - Sub-50ms acknowledgment latency
```

### Cost Comparison: Zerobus vs Kafka

| Metric | Zerobus Ingest | Amazon MSK |
|--------|----------------|------------|
| **Infrastructure Cost** | $0 (fully managed) | $0.20/hour per broker |
| **Data Transfer** | $0.09/GB | $0.09/GB |
| **Operational Overhead** | Minimal | High (monitoring, scaling) |
| **Time to Production** | Minutes | Hours/Days |
| **Expertise Required** | Basic | Kafka expertise |
| **Typical Cost Reduction** | 67% vs MSK | - |

## When to Use vs When to Use Kafka

### Choose Zerobus Ingest When:
- ✅ Simple ingestion patterns (append-only)
- ✅ Cost-sensitive workloads
- ✅ Team lacks Kafka expertise
- ✅ At-least-once delivery is sufficient
- ✅ Single consumer per stream
- ✅ Schema validation at ingestion time

### Choose Kafka When:
- 🔴 Exactly-once semantics required
- 🔴 Multiple consumers per stream
- 🔴 Message retention and replay needed
- 🔴 Complex event processing topologies
- 🔴 Ultra-low latency fan-out patterns

## Key Takeaways

1. **Zero Configuration** - No message bus management, partitioning, or capacity planning
2. **Write-Ahead Log (WAL)** - Sub-50ms acknowledgments with durable storage
3. **Automatic Recovery** - SDK handles network failures transparently
4. **Schema Validation** - Catches data quality issues at ingestion time
5. **Cost Effective** - 67% cost reduction vs Amazon MSK in typical scenarios
6. **At-Least-Once Delivery** - Reliable delivery with possibility of duplicates
7. **Unity Catalog Integration** - Works only with managed Delta tables

## Limitations and Roadmap

### Current Limitations:
- At-least-once delivery (exactly-once coming soon)
- Single availability zone durability
- Managed Delta tables only
- No schema evolution support
- 10MB message size limit

### Coming Soon:
- Exactly-once delivery semantics
- MQTT protocol support
- CDC pipeline capabilities (updates/deletes)
- Subscriber/consumer model
- Multi-AZ durability

## When to Use This Skill

- Setting up streaming data ingestion to Databricks
- Evaluating alternatives to Kafka/Event Hubs
- Building IoT data pipelines
- Implementing real-time analytics ingestion
- Optimizing data ingestion costs and complexity
- Migrating from complex message bus architectures

## Migration Guide: From Kafka to Zerobus

```python
# Before: Kafka Producer
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['kafka:9092'])
producer.send('topic', value=data)

# After: Zerobus Ingest
from zerobus_ingest import ZerobusIngestClient
client = ZerobusIngestClient(table_path="main.ingest.events")
client.send(data)
```

## Related Skills

- databricks-delta-tables
- streaming-data-architectures
- kafka-alternatives
- real-time-data-ingestion
- cost-optimization-data-pipelines