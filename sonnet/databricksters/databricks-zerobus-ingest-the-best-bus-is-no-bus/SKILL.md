---
name: "Zerobus Ingest - Simplified Streaming Ingestion"
description: "Replace Kafka infrastructure with Databricks Zerobus Ingest for direct, durable writes to Delta tables using Write-Ahead Logging at fraction of the cost."
author: "Yashodhan"
url: "https://www.databricksters.com/p/databricks-zerobus-the-best-bus-is"
date: "2025-12-30"
tags: ["zerobus", "ingestion", "streaming", "wal", "cost-optimization", "kafka-alternative", "databricks"]
---

# Zerobus Ingest - Simplified Streaming Ingestion

## Overview

Zerobus Ingest is a fully managed, zero-configuration service enabling record-by-record ingestion directly into Delta tables without intermediate message buses. Eliminates Kafka/MSK operational complexity while providing durable acknowledgments via Write-Ahead Logging (WAL). Best for straightforward ingestion use cases requiring high throughput (100MB/s per stream, 15K rows/s) at significantly lower cost than managed Kafka.

**Use this skill when:** Simplifying application-to-lakehouse ingestion, replacing Kafka for append-only workloads, or reducing operational overhead and costs.

## Quick Start

Basic Zerobus ingestion in Python:

```python
# Install SDK
# pip install databricks-zerobus-ingest-sdk

from databricks.zerobus import ZerobusClient

# Initialize client
client = ZerobusClient(
    host="your-workspace.cloud.databricks.com",
    token="dapi...",
    catalog="main",
    schema="bronze",
    table="events"
)

# Send records (protobuf or JSON)
records = [
    {"user_id": "user123", "event": "page_view", "timestamp": "2026-01-01T00:00:00Z"},
    {"user_id": "user456", "event": "click", "timestamp": "2026-01-01T00:00:01Z"}
]

# Write with automatic retry and durability
for record in records:
    client.write(record)

client.close()
```

## Common Patterns

### Pattern 1: Go Application Integration

Minimal code changes for existing Go applications:

```go
import (
    "github.com/databricks/zerobus-sdk-go/zerobus"
)

// Initialize Zerobus client
client, err := zerobus.NewClient(zerobus.Config{
    Host:     "workspace.cloud.databricks.com",
    Token:    "dapi...",
    Catalog:  "main",
    Schema:   "bronze",
    Table:    "device_telemetry",
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Send telemetry data
record := map[string]interface{}{
    "device_id":   "device-001",
    "temperature": 72.5,
    "timestamp":   time.Now().Unix(),
}

// Automatic buffering, batching, and retry
err = client.Write(record)
if err != nil {
    log.Printf("Write failed: %v", err)
}
```

**Result:** Same append-only pattern, properly architected with WAL durability.

### Pattern 2: IoT Device Ingestion at Scale

High-volume device data ingestion:

```python
from databricks.zerobus import ZerobusClient
import json

# Connect to Zerobus
client = ZerobusClient(
    host="workspace.cloud.databricks.com",
    token="dapi...",
    catalog="iot",
    schema="raw",
    table="sensor_readings"
)

# Stream device data
def ingest_sensor_data(device_id, readings):
    for reading in readings:
        record = {
            "device_id": device_id,
            "temperature": reading["temp"],
            "humidity": reading["humidity"],
            "pressure": reading["pressure"],
            "timestamp": reading["ts"]
        }
        
        try:
            client.write(record)
        except ZerobusException as e:
            # Automatic retry for retriable errors
            if e.is_retriable:
                # SDK handles retry
                pass
            else:
                # Log non-retriable error
                print(f"Non-retriable error: {e}")

# Performance: 15,000 rows/sec per stream
# For higher throughput: Use multiple streams/tables
```

### Pattern 3: Schema Validation and Error Handling

Leverage built-in schema validation:

```python
from databricks.zerobus import ZerobusClient, NonRetriableException, ZerobusException

# Create Delta table with schema FIRST
spark.sql("""
    CREATE TABLE main.bronze.events (
        user_id STRING NOT NULL,
        event_type STRING,
        event_data STRING,
        timestamp TIMESTAMP
    ) USING DELTA
""")

client = ZerobusClient(
    host="workspace.cloud.databricks.com",
    token="dapi...",
    catalog="main",
    schema="bronze",
    table="events"
)

# Write with validation
def write_with_validation(record):
    try:
        client.write(record)
    except NonRetriableException as e:
        # Schema mismatch or validation error
        print(f"Invalid record rejected: {e}")
        # Log to dead-letter queue
        log_to_dlq(record, str(e))
    except ZerobusException as e:
        # Transient error - SDK will retry
        print(f"Temporary error (retrying): {e}")

# Automatic validation catches data quality issues at ingestion
```

### Pattern 4: Protobuf for High Performance

Use Protocol Buffers for efficient serialization:

```python
# Define protobuf schema (event.proto)
"""
syntax = "proto3";

message Event {
    string user_id = 1;
    string event_type = 2;
    int64 timestamp = 3;
    bytes payload = 4;
}
"""

# Generate Python classes
# protoc --python_out=. event.proto

from databricks.zerobus import ZerobusClient
import event_pb2

client = ZerobusClient(
    host="workspace.cloud.databricks.com",
    token="dapi...",
    catalog="main",
    schema="bronze",
    table="events_proto",
    format="protobuf"  # Use protobuf format
)

# Send protobuf messages
event = event_pb2.Event(
    user_id="user123",
    event_type="purchase",
    timestamp=1234567890,
    payload=b"..."
)

client.write(event.SerializeToString())
```

**Benefits:** Smaller payload, faster serialization, type safety.

### Pattern 5: Monitoring and Observability

Track ingestion health:

```python
# Monitor table updates via Unity Catalog
spark.sql("""
    DESCRIBE HISTORY main.bronze.events
""").show(10, False)

# Check ingestion frequency
spark.sql("""
    SELECT 
        date_trunc('minute', _commit_timestamp) as minute,
        COUNT(*) as commits,
        SUM(operationMetrics.numOutputRows) as rows
    FROM (DESCRIBE HISTORY main.bronze.events)
    WHERE operation = 'STREAMING UPDATE'
    GROUP BY minute
    ORDER BY minute DESC
    LIMIT 60
""").show()

# File compaction happens automatically
# Check file count (should stay reasonable)
details = spark.sql("DESCRIBE DETAIL main.bronze.events").collect()[0]
print(f"Number of files: {details.numFiles}")
```

## Reference Files

- [Zerobus Ingest Documentation](https://docs.databricks.com/en/ingestion/zerobus-ingest.html)
- [Python SDK](https://github.com/databricks/zerobus-sdk-py)
- [Go SDK](https://github.com/databricks/zerobus-sdk-go)
- [Rust SDK](https://github.com/databricks/zerobus-sdk-rs)
- [Java SDK](https://github.com/databricks/zerobus-sdk-java)

## Common Issues

| Issue | Solution |
|-------|----------|
| **Records not appearing in table** | Check table schema matches record structure. Enable schema validation. |
| **NonRetriableException** | Schema mismatch or invalid data. Fix record format or create DLQ. |
| **High latency** | Zerobus writes every 1-5 seconds. Use multiple streams for higher throughput. |
| **Message size limit exceeded** | Max 10MB per message. Split large messages or use file-based ingestion. |
| **Schema evolution needed** | Currently not supported. Create new table version with updated schema. |
| **Single AZ downtime** | Expected behavior. Plan for brief unavailability during maintenance. |

## Advanced Tips

### When Kafka Still Wins

Zerobus doesn't replace Kafka in these scenarios:

```markdown
**Use Kafka when:**
- ✅ Exactly-once semantics required (financial transactions)
- ✅ Multiple consumers need same stream (fan-out pattern)
- ✅ Message retention/replay needed (audit logs)
- ✅ Multi-AZ durability required

**Use Zerobus when:**
- ✅ Append-only ingestion to Delta tables
- ✅ Single consumer (lakehouse)
- ✅ Cost optimization priority
- ✅ Simplified operations
```

### Performance Benchmarks

```python
# Tested performance (same region, 1KB messages)
# - 100 MB/second per stream
# - 15,000 rows/second per stream

# For higher throughput: Use multiple streams
def parallel_ingest(partitions):
    for partition_id, records in enumerate(partitions):
        # Create separate stream per partition
        client = ZerobusClient(
            host="...",
            token="...",
            catalog="main",
            schema="bronze",
            table=f"events_partition_{partition_id}"
        )
        
        for record in records:
            client.write(record)
        
        client.close()

# Aggregate partitioned tables with view
spark.sql("""
    CREATE VIEW main.bronze.events_all AS
    SELECT * FROM main.bronze.events_partition_*
""")
```

### Idempotent Writes Pattern

```python
# Zerobus provides at-least-once delivery
# Implement deduplication for exactly-once semantics

# Step 1: Add unique ID to records
record = {
    "event_id": str(uuid.uuid4()),  # Unique ID
    "user_id": "user123",
    "event_type": "purchase",
    "timestamp": "2026-01-01T00:00:00Z"
}

client.write(record)

# Step 2: Deduplicate downstream
spark.sql("""
    CREATE OR REPLACE TABLE main.silver.events_deduped AS
    SELECT * FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY timestamp) as rn
        FROM main.bronze.events
    )
    WHERE rn = 1
""")
```

### Cost Comparison

```python
# Rough cost comparison (monthly, single pipeline):
# - Kafka (MSK): ~$800/month (r6g.large, 3 brokers)
# - Zerobus: Included in DBU costs

# Additional savings:
# - No Kafka expertise needed (saves $150K+ in salary)
# - Faster development (weeks vs. months)
# - No operational overhead (24/7 monitoring)

# Total effective savings: 70-80% for simple ingestion
```

## FAQ

**Q: What's the max message size?**  
A: 10MB per message. For larger payloads, use file-based ingestion or chunk data.

**Q: Can I use Zerobus for CDC?**  
A: CDC support (updates/deletes) is on the roadmap. Currently append-only (inserts) only.

**Q: Does it support exactly-once semantics?**  
A: Currently at-least-once. Implement deduplication downstream using unique IDs for exactly-once.

**Q: Can I write to external tables?**  
A: No, only managed Delta tables currently supported.

**Q: What about schema evolution?**  
A: Not yet supported. Create new table version with updated schema.

**Q: Is multi-AZ durability available?**  
A: Currently single-AZ. Multi-AZ support planned for future releases.

**Q: Can multiple applications write to same table?**  
A: Yes! Zerobus handles concurrent writes from multiple clients.
