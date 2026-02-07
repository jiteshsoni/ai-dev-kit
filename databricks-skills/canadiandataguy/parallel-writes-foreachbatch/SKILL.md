---
name: "Speeding Up Spark Streaming: Mastering Parallel Writes in ForEachBatch"
description: "Achieve parallel writes within ForEachBatch using Python threading to reduce latency when writing to multiple destinations."
author: "Canadian Data Guy"
url: "https://www.youtube.com/watch?v=n9jodzYq1e4"
date: "2025-11-29"
tags: ["spark-streaming", "foreachbatch", "parallel-writes", "threading", "performance"]
---

# Parallel Writes in ForEachBatch

## Overview

When using `forEachBatch` to write to multiple targets, sequential writes increase latency. Use Python threading with `concurrent.futures` to parallelize writes within each microbatch.

**Use Case**: Demultiplex one stream into multiple tables (e.g., device-specific tables)

## Quick Start

### Basic Parallel Write Pattern

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

class ParallelWriter:
    def __init__(self, catalog, database):
        self.catalog = catalog
        self.database = database
    
    def process_table(self, device_type, batch_id, df):
        """Write one device type to its table"""
        target_table = f"{self.catalog}.{self.database}.{device_type}"
        
        (df.filter(col("device_type") == device_type)
           .write
           .format("delta")
           .mode("append")
           .saveAsTable(target_table))
        
        return f"Completed: {device_type}"
    
    def process_microbatch(self, df, batch_id):
        """Process microbatch with parallel writes"""
        # Cache to avoid recomputation
        df.cache()
        
        device_types = ["sensor", "actuator", "gateway", "controller"]
        errors = []
        
        # Parallel execution with 2 threads
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(
                    self.process_table, dt, batch_id, df
                ): dt for dt in device_types
            }
            
            for future in as_completed(futures):
                device_type = futures[future]
                try:
                    result = future.result()
                    print(f"{result} at {datetime.now()}")
                except Exception as e:
                    errors.append((device_type, str(e)))
        
        # Release cache
        df.unpersist()
        
        # Fail microbatch if any writes failed
        if errors:
            raise Exception(f"Failed writes: {errors}")
```

### Streaming Query Setup

```python
# Streaming source
stream_df = (spark
    .readStream
    .format("delta")
    .load("/path/to/source")
    .option("maxFilesPerTrigger", 500)  # Control microbatch size
)

# ForEachBatch with parallel writes
writer = ParallelWriter("catalog", "database")

query = (stream_df
    .writeStream
    .foreachBatch(writer.process_microbatch)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .queryName("parallel_foreachbatch")
    .start()
)
```

## Common Patterns

### Pattern 1: Controlled Concurrency

```python
# Adjust max_workers based on:
# - Number of target tables
# - Cluster resources
# - Target system capacity

with ThreadPoolExecutor(max_workers=4) as executor:
    # 4 parallel writes
    # If 8 tables: 2 waves of 4
    pass

# Start conservative (2 workers), increase based on monitoring
```

### Pattern 2: Error Handling

```python
def process_microbatch(self, df, batch_id):
    df.cache()
    device_types = get_device_types(df)
    exceptions = []
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(write_table, dt, df): dt 
            for dt in device_types
        }
        
        for future in as_completed(futures):
            device_type = futures[future]
            try:
                future.result()
            except Exception as e:
                exceptions.append((device_type, e))
    
    df.unpersist()
    
    # Collect all errors before failing
    if exceptions:
        raise Exception(f"Write failures: {exceptions}")
```

### Pattern 3: Deduplication Configuration

```python
# When using parallel writes, enable Delta deduplication
(df
    .write
    .format("delta")
    .option("txnVersion", batch_id)  # Idempotent writes
    .option("txnAppId", "streaming_job_name")
    .mode("append")
    .saveAsTable(target)
)
```

## Reference Files

### Threading Model

```
Microbatch arrives
       ↓
   Cache DF
       ↓
  ┌────┴────┐
  │ Thread 1│ → Write Table A
  │ Thread 2│ → Write Table B
  └────┬────┘
       ↓
  Wait for completion
       ↓
   Unpersist
```

### Performance Comparison

| Approach | Latency (4 tables) | Resource Usage |
|----------|-------------------|----------------|
| Sequential | 4 × single write | Lower |
| Parallel (2 workers) | 2 × single write | Medium |
| Parallel (4 workers) | ~1 × single write | Higher |

## Common Issues

| Issue | Solution |
|-------|----------|
| **Too many threads** | Reduce max_workers; watch cluster CPU/memory |
| **Target system overloaded** | Match workers to target capacity |
| **DataFrame not cached** | Always cache before parallel operations |
| **Partial write success** | Collect all errors; fail batch if any error |
| **Memory issues** | Unpersist after writes; control microbatch size |

## Advanced Tips

### Optimal Thread Count

```python
# Formula: min(target_tables, cluster_cores / 2)
# Example: 8 tables, 8 cores → 4 workers
# Example: 4 tables, 4 cores → 2 workers

max_workers = min(len(device_types), max(2, total_cores // 2))
```

### Monitoring Parallel Writes

```python
# Add timestamps to track parallel execution:
from datetime import datetime

def process_table(self, device_type, batch_id, df):
    start = datetime.now()
    # ... write logic ...
    end = datetime.now()
    print(f"{device_type}: {start} → {end} ({end-start})")
    return device_type

# Look for overlapping timestamps = true parallelism
```

### Microbatch Size Control

```python
# Prevent oversized microbatches
stream_df = (spark
    .readStream
    .format("kafka")
    .option("maxOffsetsPerTrigger", 10000)  # Kafka
    .load()
)

# or
stream_df = (spark
    .readStream
    .format("delta")
    .option("maxFilesPerTrigger", 500)  # Delta
    .load()
)
```

## FAQ

**Q: How many threads should I use?**
A: Start with 2, monitor cluster utilization, increase gradually. Typical: 2-4 workers.

**Q: Can I use multiprocessing instead of threading?**
A: Not recommended within Spark executors. Use threading.

**Q: What if one table write fails?**
A: Design for atomic microbatch: all succeed or all fail. Use error collection pattern.

**Q: Does parallel writing increase throughput?**
A: Reduces latency for multi-target writes. Throughput depends on target systems.

**Q: Should I always use parallel writes?**
A: Only when writing to multiple targets. Single target doesn't benefit.