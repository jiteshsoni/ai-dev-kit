---
name: "SQL Warehouse Query Throughput"
description: "Maximize concurrent query throughput using warehouse scaling, query queuing, and optimization techniques."
author: "Databricksters"
url: "https://www.databricksters.com/p/concurrent-execution-and-query-throughput-in-datab"
date: "2025"
tags: ["concurrency", "throughput", "sql-warehouses", "performance", "databricks"]
---

# SQL Warehouse Query Throughput

## Overview

SQL warehouse throughput depends on size, scaling policy, and query complexity. Maximize throughput: enable auto-scaling, optimize queries for parallelism, and monitor queue times.

## Quick Start

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Create auto-scaling warehouse for high concurrency
warehouse = w.warehouses.create(
    name="high-throughput-warehouse",
    cluster_size="Large",
    min_num_clusters=1,
    max_num_clusters=10,  # Scale to 10 clusters
    auto_stop_mins=5,
    enable_photon=True,
    enable_serverless_compute=False
)

# Monitor throughput
spark.sql("""
  SELECT 
    DATE_TRUNC('minute', start_time) as minute,
    COUNT(*) as queries_per_minute
  FROM system.query.history
  WHERE warehouse_id = 'warehouse_id'
    AND start_time >= current_timestamp() - INTERVAL 1 HOUR
  GROUP BY minute
  ORDER BY minute
""").show()
```

## Common Patterns

### Pattern 1: Queue Monitoring

```sql
SELECT 
  query_id,
  queue_duration_ms,
  execution_duration_ms,
  queue_duration_ms / execution_duration_ms as queue_ratio
FROM system.query.history
WHERE warehouse_id = 'warehouse_id'
ORDER BY queue_duration_ms DESC
LIMIT 20;

-- High queue_ratio → Need more clusters
```

## FAQ

**Q: What's optimal cluster count?**  
A: Monitor queue times. Scale up if >10s average queue time.

**Q: Serverless vs classic for concurrency?**  
A: Serverless scales instantly, better for bursty workloads.
