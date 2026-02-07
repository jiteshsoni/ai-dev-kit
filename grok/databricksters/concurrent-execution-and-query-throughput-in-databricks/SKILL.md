---
name: "concurrent-execution-query-throughput"
description: "Optimize Databricks SQL warehouse concurrency and query throughput through autoscaling, warehouse sizing, and workload management strategies."
---

# Concurrent Execution and Query Throughput in Databricks SQL

## Overview

This skill covers optimizing concurrent query execution and query throughput in Databricks SQL warehouses. Learn how autoscaling works, configure warehouse sizing for high-concurrency workloads, understand the relationship between concurrency limits and throughput, and implement strategies to support hundreds of concurrent users and millions of queries per hour.

## Quick Start

### Configure Autoscaling for High Concurrency
Set up warehouses to handle concurrent workloads:

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import EndpointInfoWarehouseType

def configure_high_concurrency_warehouse(name: str, min_clusters: int = 2, max_clusters: int = 5):
    """Configure SQL warehouse for high concurrency"""
    
    w = WorkspaceClient()
    
    # Create or update warehouse with autoscaling
    warehouse_config = {
        "name": name,
        "cluster_size": "Large",  # Larger clusters = faster queries = higher throughput
        "min_num_clusters": min_clusters,
        "max_num_clusters": max_clusters,
        "warehouse_type": EndpointInfoWarehouseType.PRO,
        "enable_photon": True,
        "enable_serverless_compute": False,  # Use Pro for predictable scaling
        "channel": {
            "name": "CHANNEL_NAME_CURRENT"
        }
    }
    
    # Create warehouse
    warehouse = w.warehouses.create(**warehouse_config)
    
    print(f"Warehouse '{name}' configured:")
    print(f"  Size: Large")
    print(f"  Clusters: {min_clusters}-{max_clusters}")
    print(f"  Max concurrent queries: {max_clusters * 10}")
    
    return warehouse

# Usage
warehouse = configure_high_concurrency_warehouse("high_concurrency_warehouse", min_clusters=2, max_clusters=5)
```

### Monitor Query Throughput
Track queries per hour and concurrency:

```python
def monitor_query_throughput(warehouse_id: str, hours: int = 1):
    """Monitor query throughput metrics"""
    
    from datetime import datetime, timedelta
    import pandas as pd
    
    # Query system tables for metrics
    query = f"""
    SELECT 
        COUNT(*) as total_queries,
        COUNT(DISTINCT user_name) as unique_users,
        AVG(execution_time_ms) / 1000.0 as avg_execution_seconds,
        PERCENTILE(execution_time_ms, 0.95) / 1000.0 as p95_execution_seconds,
        MAX(concurrency) as max_concurrent_queries
    FROM system.query.history
    WHERE warehouse_id = '{warehouse_id}'
      AND start_time >= CURRENT_TIMESTAMP() - INTERVAL {hours} HOUR
    """
    
    df = spark.sql(query)
    result = df.collect()[0]
    
    queries_per_hour = result['total_queries'] / hours
    
    print(f"Query Throughput Metrics ({hours}h window):")
    print(f"  Total queries: {result['total_queries']:,}")
    print(f"  Queries per hour: {queries_per_hour:,.0f}")
    print(f"  Unique users: {result['unique_users']}")
    print(f"  Avg execution: {result['avg_execution_seconds']:.2f}s")
    print(f"  P95 execution: {result['p95_execution_seconds']:.2f}s")
    print(f"  Max concurrent: {result['max_concurrent_queries']}")
    
    return result

# Usage
metrics = monitor_query_throughput("your-warehouse-id", hours=1)
```

## Common Patterns

### Pattern 1: Serverless for Instant Scaling
Use Serverless warehouses for near-instant scaling:

```python
def create_serverless_warehouse(name: str):
    """Create Serverless SQL warehouse for instant scaling"""
    
    w = WorkspaceClient()
    
    warehouse_config = {
        "name": name,
        "cluster_size": "Large",
        "min_num_clusters": 1,
        "max_num_clusters": 10,
        "warehouse_type": EndpointInfoWarehouseType.SERVERLESS,
        "enable_photon": True,
        "channel": {
            "name": "CHANNEL_NAME_CURRENT"
        }
    }
    
    warehouse = w.warehouses.create(**warehouse_config)
    
    print("Serverless warehouse benefits:")
    print("  - Near-instant scaling (no cluster startup delay)")
    print("  - Intelligent Workload Management (IWM)")
    print("  - Pre-provisioned resource pool")
    print("  - Automatic capacity prediction")
    
    return warehouse

# Usage
serverless_wh = create_serverless_warehouse("serverless_analytics")
```

### Pattern 2: Fixed Cluster Count for Predictable Performance
Use fixed clusters to avoid queuing:

```python
def create_fixed_cluster_warehouse(name: str, num_clusters: int = 2):
    """Create warehouse with fixed cluster count"""
    
    w = WorkspaceClient()
    
    warehouse_config = {
        "name": name,
        "cluster_size": "Large",
        "min_num_clusters": num_clusters,
        "max_num_clusters": num_clusters,  # Fixed count
        "warehouse_type": EndpointInfoWarehouseType.PRO,
        "enable_photon": True
    }
    
    warehouse = w.warehouses.create(**warehouse_config)
    
    print(f"Fixed cluster warehouse:")
    print(f"  Clusters: {num_clusters}")
    print(f"  Max concurrent queries: {num_clusters * 10}")
    print(f"  No queuing delay (clusters always available)")
    
    return warehouse

# Usage
fixed_wh = create_fixed_cluster_warehouse("predictable_workload", num_clusters=2)
```

### Pattern 3: Optimize Warehouse Size for Throughput
Larger warehouses = faster queries = higher throughput:

```python
def optimize_warehouse_size_for_throughput():
    """Choose optimal warehouse size based on workload"""
    
    size_recommendations = {
        "X-Small": {
            "concurrent_queries": 10,
            "use_case": "Development, small teams",
            "throughput": "Low"
        },
        "Small": {
            "concurrent_queries": 10,
            "use_case": "Small production workloads",
            "throughput": "Low-Medium"
        },
        "Medium": {
            "concurrent_queries": 10,
            "use_case": "Medium production workloads",
            "throughput": "Medium"
        },
        "Large": {
            "concurrent_queries": 10,
            "use_case": "High-throughput production",
            "throughput": "High",
            "recommended": True
        },
        "X-Large": {
            "concurrent_queries": 10,
            "use_case": "Very high-throughput",
            "throughput": "Very High"
        },
        "2X-Large": {
            "concurrent_queries": 10,
            "use_case": "Extreme throughput",
            "throughput": "Extreme"
        },
        "4X-Large": {
            "concurrent_queries": 10,
            "use_case": "World-record workloads",
            "throughput": "Maximum"
        }
    }
    
    print("Warehouse Size Recommendations:")
    for size, config in size_recommendations.items():
        marker = " ⭐" if config.get("recommended") else ""
        print(f"\n{size}{marker}:")
        print(f"  Concurrent queries per cluster: {config['concurrent_queries']}")
        print(f"  Use case: {config['use_case']}")
        print(f"  Throughput: {config['throughput']}")
    
    return size_recommendations

# Usage
recommendations = optimize_warehouse_size_for_throughput()
```

### Pattern 4: BI Dashboard Optimization
Optimize for dashboard refresh workloads:

```python
def optimize_for_bi_dashboards(warehouse_id: str, dashboard_query_count: int = 20):
    """Optimize warehouse for BI dashboard workloads"""
    
    # Calculate required clusters
    # Rule: 1 cluster per 10 concurrent queries
    # Dashboard with 20 queries needs 2 clusters minimum
    
    min_clusters = max(1, (dashboard_query_count // 10) + 1)
    max_clusters = min_clusters * 2  # Allow scaling for multiple dashboards
    
    w = WorkspaceClient()
    
    # Update warehouse configuration
    w.warehouses.edit(
        id=warehouse_id,
        min_num_clusters=min_clusters,
        max_num_clusters=max_clusters
    )
    
    print(f"BI Dashboard Optimization:")
    print(f"  Dashboard queries: {dashboard_query_count}")
    print(f"  Min clusters: {min_clusters}")
    print(f"  Max clusters: {max_clusters}")
    print(f"  Max concurrent queries: {max_clusters * 10}")
    print(f"  Expected refresh time: < 1 minute (with Large warehouse)")

# Usage
optimize_for_bi_dashboards("your-warehouse-id", dashboard_query_count=20)
```

## Reference Files

- [SQL Warehouse Behavior](https://docs.databricks.com/en/compute/sql-warehouse/warehouse-behavior.html) - Autoscaling and queuing
- [Performance Benchmarks](https://www.databricks.com/blog/2021/11/02/databricks-sets-official-data-warehousing-performance-record.html) - World record throughput
- [Serverless SQL Warehouses](https://docs.databricks.com/en/compute/sql-warehouse/serverless.html) - Serverless benefits

## Common Issues

| Issue | Solution |
|-------|----------|
| **Query queuing** | Increase min_clusters or use fixed cluster count |
| **Low throughput** | Use larger warehouse size, enable Photon |
| **Slow scaling** | Use Serverless warehouses for instant scaling |
| **High costs** | Optimize query performance, use result caching |
| **Concurrent limit reached** | Increase max_clusters or use multiple warehouses |

## Key Takeaways

1. **Concurrency Rule**: 1 cluster per 10 concurrent queries (recommended)
2. **Throughput**: Larger warehouses = faster queries = higher throughput
3. **Autoscaling**: Min/Max clusters control scaling behavior
4. **Serverless**: Near-instant scaling with Intelligent Workload Management
5. **Fixed Clusters**: Avoid queuing by setting min = max
6. **Benchmarks**: 14,777 QPH (10GB) to 32M+ QPH (100TB) demonstrated

## Performance Benchmarks

### Small-Scale Workloads
```python
# Benchmark: 14,777 queries per hour on 10GB dataset
# Warehouse: DBSQL Large, no autoscaling
# Use case: Multi-tenant environments, small datasets

def benchmark_small_scale():
    """Small-scale workload benchmark"""
    return {
        "queries_per_hour": 14777,
        "dataset_size": "10GB",
        "warehouse_size": "Large",
        "dashboards_per_hour": 738,  # Assuming 20 queries per dashboard
        "use_case": "Multi-tenant, small datasets"
    }
```

### Large-Scale Workloads
```python
# Benchmark: 32,941,245 queries per hour on 100TB dataset
# Warehouse: DBSQL 4X-Large, no autoscaling
# World record: TPC-DS benchmark

def benchmark_large_scale():
    """Large-scale workload benchmark"""
    return {
        "queries_per_hour": 32941245,
        "dataset_size": "100TB",
        "warehouse_size": "4X-Large",
        "dashboards_per_hour": 1647062,  # Assuming 20 queries per dashboard
        "use_case": "Enterprise-scale analytical workloads"
    }
```

## Autoscaling Strategy

### When to Use Autoscaling
```python
def autoscaling_strategy(workload_type: str):
    """Choose autoscaling strategy based on workload"""
    
    strategies = {
        "predictable": {
            "min_clusters": 2,
            "max_clusters": 2,
            "reason": "Fixed cluster count avoids queuing"
        },
        "variable": {
            "min_clusters": 1,
            "max_clusters": 5,
            "reason": "Scale based on demand"
        },
        "high_concurrency": {
            "min_clusters": 3,
            "max_clusters": 10,
            "reason": "Support many concurrent users"
        },
        "cost_optimized": {
            "min_clusters": 1,
            "max_clusters": 3,
            "reason": "Minimize costs while handling peaks"
        }
    }
    
    return strategies.get(workload_type, strategies["variable"])

# Usage
strategy = autoscaling_strategy("high_concurrency")
print(f"Min clusters: {strategy['min_clusters']}")
print(f"Max clusters: {strategy['max_clusters']}")
print(f"Reason: {strategy['reason']}")
```

## Query Throughput Optimization

### Enable Result Caching
```python
def enable_result_caching():
    """Enable result caching for repeated queries"""
    
    # Result caching dramatically improves throughput
    # Repeated queries served from cache = no compute cost
    
    spark.conf.set("spark.databricks.query.resultCache.enabled", "true")
    spark.conf.set("spark.databricks.query.resultCache.maxSize", "10GB")
    
    print("Result caching enabled:")
    print("  - Repeated queries served from cache")
    print("  - Zero compute cost for cached results")
    print("  - Dramatically improved throughput")

# Usage
enable_result_caching()
```

### Optimize Query Performance
```python
def optimize_query_performance():
    """Query optimization tips for higher throughput"""
    
    optimizations = [
        "Use Photon engine (enabled by default)",
        "Enable result caching for repeated queries",
        "Use appropriate warehouse size (larger = faster)",
        "Optimize table layout (Liquid Clustering, Z-order)",
        "Use materialized views for common aggregations",
        "Partition tables appropriately",
        "Avoid SELECT * - select only needed columns",
        "Use query filters early in the pipeline"
    ]
    
    print("Query Performance Optimizations:")
    for i, opt in enumerate(optimizations, 1):
        print(f"  {i}. {opt}")
    
    return optimizations

# Usage
optimize_query_performance()
```

## When to Use This Skill

- Configuring warehouses for high-concurrency workloads
- Optimizing BI dashboard performance
- Scaling to support hundreds of concurrent users
- Maximizing query throughput
- Choosing between Pro, Classic, and Serverless warehouses
- Understanding autoscaling behavior

## Related Skills

- sql-warehouse-configuration
- query-performance-optimization
- autoscaling-strategies
- bi-dashboard-optimization