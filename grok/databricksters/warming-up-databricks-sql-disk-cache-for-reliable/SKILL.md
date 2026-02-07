---
name: "warm-sql-disk-cache-benchmarking"
description: "Warm up Databricks SQL disk cache for reliable BI dashboard benchmarking: extract historical queries, replay with concurrency, avoid result cache, and visualize cache stabilization."
---

# Warming Up Databricks SQL Disk Cache for Reliable Benchmarking

## Overview

This skill covers warming up Databricks SQL disk cache before benchmarking BI dashboards to ensure realistic, steady-state performance measurements. Learn how to extract historical queries from system.query.history, replay them with controlled concurrency, bypass result cache to exercise disk cache, and visualize cache stabilization for reliable benchmarks.

## Quick Start

### Extract Historical Queries
Get real production queries for warm-up:

```python
from datetime import datetime, timedelta
from databricks import sql

# Configuration
EXECUTED_BY = "dashboard-user@company.com"  # User whose queries represent dashboard workload
WAREHOUSE_ID = "abc123def456"  # Source warehouse for query history
DAYS_BACK = 7  # Time range to extract

start_time_utc = datetime.utcnow() - timedelta(days=DAYS_BACK)
end_time_utc = datetime.utcnow()

# Extract historical queries
query = f"""
SELECT statement_text
FROM system.query.history
WHERE start_time >= '{start_time_utc.isoformat()}'
  AND start_time <= '{end_time_utc.isoformat()}'
  AND executed_by = '{EXECUTED_BY}'
  AND compute.warehouse_id = '{WAREHOUSE_ID}'
  AND statement_text LIKE 'SELECT%'  -- Only SELECT queries
ORDER BY start_time
"""

# Execute and collect queries
historical_queries = spark.sql(query).collect()
queries = [row['statement_text'] for row in historical_queries]

print(f"Extracted {len(queries)} historical queries")
```

### Modify Queries to Bypass Result Cache
Inject dynamic column to force fresh execution:

```python
def modify_query_to_bypass_cache(query: str) -> str:
    """
    Inject NOW() to bypass result cache while preserving scan patterns.
    
    This forces fresh execution but maintains same data access patterns
    for disk cache warm-up.
    """
    # Simple injection: add NOW() AS injected_now to SELECT
    if query.strip().upper().startswith('SELECT'):
        # Find first SELECT and inject column
        modified = query.replace(
            'SELECT',
            'SELECT NOW() AS injected_now,',
            1
        )
        return modified
    return query

# Modify all queries
modified_queries = [modify_query_to_bypass_cache(q) for q in queries]

# Alternative: Disable result cache for session
# SET use_cached_result = false;
```

### Replay Queries with Concurrency
Warm up cache with realistic workload:

```python
from databricks import sql
import concurrent.futures
import time

# DBSQL connection config
DBSQL_CONFIG = {
    "server_hostname": dbutils.secrets.get(scope="dbsql", key="hostname"),
    "http_path": dbutils.secrets.get(scope="dbsql", key="http_path"),
    "access_token": dbutils.secrets.get(scope="dbsql", key="token")
}

def execute_query(query: str):
    """Execute single query"""
    conn = sql.connect(**DBSQL_CONFIG)
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        cursor.fetchall()  # Consume results
    finally:
        cursor.close()
        conn.close()

def run_warmup(queries: list, passes: int = 5, concurrency: int = 10, delay_sec: int = 2):
    """
    Run warm-up passes with concurrency.
    
    Args:
        queries: List of queries to replay
        passes: Number of warm-up iterations
        concurrency: Concurrent queries per pass (10 per cluster recommended)
        delay_sec: Delay between passes
    """
    durations = []
    
    for i in range(passes):
        start = time.time()
        
        # Execute queries concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            pool.map(execute_query, queries)
        
        duration = round(time.time() - start, 2)
        durations.append(duration)
        
        print(f"Pass {i+1}/{passes}: {duration}s")
        
        if i < passes - 1:
            time.sleep(delay_sec)
    
    return durations

# Run warm-up
# Concurrency: 10 per cluster (if 2 clusters, use 20)
durations = run_warmup(modified_queries, passes=5, concurrency=10, delay_sec=2)
```

## Common Patterns

### Pattern 1: Visualize Cache Stabilization
Confirm cache has reached steady state:

```python
import matplotlib.pyplot as plt

def visualize_cache_warmup(durations: list):
    """Visualize cache warm-up convergence"""
    
    plt.figure(figsize=(10, 6))
    plt.bar(range(1, len(durations)+1), durations, color='steelblue', alpha=0.7)
    plt.title("Execution Time per Warm-up Pass", fontsize=14, fontweight='bold')
    plt.xlabel("Pass Number", fontsize=12)
    plt.ylabel("Duration (seconds)", fontsize=12)
    plt.grid(axis='y', alpha=0.3)
    
    # Add trend line
    if len(durations) > 1:
        z = np.polyfit(range(1, len(durations)+1), durations, 1)
        p = np.poly1d(z)
        plt.plot(range(1, len(durations)+1), p(range(1, len(durations)+1)), 
                "r--", alpha=0.5, label="Trend")
        plt.legend()
    
    plt.tight_layout()
    plt.show()
    
    # Check convergence
    if len(durations) >= 3:
        recent_avg = np.mean(durations[-3:])
        first = durations[0]
        improvement = ((first - recent_avg) / first) * 100
        
        print(f"\nCache Warm-up Analysis:")
        print(f"  First pass: {durations[0]:.2f}s")
        print(f"  Recent average (last 3): {recent_avg:.2f}s")
        print(f"  Improvement: {improvement:.1f}%")
        
        if abs(durations[-1] - durations[-2]) / durations[-2] < 0.1:
            print(f"  ✓ Cache stabilized!")
        else:
            print(f"  ⚠ Cache may need more passes")

# Usage
visualize_cache_warmup(durations)
```

### Pattern 2: Separate Extraction and Warm-up Warehouses
Use different warehouses for safety:

```python
def warm_up_benchmark_warehouse_from_production():
    """
    Extract queries from production warehouse,
    warm up benchmark warehouse.
    """
    
    # Step 1: Extract from production warehouse
    PRODUCTION_WAREHOUSE_ID = "prod-warehouse-123"
    BENCHMARK_WAREHOUSE_ID = "benchmark-warehouse-456"
    
    # Extract queries (lightweight, metadata-only)
    extraction_query = f"""
    SELECT statement_text
    FROM system.query.history
    WHERE compute.warehouse_id = '{PRODUCTION_WAREHOUSE_ID}'
      AND start_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
      AND executed_by LIKE '%dashboard%'
    """
    
    # Execute on any warehouse with access to system.query.history
    queries = spark.sql(extraction_query).collect()
    
    # Step 2: Warm up benchmark warehouse
    # Connect to benchmark warehouse
    BENCHMARK_CONFIG = {
        "server_hostname": BENCHMARK_WAREHOUSE_HOSTNAME,
        "http_path": BENCHMARK_WAREHOUSE_HTTP_PATH,
        "access_token": BENCHMARK_TOKEN
    }
    
    # Replay queries on benchmark warehouse
    modified_queries = [modify_query_to_bypass_cache(q['statement_text']) for q in queries]
    durations = run_warmup(modified_queries, concurrency=10)
    
    print("Benchmark warehouse warmed up with production-like workload!")
```

### Pattern 3: Calculate Optimal Concurrency
Match concurrency to cluster count:

```python
def calculate_optimal_concurrency(num_clusters: int = 1):
    """
    Calculate optimal concurrency for warm-up.
    
    Rule: ~10 concurrent queries per cluster
    """
    optimal = num_clusters * 10
    
    print(f"Optimal concurrency:")
    print(f"  Clusters: {num_clusters}")
    print(f"  Recommended concurrency: {optimal}")
    print(f"  Rationale: Ensures all clusters participate in warm-up")
    
    return optimal

# Usage
# For warehouse with min=2, max=2 clusters:
optimal_concurrency = calculate_optimal_concurrency(num_clusters=2)  # Returns 20
```

### Pattern 4: Disable Result Cache for Session
Alternative to query modification:

```python
def warmup_with_cache_disabled(queries: list):
    """Warm up with result cache disabled"""
    
    conn = sql.connect(**DBSQL_CONFIG)
    cursor = conn.cursor()
    
    try:
        # Disable result cache for session
        cursor.execute("SET use_cached_result = false")
        
        # Execute queries (no modification needed)
        for query in queries:
            cursor.execute(query)
            cursor.fetchall()
    
    finally:
        cursor.close()
        conn.close()
    
    print("Warm-up completed with result cache disabled")
```

## Reference Files

- [Query Caching](https://docs.databricks.com/en/sql/user/queries/query-caching.html) - Cache documentation
- [system.query.history](https://docs.databricks.com/en/sql/system-tables/query-history.html) - Query history table
- [Companion Notebook](https://github.com/ArtemChebotko/databricks-dbsql-disk-cache) - Full implementation

## Common Issues

| Issue | Solution |
|-------|----------|
| **Result cache shortcuts** | Inject NOW() or disable use_cached_result |
| **Cache not warming** | Increase concurrency (10 per cluster), run more passes |
| **Benchmark inconsistent** | Always warm cache before benchmarking |
| **Queries too slow** | Filter to most common queries, limit time range |
| **Production impact** | Use separate extraction and warm-up warehouses |

## Key Takeaways

1. **Warm Cache Required**: Production dashboards run on warm cache, not cold
2. **Historical Queries**: Extract real queries from system.query.history
3. **Bypass Result Cache**: Inject NOW() or disable use_cached_result
4. **Concurrency**: ~10 queries per cluster for optimal warm-up
5. **Visualization**: Monitor convergence to confirm cache stabilization
6. **Warehouse Separation**: Extract from production, warm up benchmark warehouse

## Complete Workflow

```python
def complete_cache_warmup_workflow():
    """Complete cache warm-up workflow"""
    
    # Step 1: Extract historical queries
    queries = extract_historical_queries(
        executed_by="dashboard-user@company.com",
        warehouse_id="prod-warehouse-123",
        days_back=7
    )
    
    # Step 2: Modify to bypass result cache
    modified_queries = [modify_query_to_bypass_cache(q) for q in queries]
    
    # Step 3: Calculate optimal concurrency
    num_clusters = 2  # From warehouse config
    concurrency = calculate_optimal_concurrency(num_clusters)
    
    # Step 4: Run warm-up passes
    durations = run_warmup(
        modified_queries,
        passes=5,
        concurrency=concurrency,
        delay_sec=2
    )
    
    # Step 5: Visualize and confirm stabilization
    visualize_cache_warmup(durations)
    
    # Step 6: Benchmark dashboard
    print("Cache warmed! Ready for benchmarking.")
    
    return durations

# Usage
durations = complete_cache_warmup_workflow()
```

## When to Use This Skill

- Benchmarking BI dashboards (Power BI, Tableau, AIBI)
- Comparing tool performance fairly
- Testing dashboard changes
- Tuning warehouse configurations
- Ensuring consistent performance measurements

## Related Skills

- sql-warehouse-configuration
- query-performance-optimization
- bi-dashboard-benchmarking
- cache-management-strategies