---
name: "Warm SQL Disk Cache for BI Benchmarking"
description: "Warm Databricks SQL disk cache using historical queries to ensure reliable, production-like BI dashboard benchmarks instead of cold-start measurements."
author: "Artem Chebotko"
url: "https://www.databricksters.com/p/warming-up-databricks-sql-disk-cache"
date: "2025-11-04"
tags: ["sql-warehouses", "benchmarking", "caching", "bi-tools", "performance", "databricks"]
---

# Warm SQL Disk Cache for BI Benchmarking

## Overview

Production BI dashboards run on warm warehouses with populated disk caches, not cold starts. Benchmarking against a cold cache measures only the first run after restart—not actual user experience. This skill provides a production-friendly warm-up approach using historical query replay to ensure BI dashboard benchmarks reflect steady-state performance.

**Use this skill when:** Benchmarking BI tools (Power BI, Tableau, AI/BI Dashboards), comparing warehouse performance, or validating dashboard optimizations.

## Quick Start

Basic warm-up process:

```python
from databricks import sql
import time

# 1. Extract historical queries from production warehouse
queries = spark.sql("""
  SELECT statement_text
  FROM system.query.history
  WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
    AND executed_by = 'dashboard-service-principal'
    AND compute.warehouse_id = 'prod-warehouse-id'
    AND statement_type = 'SELECT'
  ORDER BY start_time
  LIMIT 100
""").collect()

query_list = [row.statement_text for row in queries]

# 2. Modify queries to avoid result cache
def bypass_result_cache(query):
    # Inject NOW() to force fresh execution
    if "SELECT" in query.upper():
        query = query.replace("SELECT", "SELECT NOW() as _cache_bypass,", 1)
    return query

modified_queries = [bypass_result_cache(q) for q in query_list]

# 3. Execute warm-up (simple version)
conn = sql.connect(
    server_hostname="your-workspace.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/benchmark-warehouse-id",
    access_token="dapi..."
)

for query in modified_queries:
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.fetchall()
    cursor.close()

conn.close()
print("Warm-up complete!")
```

## Common Patterns

### Pattern 1: Historical Query Extraction

Extract queries that represent actual dashboard workload.

```python
from datetime import datetime, timedelta

# Configure extraction parameters
EXECUTED_BY = "dashboard-service-principal@company.com"
WAREHOUSE_ID = "abc123def456"
LOOKBACK_DAYS = 7

start_time = datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)
end_time = datetime.utcnow()

# Extract SELECT queries from production
query_text = f"""
  SELECT 
    statement_text,
    execution_duration,
    read_bytes
  FROM system.query.history
  WHERE start_time >= '{start_time.isoformat()}'
    AND start_time <= '{end_time.isoformat()}'
    AND executed_by = '{EXECUTED_BY}'
    AND compute.warehouse_id = '{WAREHOUSE_ID}'
    AND statement_type = 'SELECT'
    AND error_message IS NULL
  ORDER BY start_time
"""

queries_df = spark.sql(query_text)
queries = [row.statement_text for row in queries_df.collect()]

print(f"Extracted {len(queries)} queries for warm-up")
```

**Best practice:** Use a representative time window (7-30 days) that captures typical dashboard patterns.

### Pattern 2: Bypass Result Cache

Prevent result cache shortcuts during warm-up.

```python
# Method 1: Inject dynamic column (recommended)
def inject_now_column(query):
    """Add NOW() to force fresh execution while preserving scan patterns"""
    import re
    
    # Handle simple SELECT
    if query.strip().upper().startswith("SELECT"):
        return query.replace("SELECT", "SELECT NOW() AS _warmup_timestamp,", 1)
    
    # Handle WITH clauses (CTEs)
    elif "WITH" in query.upper():
        # Find the final SELECT in the CTE chain
        pattern = r'(SELECT\s+)(?!.*WITH)'
        return re.sub(pattern, r'\1NOW() AS _warmup_timestamp, ', query, count=1, flags=re.IGNORECASE)
    
    return query

# Method 2: Disable result cache for session (alternative)
def disable_result_cache(conn):
    """Disable result cache at session level"""
    cursor = conn.cursor()
    cursor.execute("SET use_cached_result = false")
    cursor.close()

# Usage
modified_queries = [inject_now_column(q) for q in queries]

# Or use session-level disable
conn = sql.connect(**config)
disable_result_cache(conn)
```

**Why this matters:** Without bypassing result cache, queries return instantly from cache, defeating the purpose of warming disk cache.

### Pattern 3: Concurrent Query Replay

Simulate dashboard burst patterns with concurrency.

```python
import concurrent.futures
from databricks import sql

DBSQL_CONFIG = {
    "server_hostname": "your-workspace.cloud.databricks.com",
    "http_path": "/sql/1.0/warehouses/benchmark-id",
    "access_token": "dapi..."
}

def execute_query(query):
    """Execute a single query with fresh connection"""
    conn = sql.connect(**DBSQL_CONFIG)
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

def run_warmup_pass(queries, concurrency=10):
    """Run queries concurrently to simulate dashboard refresh"""
    start = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        list(executor.map(execute_query, queries))
    
    duration = time.time() - start
    return round(duration, 2)

# Run multiple passes until cache stabilizes
def warmup_cache(queries, passes=5, concurrency=10, delay_seconds=5):
    """Execute warm-up passes and track convergence"""
    durations = []
    
    for i in range(1, passes + 1):
        print(f"Running warm-up pass {i}...")
        duration = run_warmup_pass(queries, concurrency)
        durations.append(duration)
        print(f"  Pass {i} completed in {duration}s")
        
        if i < passes:
            time.sleep(delay_seconds)
    
    return durations

# Execute warm-up
durations = warmup_cache(modified_queries, passes=5, concurrency=10)
```

**Concurrency guideline:**
- **Single cluster warehouse:** 10 concurrent queries
- **Multi-cluster warehouse:** 10 × N (where N = number of clusters)

### Pattern 4: Visualize Cache Stabilization

Confirm cache has reached steady state before benchmarking.

```python
import matplotlib.pyplot as plt

def visualize_warmup(durations):
    """Plot warm-up durations to confirm convergence"""
    plt.figure(figsize=(10, 6))
    plt.bar(range(1, len(durations) + 1), durations, color='steelblue', alpha=0.7)
    plt.axhline(y=min(durations), color='red', linestyle='--', label='Steady State')
    plt.title('Disk Cache Warm-Up Convergence', fontsize=14, fontweight='bold')
    plt.xlabel('Warm-Up Pass', fontsize=12)
    plt.ylabel('Duration (seconds)', fontsize=12)
    plt.legend()
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.show()
    
    # Analyze convergence
    first_pass = durations[0]
    steady_state = min(durations)
    improvement = ((first_pass - steady_state) / first_pass) * 100
    
    print(f"\nCache Warm-Up Analysis:")
    print(f"  First pass (cold): {first_pass}s")
    print(f"  Steady state: {steady_state}s")
    print(f"  Improvement: {improvement:.1f}%")
    print(f"  Converged: {'Yes' if durations[-1] <= steady_state * 1.1 else 'No'}")

# Usage
visualize_warmup(durations)
```

**Expected pattern:** First pass is slowest (cold cache). Subsequent passes should flatten, indicating warm cache.

### Pattern 5: Separate Extraction and Warm-Up Warehouses

Safely warm a benchmark warehouse using production workload.

```python
# Configuration
EXTRACTION_CONFIG = {
    "server_hostname": "workspace.cloud.databricks.com",
    "http_path": "/sql/1.0/warehouses/small-utility-warehouse",
    "access_token": "dapi..."
}

WARMUP_CONFIG = {
    "server_hostname": "workspace.cloud.databricks.com",
    "http_path": "/sql/1.0/warehouses/benchmark-warehouse",
    "access_token": "dapi..."
}

# Step 1: Extract queries from production (lightweight metadata operation)
extraction_conn = sql.connect(**EXTRACTION_CONFIG)
cursor = extraction_conn.cursor()

cursor.execute(f"""
  SELECT statement_text
  FROM system.query.history
  WHERE compute.warehouse_id = 'prod-warehouse-id'
    AND start_time >= current_timestamp() - INTERVAL 7 DAYS
    AND statement_type = 'SELECT'
  LIMIT 200
""")

queries = [row.statement_text for row in cursor.fetchall()]
cursor.close()
extraction_conn.close()

# Step 2: Warm up benchmark warehouse (isolated environment)
modified_queries = [inject_now_column(q) for q in queries]
durations = warmup_cache(modified_queries, passes=5, concurrency=10)

# Step 3: Begin actual benchmark
print("Cache warmed! Ready for benchmark.")
```

**Benefits:**
- Production warehouse remains untouched
- Safe isolation of benchmark testing
- Realistic workload simulation

## Reference Files

- [Notebook: Disk Cache Warming](https://github.com/ArtemChebotko/databricks-dbsql-disk-cache/blob/main/Warming%20Up%20Databricks%20SQL%20Disk%20Cache%20for%20Reliable%20BI%20Dashboard%20Benchmarking.py)
- [Query Caching Documentation](https://docs.databricks.com/en/sql/user/queries/query-caching)
- [SQL Warehouse Configuration](https://docs.databricks.com/en/sql/admin/sql-endpoints.html)

## Common Issues

| Issue | Solution |
|-------|----------|
| **Queries return instantly (result cache hit)** | Inject `NOW()` column or set `use_cached_result = false`. |
| **Durations not converging** | Increase warm-up passes or check warehouse auto-stop settings. |
| **First pass too fast** | Warehouse may already be warm. Stop and restart warehouse. |
| **Connection timeouts** | Reduce concurrency or increase statement timeout settings. |
| **Out of memory errors** | Lower concurrency to match warehouse size. |
| **Queries failing during warm-up** | Filter out failed queries from historical extraction. |

## Advanced Tips

### Complete Warm-Up Script

```python
# Complete production-ready warm-up implementation
class DBSQLCacheWarmer:
    def __init__(self, extraction_config, warmup_config):
        self.extraction_config = extraction_config
        self.warmup_config = warmup_config
    
    def extract_queries(self, warehouse_id, days=7, limit=200):
        """Extract historical queries from production warehouse"""
        conn = sql.connect(**self.extraction_config)
        cursor = conn.cursor()
        
        query = f"""
          SELECT statement_text
          FROM system.query.history
          WHERE compute.warehouse_id = '{warehouse_id}'
            AND start_time >= current_timestamp() - INTERVAL {days} DAYS
            AND statement_type = 'SELECT'
            AND error_message IS NULL
          ORDER BY start_time
          LIMIT {limit}
        """
        
        cursor.execute(query)
        queries = [row.statement_text for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return queries
    
    def modify_for_warmup(self, queries):
        """Inject NOW() to bypass result cache"""
        return [q.replace("SELECT", "SELECT NOW() AS _ts,", 1) for q in queries]
    
    def execute_query(self, query):
        """Execute single query"""
        conn = sql.connect(**self.warmup_config)
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            cursor.fetchall()
        finally:
            cursor.close()
            conn.close()
    
    def warm_cache(self, queries, passes=5, concurrency=10, delay=5):
        """Execute warm-up and return durations"""
        durations = []
        
        for pass_num in range(1, passes + 1):
            print(f"Pass {pass_num}/{passes}...")
            start = time.time()
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
                list(executor.map(self.execute_query, queries))
            
            duration = round(time.time() - start, 2)
            durations.append(duration)
            print(f"  Completed in {duration}s")
            
            if pass_num < passes:
                time.sleep(delay)
        
        return durations

# Usage
warmer = DBSQLCacheWarmer(EXTRACTION_CONFIG, WARMUP_CONFIG)
queries = warmer.extract_queries("prod-warehouse-id", days=7, limit=150)
modified = warmer.modify_for_warmup(queries)
durations = warmer.warm_cache(modified, passes=5, concurrency=10)

print(f"\nWarm-up complete! Ready to benchmark.")
print(f"Steady-state duration: {min(durations)}s")
```

### Calculate Optimal Concurrency

```python
# Concurrency should match cluster count × 10
def calculate_concurrency(warehouse_id):
    from databricks.sdk import WorkspaceClient
    
    w = WorkspaceClient()
    warehouse = w.warehouses.get(id=warehouse_id)
    
    # Get cluster count (min or current)
    clusters = warehouse.num_clusters or 1
    optimal_concurrency = clusters * 10
    
    print(f"Warehouse: {warehouse.name}")
    print(f"Clusters: {clusters}")
    print(f"Recommended concurrency: {optimal_concurrency}")
    
    return optimal_concurrency

# Use in warm-up
concurrency = calculate_concurrency("benchmark-warehouse-id")
durations = warmup_cache(queries, concurrency=concurrency)
```

## FAQ

**Q: Why not just run the benchmark multiple times and use the second run?**  
A: The second run may hit result cache instead of disk cache. This approach ensures disk cache is properly warmed.

**Q: How many warm-up passes do I need?**  
A: Typically 3-5. Look for duration convergence in visualization.

**Q: Can I use this for multi-cluster warehouses?**  
A: Yes! Scale concurrency proportionally: 10 × number_of_clusters.

**Q: Does this work for AI/BI Dashboards?**  
A: Yes! All BI tools benefit from warm disk cache.

**Q: How long does warm-up take?**  
A: Depends on query count and complexity. Typically 5-15 minutes for 100-200 queries.

**Q: Should I restart the warehouse before warming?**  
A: Yes, to ensure you're starting from a known cold state.
