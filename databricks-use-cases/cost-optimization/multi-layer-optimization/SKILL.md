---
name: cost-optimization-suite
description: Comprehensive cost optimization strategies for Databricks covering storage (VACUUM), compute (SQL warehouse tuning), and GenAI (token rate limiting). Multi-layer approach to reduce costs across the platform.
author: Canadian Data Guy, Artem Chebotko
type: use-case-collection
source_blogs:
  - title: "Your Storage Bill Is Too High. Here Are 3 Levels of VACUUM to Fix It"
    url: https://www.databricksters.com/p/your-storage-bill-is-too-high-here
    author: Canadian Data Guy
  - title: "10 Lessons from Analyzing and Tuning Two Dozen Databricks SQL Warehouses"
    url: https://www.databricksters.com/p/10-lessons-from-analyzing-and-tuning
    author: Artem Chebotko
  - title: "Getting Medieval on Token Costs"
    url: https://www.databricksters.com/p/getting-medieval-on-token-costs
    author: Anonymous
---

# Databricks Cost Optimization Suite

## Overview

This use case provides a comprehensive, multi-layer approach to reducing costs across your Databricks deployment. By addressing storage, compute, and GenAI expenses systematically, organizations can achieve significant cost savings without sacrificing performance.

**Business Value:**
- Reduce storage costs by 50-80% through proper VACUUM strategies
- Decrease SQL warehouse costs by 20-40% via tuning
- Control GenAI token spend with rate limiting
- Proactive monitoring and governance

**Optimization Layers:**

```
┌─────────────────────────────────────────────────────────────────┐
│                    Cost Optimization Stack                       │
├─────────────────────────────────────────────────────────────────┤
│  Layer 3: Application Costs                                      │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ • GenAI Token Rate Limiting                                 ││
│  │ • Query Optimization                                        ││
│  │ • Workload Consolidation                                    ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                  │
│  Layer 2: Compute Optimization                                   │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ • SQL Warehouse Auto-Stop Tuning                            ││
│  │ • Cluster Right-Sizing                                      ││
│  │ • Spot Instance Usage                                       ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                  │
│  Layer 1: Storage Optimization                                   │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ • VACUUM Strategies (FULL/LITE/INVENTORY)                   ││
│  │ • Time Travel Retention                                     ││
│  │ • Predictive Optimization                                   ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

## Layer 1: Storage Optimization with VACUUM

Delta Lake's time travel feature preserves historical data, but without cleanup, storage costs can balloon. Implement a tiered VACUUM strategy.

### Understanding the Storage Problem

```sql
-- Check current storage breakdown
SELECT 
    table_name,
    num_files,
    size_in_bytes / 1024 / 1024 / 1024 as size_gb,
    (size_in_bytes / 1024 / 1024 / 1024) * 0.05 as estimated_monthly_cost_usd
FROM (
    DESCRIBE DETAIL your_table
);
```

### Three VACUUM Strategies

| Strategy | When to Use | Frequency | Duration |
|----------|-------------|-----------|----------|
| **VACUUM FULL** | First run, compliance cleanup, error recovery | Weekly/Monthly | 30-60 min |
| **VACUUM LITE** | Routine maintenance | Daily | 2-10 min |
| **VACUUM USING INVENTORY** | Petabyte-scale with platform team | Weekly | Varies |

### Implementation

```sql
-- Step 1: Check Delta Lake version (VACUUM LITE requires 3.3.0+)
SELECT substring(version, 1, 5) as delta_version 
FROM (DESCRIBE HISTORY your_table LIMIT 1);

-- Step 2: Initial FULL VACUUM (establish baseline)
VACUUM your_database.your_table RETAIN 168 HOURS;  -- 7 days

-- Step 3: Schedule daily LITE VACUUM
VACUUM your_database.your_table LITE RETAIN 168 HOURS;
```

### Automated Storage Cleanup

```python
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def optimize_storage_with_vacuum(
    table_name: str,
    retention_hours: int = 168,
    use_lite: bool = True,
    catalog: str = None,
    schema: str = None
):
    """
    Intelligent storage optimization with VACUUM.
    
    Args:
        table_name: Target table name
        retention_hours: Data retention (default 7 days)
        use_lite: Use VACUUM LITE when possible
        catalog: Unity Catalog name
        schema: Schema name
    """
    full_table_name = f"{catalog}.{schema}.{table_name}" if catalog else table_name
    
    # Get current stats
    detail_df = spark.sql(f"DESCRIBE DETAIL {full_table_name}")
    stats = detail_df.select("num_files", "size_in_bytes").collect()[0]
    
    initial_files = stats.num_files
    initial_size_gb = stats.size_in_bytes / 1024**3
    
    logger.info(f"Pre-optimization: {initial_files} files, {initial_size_gb:.2f} GB")
    
    # Run VACUUM
    delta_table = DeltaTable.forName(spark, full_table_name)
    
    try:
        if use_lite:
            logger.info("Running VACUUM LITE...")
            delta_table.vacuum(retention_hours, lite=True)
        else:
            logger.info("Running VACUUM FULL...")
            delta_table.vacuum(retention_hours)
        
        # Get post-optimization stats
        detail_df = spark.sql(f"DESCRIBE DETAIL {full_table_name}")
        stats = detail_df.select("num_files", "size_in_bytes").collect()[0]
        
        final_files = stats.num_files
        final_size_gb = stats.size_in_bytes / 1024**3
        
        savings_gb = initial_size_gb - final_size_gb
        savings_pct = (savings_gb / initial_size_gb) * 100 if initial_size_gb > 0 else 0
        
        logger.info(f"Post-optimization: {final_files} files, {final_size_gb:.2f} GB")
        logger.info(f"Savings: {savings_gb:.2f} GB ({savings_pct:.1f}%)")
        
        return {
            "table": full_table_name,
            "initial_size_gb": initial_size_gb,
            "final_size_gb": final_size_gb,
            "savings_gb": savings_gb,
            "savings_pct": savings_pct,
            "files_removed": initial_files - final_files
        }
        
    except Exception as e:
        if "DELTA_CANNOT_VACUUM_LITE" in str(e):
            logger.warning("VACUUM LITE not available, falling back to FULL")
            return optimize_storage_with_vacuum(
                table_name, retention_hours, use_lite=False, 
                catalog=catalog, schema=schema
            )
        raise

# Run optimization
result = optimize_storage_with_vacuum(
    table_name="events",
    catalog="production",
    schema="analytics"
)
```

### Cost-Optimized VACUUM Configuration

```python
# Use single-node cluster for VACUUM LITE (cheaper)
vacuum_config = {
    "spark.databricks.cluster.profile": "singleNode",
    "spark.master": "local[*, 4]",
}

# Notebook cell to run VACUUM with optimized cluster
# 1. Create a new job cluster with:
#    - Single node: true
#    - Driver: c5.4xlarge (compute-optimized)
#    - Workers: 0

# 2. Run VACUUM job
spark.sql("VACUUM analytics.events LITE RETAIN 168 HOURS")
```

## Layer 2: Compute Optimization

### SQL Warehouse Tuning

Implement proven optimizations from production environments:

```sql
-- Lesson 1: Enable Predictive Optimization
ALTER CATALOG your_catalog ENABLE PREDICTIVE OPTIMIZATION;

-- Lesson 2: Apply Liquid Clustering
ALTER TABLE large_table CLUSTER BY (date_column, category);

-- Lesson 3: Collect Statistics
ANALYZE TABLE your_table COMPUTE STATISTICS FOR ALL COLUMNS;
```

### Warehouse Auto-Stop Optimization

```python
import requests
import json

def optimize_warehouse_auto_stop(
    workspace_url: str,
    warehouse_id: str,
    token: str,
    auto_stop_minutes: int = 1
):
    """
    Reduce auto-stop timeout to minimize idle costs.
    
    Default is 10 minutes. Reducing to 1 minute can save
    thousands monthly for bursty workloads.
    """
    url = f"{workspace_url}/api/2.0/sql/warehouses/{warehouse_id}"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "auto_stop_mins": auto_stop_minutes
    }
    
    response = requests.patch(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        print(f"✅ Auto-stop set to {auto_stop_minutes} minutes")
        return True
    else:
        print(f"❌ Failed: {response.text}")
        return False

# Apply optimization
optimize_warehouse_auto_stop(
    workspace_url="https://your-workspace.cloud.databricks.com",
    warehouse_id="123456789",
    token=dbutils.secrets.get("scope", "token"),
    auto_stop_minutes=1  # Aggressive cost savings
)
```

### Detect and Eliminate Disk Spill

```sql
-- Find queries with disk spill (expensive operations)
SELECT 
    query_id,
    query_text,
    disk_spill_bytes / 1024 / 1024 / 1024 as spill_gb,
    duration_ms / 1000 / 60 as duration_minutes,
    warehouse_size
FROM system.query.history
WHERE disk_spill_bytes > 0
    AND start_time > current_timestamp() - INTERVAL 7 DAYS
ORDER BY disk_spill_bytes DESC
LIMIT 20;
```

### Replace INSERT OVERWRITE with MERGE

```python
# ❌ Expensive: Rewrites entire partitions
df.write \
    .mode("overwrite") \
    .insertInto("daily_metrics")

# ✅ Efficient: Only updates changed records
from delta.tables import DeltaTable

target_table = DeltaTable.forName(spark, "daily_metrics")

# Incremental update with MERGE
target_table.alias("target").merge(
    df.alias("source"),
    "target.date = source.date"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

### Consolidate Warehouses

```sql
-- Identify underutilized warehouses for consolidation
SELECT 
    warehouse_name,
    warehouse_size,
    COUNT(DISTINCT DATE(start_time)) as active_days,
    AVG(duration_ms) as avg_query_time_ms,
    SUM(execution_time_ms) / 1000 / 60 / 60 as total_compute_hours,
    -- Estimate monthly cost (simplified)
    CASE warehouse_size
        WHEN '2X-Small' THEN 0.22
        WHEN 'X-Small' THEN 0.44
        WHEN 'Small' THEN 0.88
        WHEN 'Medium' THEN 1.76
        WHEN 'Large' THEN 3.52
        WHEN 'X-Large' THEN 7.04
        WHEN '2X-Large' THEN 14.08
        WHEN '3X-Large' THEN 28.16
        WHEN '4X-Large' THEN 56.32
    END * (SUM(execution_time_ms) / 1000 / 60 / 60) as estimated_cost_usd
FROM system.query.history
WHERE start_time > current_timestamp() - INTERVAL 30 DAYS
GROUP BY warehouse_name, warehouse_size
HAVING COUNT(*) < 100  -- Low usage threshold
ORDER BY estimated_cost_usd DESC;
```

## Layer 3: GenAI Token Rate Limiting

Control runaway token costs with custom rate limiting.

### Token-Limited Gateway Model

```python
import mlflow
import mlflow.pyfunc
import psycopg2
import requests
import pandas as pd
import json
from mlflow.types import DataType, Schema, ColSpec
import mlflow.models

class TokenLimitedGatewayModel(mlflow.pyfunc.PythonModel):
    """
    Rate-limited gateway for foundation model endpoints.
    Prevents runaway token costs per user.
    """
    
    def load_context(self, context):
        """Initialize database connection."""
        self.conn = psycopg2.connect(
            host=os.environ['POSTGRES_HOST'],
            dbname=os.environ['POSTGRES_DBNAME'],
            user=os.environ['POSTGRES_USER'],
            password=os.environ['POSTGRES_PASSWORD'],
            port=int(os.environ.get('POSTGRES_PORT', 5432)),
            sslmode=os.environ.get('POSTGRES_SSLMODE', 'require')
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        
        # FM endpoint configuration
        self.fm_endpoint = os.environ.get('FM_ENDPOINT')
        self.api_token = os.environ.get('DATABRICKS_TOKEN')
        
    def predict(self, context, model_input):
        """Process request with token limit checking."""
        
        # Parse input
        if isinstance(model_input, pd.DataFrame):
            data = model_input.iloc[0].to_dict() if len(model_input) > 0 else {}
        else:
            data = model_input
        
        # Extract parameters
        messages = data.get("messages", [])
        if isinstance(messages, str):
            messages = json.loads(messages)
        
        user_name = str(data.get("user_name", "default_user"))
        model_name = str(data.get("model", "gpt-4"))
        max_tokens = int(data.get("max_tokens", 128))
        temperature = float(data.get("temperature", 0.7))
        
        # Check current token usage
        self.cursor.execute("""
            SELECT COALESCE(SUM(total_tokens), 0) as total_used
            FROM token_usage
            WHERE user_name = %s AND model_name = %s
        """, (user_name, model_name))
        
        result = self.cursor.fetchone()
        tokens_used = int(result[0]) if result else 0
        
        # Check limit
        self.cursor.execute("""
            SELECT token_limit
            FROM user_token_limits
            WHERE user_name = %s AND model_name = %s
        """, (user_name, model_name))
        
        limit_result = self.cursor.fetchone()
        
        if not limit_result:
            return {
                "error": f"No token limit configured for {user_name}/{model_name}",
                "tokens_used": tokens_used
            }
        
        token_limit = int(limit_result[0])
        
        # Check if limit exceeded
        if tokens_used >= token_limit:
            return {
                "error": f"Token limit exceeded. Used: {tokens_used}, Limit: {token_limit}",
                "tokens_used": tokens_used,
                "token_limit": token_limit,
                "suggestion": "Contact admin to increase limit or wait until next billing cycle"
            }
        
        # Call foundation model
        try:
            fm_request = {
                "messages": messages,
                "max_tokens": max_tokens,
                "temperature": temperature
            }
            
            response = requests.post(
                self.fm_endpoint,
                json=fm_request,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.api_token}"
                },
                timeout=30
            )
            response.raise_for_status()
            
            fm_response = response.json()
            
            # Extract token usage
            usage = fm_response.get("usage", {})
            prompt_tokens = int(usage.get("prompt_tokens", 0))
            completion_tokens = int(usage.get("completion_tokens", 0))
            total_tokens = int(usage.get("total_tokens", 0))
            
            # Log usage
            self.cursor.execute("""
                INSERT INTO token_usage 
                (user_name, model_name, prompt_tokens, completion_tokens, total_tokens, 
                 request_timestamp, request_id, response_content)
                VALUES (%s, %s, %s, %s, %s, NOW(), %s, %s)
            """, (
                user_name, model_name, prompt_tokens, completion_tokens, total_tokens,
                fm_response.get("id", ""),
                json.dumps(fm_response)
            ))
            
            # Add usage info to response
            fm_response["usage_info"] = {
                "tokens_used_total": tokens_used + total_tokens,
                "token_limit": token_limit,
                "tokens_remaining": token_limit - (tokens_used + total_tokens)
            }
            
            return fm_response
            
        except requests.exceptions.RequestException as e:
            return {
                "error": f"FM endpoint error: {str(e)}",
                "tokens_used": tokens_used,
                "token_limit": token_limit
            }

# Setup database tables
setup_sql = """
-- Token usage tracking
CREATE TABLE IF NOT EXISTS token_usage (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_name VARCHAR(255) NOT NULL,
    model_name VARCHAR(100) NOT NULL,
    prompt_tokens INTEGER NOT NULL,
    completion_tokens INTEGER NOT NULL,
    total_tokens INTEGER NOT NULL,
    request_timestamp TIMESTAMP NOT NULL,
    request_id VARCHAR(255),
    response_content TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User limits
CREATE TABLE IF NOT EXISTS user_token_limits (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_name VARCHAR(255) NOT NULL UNIQUE,
    model_name VARCHAR(100) NOT NULL DEFAULT 'all',
    token_limit INTEGER NOT NULL DEFAULT 10000,
    period VARCHAR(20) NOT NULL DEFAULT 'monthly',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert default limit
INSERT INTO user_token_limits (user_name, token_limit, period)
VALUES ('default_user', 100000, 'monthly')
ON CONFLICT (user_name) DO NOTHING;
"""
```

### Token Usage Monitoring Dashboard

```sql
-- Daily token usage by user
SELECT 
    DATE(request_timestamp) as date,
    user_name,
    model_name,
    COUNT(*) as request_count,
    SUM(prompt_tokens) as total_prompt_tokens,
    SUM(completion_tokens) as total_completion_tokens,
    SUM(total_tokens) as total_tokens,
    AVG(total_tokens) as avg_tokens_per_request
FROM token_usage
WHERE request_timestamp > CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY DATE(request_timestamp), user_name, model_name
ORDER BY date DESC, total_tokens DESC;

-- Users approaching limits
SELECT 
    tul.user_name,
    tul.model_name,
    tul.token_limit,
    COALESCE(SUM(tu.total_tokens), 0) as tokens_used,
    tul.token_limit - COALESCE(SUM(tu.total_tokens), 0) as tokens_remaining,
    (COALESCE(SUM(tu.total_tokens), 0)::FLOAT / tul.token_limit * 100) as pct_used
FROM user_token_limits tul
LEFT JOIN token_usage tu ON 
    tul.user_name = tu.user_name 
    AND tul.model_name = tu.model_name
    AND tu.request_timestamp > DATE_TRUNC('month', CURRENT_DATE)
GROUP BY tul.user_name, tul.model_name, tul.token_limit
HAVING (COALESCE(SUM(tu.total_tokens), 0)::FLOAT / tul.token_limit) > 0.8
ORDER BY pct_used DESC;
```

## Integrated Cost Monitoring

### Unified Cost Dashboard

```python
def generate_cost_report(catalog: str = "system", days: int = 30):
    """Generate comprehensive cost report across all layers."""
    
    # Storage costs
    storage_df = spark.sql(f"""
        SELECT 
            table_schema,
            table_name,
            size_in_bytes / POWER(1024, 3) as size_gb,
            num_files
        FROM {catalog}.information_schema.tables
        WHERE table_catalog = '{catalog}'
        ORDER BY size_in_bytes DESC
    """)
    
    # Compute costs
    compute_df = spark.sql(f"""
        SELECT 
            warehouse_name,
            warehouse_size,
            COUNT(*) as query_count,
            SUM(execution_time_ms) / 1000 / 60 / 60 as compute_hours
        FROM system.query.history
        WHERE start_time > current_timestamp() - INTERVAL {days} DAYS
        GROUP BY warehouse_name, warehouse_size
    """)
    
    # Token costs (from custom tracking)
    token_df = spark.sql("""
        SELECT 
            user_name,
            model_name,
            SUM(total_tokens) as total_tokens,
            SUM(total_tokens) * 0.00001 as estimated_cost_usd  -- Example rate
        FROM token_usage
        WHERE request_timestamp > CURRENT_DATE - INTERVAL 30 DAYS
        GROUP BY user_name, model_name
    """)
    
    return {
        "storage": storage_df,
        "compute": compute_df,
        "tokens": token_df
    }

# Generate report
report = generate_cost_report()
report["storage"].display()
report["compute"].display()
report["tokens"].display()
```

## Best Practices

### 1. Implement Tiered Storage

```sql
-- Hot data (frequently accessed)
ALTER TABLE hot_data SET TBLPROPERTIES (
    'delta.logRetentionDuration' = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
);

-- Cold data (archival)
ALTER TABLE cold_data SET TBLPROPERTIES (
    'delta.logRetentionDuration' = 'interval 7 days',
    'delta.deletedFileRetentionDuration' = 'interval 1 days'
);
```

### 2. Schedule Optimization Jobs

```python
# Weekly optimization workflow
def weekly_optimization_job():
    """Run comprehensive optimization weekly."""
    
    # 1. Run VACUUM FULL on high-churn tables
    high_churn_tables = ["events", "transactions", "logs"]
    for table in high_churn_tables:
        spark.sql(f"VACUUM {table} RETAIN 168 HOURS")
    
    # 2. Run VACUUM LITE on all other tables
    all_tables = spark.catalog.listTables()
    for table in all_tables:
        if table.name not in high_churn_tables:
            try:
                spark.sql(f"VACUUM {table.name} LITE RETAIN 168 HOURS")
            except Exception as e:
                logger.warning(f"VACUUM failed for {table.name}: {e}")
    
    # 3. Optimize warehouse auto-stop settings
    optimize_warehouse_auto_stop(...)
    
    # 4. Review token usage and adjust limits
    review_token_usage()

# Schedule with Databricks Workflows
```

### 3. Set Up Alerts

```sql
-- Alert on unusual storage growth
CREATE OR REPLACE TEMP VIEW storage_growth AS
SELECT 
    table_name,
    size_in_bytes,
    LAG(size_in_bytes) OVER (PARTITION BY table_name ORDER BY timestamp) as prev_size,
    (size_in_bytes - LAG(size_in_bytes) OVER (PARTITION BY table_name ORDER BY timestamp)) 
        / LAG(size_in_bytes) OVER (PARTITION BY table_name ORDER BY timestamp) * 100 as growth_pct
FROM system.storage.table_statistics
WHERE timestamp > current_timestamp() - INTERVAL 7 DAYS;

-- Alert when growth > 50% in a day
SELECT * FROM storage_growth WHERE growth_pct > 50;
```

## Attribution

This use case synthesizes cost optimization strategies from:

1. **Canadian Data Guy** - VACUUM strategies and storage optimization
2. **Artem Chebotko** - SQL warehouse tuning and compute optimization
3. **Databricks Community** - Token rate limiting patterns

## Expected Savings

| Layer | Optimization | Typical Savings |
|-------|--------------|-----------------|
| Storage | VACUUM + Predictive Optimization | 50-80% |
| Compute | Warehouse tuning + auto-stop | 20-40% |
| GenAI | Token rate limiting | 30-70% |
| **Total** | **Full implementation** | **40-60%** |

## Related Skills

- [databricks-vacuum](../databricks-skills/databricks-vacuum/SKILL.md) - Detailed VACUUM guidance
- [databricks-sql-warehouse-tuning](../databricks-skills/databricks-sql-warehouse-tuning/SKILL.md) - SQL warehouse optimization
- [liquid-clustering-guide](../databricks-skills/liquid-clustering-guide/SKILL.md) - Data layout optimization
