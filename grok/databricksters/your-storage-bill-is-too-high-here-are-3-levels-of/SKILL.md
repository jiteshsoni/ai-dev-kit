---
name: "delta-vacuum-optimization"
description: "Master Delta Lake VACUUM operations for storage cost optimization - FULL, LITE, and USING INVENTORY modes with performance tuning and maintenance strategies."
---

# Delta Lake VACUUM: Storage Cost Optimization

## Overview

This skill covers Delta Lake VACUUM operations for optimizing storage costs and managing data lifecycle in data lakehouses. Learn about the three VACUUM modes (FULL, LITE, USING INVENTORY), their performance characteristics, cost implications, and when to use each approach. Includes practical maintenance strategies, hardware optimization, and automated cleanup pipelines for managing storage costs at scale.

## Quick Start

### Storage Cost Analysis
First, understand your storage footprint and identify optimization opportunities:

```python
def analyze_table_storage_cost(table_path):
    """Analyze storage costs and VACUUM optimization potential"""
    
    # Get table details
    details = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
    total_size_gb = details['sizeInBytes'] / (1024**3)
    
    # Analyze file distribution
    file_analysis = spark.sql(f"""
        SELECT 
            COUNT(*) as total_files,
            AVG(size) as avg_file_size_mb,
            COUNT(DISTINCT date) as days_with_data,
            SUM(size) / (1024*1024*1024) as total_size_gb
        FROM (
            SELECT 
                size / (1024*1024) as size,
                date(from_unixtime(modificationTime/1000)) as date
            FROM delta.`{table_path}`._files
        )
    """).collect()[0]
    
    # Estimate VACUUM potential
    # Files older than retention period can be removed
    retention_days = 7  # Default VACUUM retention
    cutoff_date = current_date() - retention_days
    
    removable_files = spark.sql(f"""
        SELECT COUNT(*) as removable_count, 
               SUM(size)/(1024*1024*1024) as removable_gb
        FROM delta.`{table_path}`._files
        WHERE date(from_unixtime(modificationTime/1000)) < '{cutoff_date}'
    """).collect()[0]
    
    analysis = {
        'table_size_gb': total_size_gb,
        'total_files': file_analysis['total_files'],
        'avg_file_size_mb': file_analysis['avg_file_size_mb'],
        'removable_files': removable_files['removable_count'],
        'removable_gb': removable_files['removable_gb'],
        'potential_savings_pct': (removable_files['removable_gb'] / total_size_gb) * 100 if total_size_gb > 0 else 0
    }
    
    print(f"Table Analysis for {table_path}:")
    print(f"  Total Size: {analysis['table_size_gb']:.2f} GB")
    print(f"  Total Files: {analysis['total_files']:,}")
    print(f"  Potential VACUUM Savings: {analysis['potential_savings_pct']:.1f}%")
    
    return analysis

# Usage
analysis = analyze_table_storage_cost("/path/to/your/table")
```

### Choose Optimal VACUUM Strategy
Select the right VACUUM approach based on your requirements:

```python
def recommend_vacuum_strategy(table_analysis, requirements):
    """
    Recommend VACUUM strategy based on table characteristics and requirements
    
    Args:
        table_analysis: Result from analyze_table_storage_cost()
        requirements: Dict with keys like 'frequency', 'thoroughness', 'compliance_critical'
    """
    
    strategies = {
        'vacuum_lite': {
            'speed': 'fast',
            'cost': 'low',
            'thoroughness': 'moderate',
            'frequency': 'daily/weekly',
            'use_case': 'Regular maintenance, well-managed tables',
            'command': 'VACUUM table_name LITE RETAIN 168 HOURS'
        },
        'vacuum_full': {
            'speed': 'slow',
            'cost': 'medium',
            'thoroughness': 'high',
            'frequency': 'weekly/monthly',
            'use_case': 'Deep cleanup, compliance, establishing baselines',
            'command': 'VACUUM table_name RETAIN 168 HOURS'
        },
        'vacuum_inventory': {
            'speed': 'fastest',
            'cost': 'low',
            'thoroughness': 'high',
            'frequency': 'weekly',
            'use_case': 'Extreme scale, dedicated platform teams',
            'command': 'VACUUM table_name USING INVENTORY RETAIN 168 HOURS'
        }
    }
    
    # Decision logic
    if requirements.get('thoroughness') == 'maximum' or requirements.get('compliance_critical'):
        return strategies['vacuum_full']
    
    if table_analysis['total_files'] > 1000000:  # Million+ files
        if requirements.get('platform_team_available'):
            return strategies['vacuum_inventory']
        else:
            return strategies['vacuum_full']
    
    if requirements.get('frequency') == 'daily':
        return strategies['vacuum_lite']
    
    # Default: hybrid approach
    return {
        'primary': strategies['vacuum_lite'],
        'secondary': strategies['vacuum_full'],
        'schedule': 'LITE daily, FULL weekly'
    }

# Usage
requirements = {'frequency': 'daily', 'compliance_critical': False}
strategy = recommend_vacuum_strategy(analysis, requirements)
print(f"Recommended: {strategy}")
```

## Common Patterns

### Pattern 1: Automated Maintenance Pipeline
Implement scheduled VACUUM operations with monitoring:

```python
from databricks.sdk import WorkspaceClient
from datetime import datetime, timedelta
import json

def create_vacuum_maintenance_job(table_name, strategy='lite', schedule='daily'):
    """
    Create automated VACUUM maintenance job
    """
    
    w = WorkspaceClient()
    
    # Determine VACUUM command
    if strategy == 'lite':
        vacuum_cmd = f"VACUUM {table_name} LITE RETAIN 168 HOURS"
        timeout_minutes = 30
    elif strategy == 'full':
        vacuum_cmd = f"VACUUM {table_name} RETAIN 168 HOURS"
        timeout_minutes = 120
    else:
        vacuum_cmd = f"VACUUM {table_name} USING INVENTORY RETAIN 168 HOURS"
        timeout_minutes = 60
    
    # Job configuration
    job_config = {
        "name": f"vacuum_{table_name}_{strategy}_{datetime.now().strftime('%Y%m%d')}",
        "tasks": [
            {
                "task_key": "analyze_before",
                "sql_task": {
                    "query": {
                        "query_text": f"""
                            -- Analyze table before VACUUM
                            SELECT 
                                COUNT(*) as files_before,
                                SUM(size)/(1024*1024*1024) as size_gb_before
                            FROM delta.`{table_name}`._files
                        """
                    }
                }
            },
            {
                "task_key": "run_vacuum",
                "sql_task": {
                    "query": {
                        "query_text": vacuum_cmd
                    }
                },
                "timeout_seconds": timeout_minutes * 60
            },
            {
                "task_key": "analyze_after",
                "sql_task": {
                    "query": {
                        "query_text": f"""
                            -- Analyze table after VACUUM
                            SELECT 
                                COUNT(*) as files_after,
                                SUM(size)/(1024*1024*1024) as size_gb_after
                            FROM delta.`{table_name}`._files
                        """
                    }
                }
            },
            {
                "task_key": "log_cleanup",
                "sql_task": {
                    "query": {
                        "query_text": f"""
                            -- Log the cleanup results
                            INSERT INTO audit.vacuum_log
                            SELECT 
                                '{table_name}' as table_name,
                                '{strategy}' as vacuum_type,
                                files_before,
                                files_after,
                                size_gb_before,
                                size_gb_after,
                                (files_before - files_after) as files_removed,
                                (size_gb_before - size_gb_after) as gb_saved,
                                current_timestamp() as execution_time
                            FROM (
                                -- This would be populated from previous task results
                                SELECT 
                                    CAST(1000 as bigint) as files_before, -- Placeholder
                                    CAST(800 as bigint) as files_after,   -- Placeholder
                                    CAST(10.5 as double) as size_gb_before, -- Placeholder
                                    CAST(8.2 as double) as size_gb_after    -- Placeholder
                            )
                        """
                    }
                }
            }
        ],
        "timeout_seconds": (timeout_minutes + 10) * 60,  # Extra buffer
        "max_concurrent_runs": 1,  # Prevent overlapping runs
        "parameters": [
            {
                "name": "table_name",
                "default": table_name
            },
            {
                "name": "vacuum_strategy", 
                "default": strategy
            }
        ]
    }
    
    # Add schedule based on frequency
    if schedule == 'daily':
        job_config["schedule"] = {
            "quartz_cron_expression": "0 2 * * *",  # Daily at 2 AM
            "timezone_id": "UTC"
        }
    elif schedule == 'weekly':
        job_config["schedule"] = {
            "quartz_cron_expression": "0 2 * * 0",  # Weekly on Sunday at 2 AM
            "timezone_id": "UTC"
        }
    
    # Create the job
    job = w.jobs.create(**job_config)
    
    print(f"Created VACUUM job: {job.job_id}")
    print(f"Command: {vacuum_cmd}")
    print(f"Schedule: {schedule}")
    
    return job

# Usage
job = create_vacuum_maintenance_job(
    table_name="analytics.events",
    strategy="lite",
    schedule="daily"
)
```

### Pattern 2: Cost-Optimized Hardware Configuration
Configure clusters for efficient VACUUM operations:

```python
def create_vacuum_cluster_config(strategy, table_size_gb):
    """
    Create cost-optimized cluster configuration for VACUUM operations
    """
    
    if strategy == 'lite':
        # Single-node for cost efficiency
        if table_size_gb < 100:
            return {
                "cluster_name": "vacuum-lite-small",
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "c5.4xlarge",  # 16 cores, cost-effective
                "driver_node_type_id": "c5.4xlarge",
                "num_workers": 0,  # Single-node
                "autoscale": {
                    "min_workers": 0,
                    "max_workers": 0
                },
                "spark_conf": {
                    "spark.databricks.delta.vacuum.parallelDelete.enabled": "true",
                    "spark.sql.adaptive.enabled": "true"
                }
            }
        else:
            return {
                "cluster_name": "vacuum-lite-large",
                "spark_version": "15.4.x-scala2.12", 
                "node_type_id": "c5.9xlarge",  # 36 cores for larger tables
                "driver_node_type_id": "c5.9xlarge",
                "num_workers": 0,
                "spark_conf": {
                    "spark.databricks.delta.vacuum.parallelDelete.enabled": "true",
                    "spark.sql.files.maxPartitionBytes": "268435456"  # 256MB
                }
            }
    
    elif strategy == 'full':
        # Multi-node for thorough cleaning
        return {
            "cluster_name": "vacuum-full-cluster",
            "spark_version": "15.4.x-scala2.12",
            "node_type_id": "c5.4xlarge",
            "driver_node_type_id": "c5.9xlarge",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 4
            },
            "spark_conf": {
                "spark.databricks.delta.vacuum.parallelDelete.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "false",
                "spark.sql.files.maxPartitionBytes": "134217728"  # 128MB
            }
        }
    
    elif strategy == 'inventory':
        # Optimized for inventory-based operations
        return {
            "cluster_name": "vacuum-inventory-cluster",
            "spark_version": "15.4.x-scala2.12",
            "node_type_id": "c5.2xlarge",
            "driver_node_type_id": "c5.4xlarge", 
            "num_workers": 2,
            "spark_conf": {
                "spark.databricks.delta.vacuum.inventory.parallelism": "10",
                "spark.databricks.delta.vacuum.parallelDelete.enabled": "true"
            }
        }

# Usage
config = create_vacuum_cluster_config('lite', table_size_gb=50)
print(f"Recommended cluster config: {config['cluster_name']}")
```

### Pattern 3: Compliance-Aware VACUUM Strategy
Implement VACUUM strategies that meet compliance requirements:

```python
def create_compliance_vacuum_strategy(table_name, retention_requirements):
    """
    Create VACUUM strategy that meets compliance requirements
    """
    
    # Define retention policies based on compliance needs
    compliance_profiles = {
        'gdpr': {
            'retention_hours': 24 * 30,  # 30 days
            'vacuum_frequency': 'daily',
            'audit_trail': True,
            'strategy': 'full'  # Thorough cleanup required
        },
        'ccpa': {
            'retention_hours': 24 * 45,  # 45 days
            'vacuum_frequency': 'daily', 
            'audit_trail': True,
            'strategy': 'full'
        },
        'financial': {
            'retention_hours': 24 * 365 * 7,  # 7 years
            'vacuum_frequency': 'monthly',
            'audit_trail': True,
            'strategy': 'full'
        },
        'standard': {
            'retention_hours': 24 * 7,  # 7 days
            'vacuum_frequency': 'weekly',
            'audit_trail': False,
            'strategy': 'lite'
        }
    }
    
    # Select appropriate profile
    profile_name = retention_requirements.get('compliance_profile', 'standard')
    profile = compliance_profiles[profile_name]
    
    # Create audit trail table if required
    if profile['audit_trail']:
        spark.sql("""
            CREATE TABLE IF NOT EXISTS audit.vacuum_operations (
                table_name STRING,
                vacuum_type STRING,
                retention_hours INT,
                files_before BIGINT,
                files_after BIGINT,
                size_gb_before DOUBLE,
                size_gb_after DOUBLE,
                execution_timestamp TIMESTAMP,
                compliance_profile STRING
            )
            USING DELTA
        """)
    
    # Generate VACUUM commands
    vacuum_commands = []
    
    if profile['strategy'] == 'full':
        vacuum_commands.append({
            'command': f'VACUUM {table_name} RETAIN {profile["retention_hours"]} HOURS',
            'frequency': profile['vacuum_frequency'],
            'description': f'Compliance-required full VACUUM ({profile_name})'
        })
    else:
        # Hybrid approach for standard cases
        vacuum_commands.extend([
            {
                'command': f'VACUUM {table_name} LITE RETAIN {profile["retention_hours"]} HOURS',
                'frequency': 'daily',
                'description': 'Daily maintenance VACUUM'
            },
            {
                'command': f'VACUUM {table_name} RETAIN {profile["retention_hours"]} HOURS', 
                'frequency': profile['vacuum_frequency'],
                'description': 'Weekly thorough cleanup'
            }
        ])
    
    return {
        'profile': profile_name,
        'retention_hours': profile['retention_hours'],
        'vacuum_commands': vacuum_commands,
        'audit_required': profile['audit_trail']
    }

# Usage
compliance_strategy = create_compliance_vacuum_strategy(
    "user_data.events", 
    {"compliance_profile": "gdpr"}
)

print(f"Compliance Profile: {compliance_strategy['profile']}")
for cmd in compliance_strategy['vacuum_commands']:
    print(f"  {cmd['frequency']}: {cmd['command']}")
```

## Reference Files

- [Delta Lake VACUUM Documentation](https://docs.databricks.com/en/delta/delta-utility.html#remove-unused-data-files-with-vacuum) - Official VACUUM guide
- [VACUUM LITE](https://docs.databricks.com/en/delta/delta-utility.html#vacuum-lite) - Fast maintenance mode
- [VACUUM USING INVENTORY](https://docs.databricks.com/en/delta/delta-utility.html#vacuum-using-inventory) - Inventory-based cleanup

## Common Issues

| Issue | Solution |
|-------|----------|
| **DELTA_CANNOT_VACUUM_LITE error** | Run VACUUM FULL first to establish baseline |
| **VACUUM taking too long** | Use LITE mode for daily maintenance, FULL for weekly deep cleans |
| **High API costs** | Switch to LITE mode to avoid expensive file listing operations |
| **Straggler files not cleaned** | Use FULL mode periodically to catch uncommitted files |
| **Compliance violations** | Set appropriate retention periods, use FULL mode for compliance-critical tables |

## Key Takeaways

1. **Three VACUUM Modes** - LITE for speed (daily), FULL for thoroughness (weekly), INVENTORY for scale
2. **Storage Cost Optimization** - Can reduce storage costs by 50-80% on tables with frequent updates
3. **Time Travel Preservation** - VACUUM only removes files older than retention period (default 7 days)
4. **Hardware Matters** - Single-node clusters often more cost-effective than multi-node for VACUUM
5. **Compliance First** - Use FULL mode for regulated data; thoroughness over performance
6. **Hybrid Approach** - Combine LITE (daily) + FULL (weekly) for optimal cost/performance

## Performance Benchmarking

### Compare VACUUM Modes
```python
def benchmark_vacuum_modes(table_name, modes=['lite', 'full']):
    """Benchmark different VACUUM modes on the same table"""
    
    results = {}
    
    for mode in modes:
        # Analyze before
        before = spark.sql(f"""
            SELECT COUNT(*) as files, SUM(size)/(1024*1024*1024) as size_gb
            FROM delta.`{table_name}`._files
        """).collect()[0]
        
        # Execute VACUUM
        start_time = time.time()
        
        if mode == 'lite':
            spark.sql(f"VACUUM {table_name} LITE RETAIN 168 HOURS")
        elif mode == 'full':
            spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS")
        
        execution_time = time.time() - start_time
        
        # Analyze after
        after = spark.sql(f"""
            SELECT COUNT(*) as files, SUM(size)/(1024*1024*1024) as size_gb
            FROM delta.`{table_name}`._files
        """).collect()[0]
        
        results[mode] = {
            'execution_time_seconds': execution_time,
            'files_before': before['files'],
            'files_after': after['files'],
            'size_gb_before': before['size_gb'],
            'size_gb_after': after['size_gb'],
            'files_removed': before['files'] - after['files'],
            'gb_saved': before['size_gb'] - after['size_gb']
        }
    
    # Print comparison
    print("VACUUM Mode Comparison:")
    print("-" * 50)
    for mode, metrics in results.items():
        print(f"{mode.upper()} Mode:")
        print(f"  Execution Time: {metrics['execution_time_seconds']:.1f}s")
        print(f"  Files Removed: {metrics['files_removed']:,}")
        print(f"  GB Saved: {metrics['gb_saved']:.2f}")
        print()
    
    return results

# Usage (run on a test table first!)
# results = benchmark_vacuum_modes("test.vacuum_comparison")
```

## Cost Analysis

### Storage Cost Savings Calculator
```python
def calculate_vacuum_roi(table_name, vacuum_results, storage_cost_per_gb_month=0.023):
    """
    Calculate ROI of VACUUM operations
    
    Args:
        storage_cost_per_gb_month: Cost per GB per month (varies by cloud provider)
    """
    
    gb_saved = vacuum_results.get('gb_saved', 0)
    execution_time_hours = vacuum_results.get('execution_time_seconds', 0) / 3600
    
    # Monthly savings
    monthly_savings = gb_saved * storage_cost_per_gb_month
    
    # Execution cost (rough estimate based on cluster cost)
    dbu_cost_per_hour = 0.07  # Approximate
    execution_cost = execution_time_hours * 20 * dbu_cost_per_hour  # 20 DBUs for VACUUM
    
    # Net savings
    net_monthly_savings = monthly_savings - execution_cost
    
    # ROI calculation
    if execution_cost > 0:
        roi_pct = (net_monthly_savings / execution_cost) * 100
    else:
        roi_pct = float('inf')
    
    return {
        'gb_saved': gb_saved,
        'monthly_storage_savings': monthly_savings,
        'execution_cost': execution_cost,
        'net_monthly_savings': net_monthly_savings,
        'roi_percentage': roi_pct,
        'break_even_months': execution_cost / monthly_savings if monthly_savings > 0 else float('inf')
    }

# Usage
vacuum_results = {'gb_saved': 100, 'execution_time_seconds': 1800}  # 30 minutes
roi = calculate_vacuum_roi("analytics.events", vacuum_results)
print(f"Monthly Savings: ${roi['net_monthly_savings']:.2f}")
print(f"ROI: {roi['roi_percentage']:.1f}%")
```

## Automated Maintenance Dashboard

### Create VACUUM Monitoring Dashboard
```python
def create_vacuum_monitoring_dashboard():
    """Create a dashboard to monitor VACUUM operations across tables"""
    
    dashboard_queries = {
        "vacuum_effectiveness": """
        SELECT 
            table_name,
            vacuum_type,
            execution_timestamp,
            files_removed,
            gb_saved,
            execution_time_seconds,
            gb_saved / execution_time_seconds as efficiency_gb_per_second
        FROM audit.vacuum_log
        WHERE execution_timestamp >= CURRENT_DATE() - INTERVAL 30 DAYS
        ORDER BY execution_timestamp DESC
        """,
        
        "storage_trends": """
        SELECT 
            DATE(execution_timestamp) as date,
            SUM(gb_saved) as daily_gb_saved,
            AVG(execution_time_seconds) as avg_execution_time
        FROM audit.vacuum_log
        WHERE execution_timestamp >= CURRENT_DATE() - INTERVAL 30 DAYS
        GROUP BY DATE(execution_timestamp)
        ORDER BY date
        """,
        
        "cost_savings": """
        SELECT 
            table_name,
            SUM(gb_saved) as total_gb_saved,
            SUM(gb_saved) * 0.023 as monthly_cost_savings,  -- $0.023/GB/month
            COUNT(*) as vacuum_runs
        FROM audit.vacuum_log
        WHERE execution_timestamp >= CURRENT_DATE() - INTERVAL 30 DAYS
        GROUP BY table_name
        ORDER BY total_gb_saved DESC
        """
    }
    
    # This would integrate with Databricks SQL dashboards
    return {
        "dashboard_name": "VACUUM Operations Monitor",
        "queries": dashboard_queries,
        "refresh_schedule": "daily"
    }

# Usage
dashboard = create_vacuum_monitoring_dashboard()
```

## When to Use This Skill

- Managing storage costs in Delta Lake tables
- Implementing data lifecycle management policies
- Optimizing cloud storage expenses for data lakes
- Ensuring compliance with data retention requirements
- Building automated maintenance pipelines
- Troubleshooting storage cost overruns

## Best Practices Summary

| Aspect | Recommendation |
|--------|----------------|
| **Frequency** | LITE daily/weekly, FULL weekly/monthly |
| **Hardware** | Single-node for LITE, multi-node for FULL |
| **Monitoring** | Track file counts, sizes, and execution times |
| **Compliance** | Use FULL mode for regulated data |
| **Cost Optimization** | LITE for routine cleanup, strategic FULL runs |
| **Automation** | Schedule jobs with Databricks Workflows |

## Related Skills

- delta-lake-optimization
- data-lifecycle-management
- cloud-storage-cost-optimization
- automated-maintenance-pipelines
- compliance-data-management