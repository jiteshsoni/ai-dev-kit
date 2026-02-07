---
name: "airflow-databricks-workflows"
description: "Orchestrate Databricks workflows using Apache Airflow: DatabricksWorkflowTaskGroup for cluster reuse, task groups, cost optimization, and performance improvements."
---

# Orchestrating Databricks Workflows using Apache Airflow

## Overview

This skill covers orchestrating Databricks workflows using Apache Airflow operators. Learn about DatabricksWorkflowTaskGroup for cluster reuse across multiple tasks, cost optimization through Jobs clusters, performance improvements by avoiding cluster startup overhead, and how to structure complex DAGs with task groups.

## Quick Start

### Basic DatabricksSubmitRunOperator
Submit a one-time job:

```python
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

submit_run = DatabricksSubmitRunOperator(
    task_id='submit_databricks_job',
    databricks_conn_id='databricks_default',
    new_cluster={
        'spark_version': '15.4.x-scala2.12',
        'node_type_id': 'i3.xlarge',
        'num_workers': 2
    },
    notebook_task={
        'notebook_path': '/path/to/notebook',
        'source': 'WORKSPACE'
    }
)
```

### DatabricksWorkflowTaskGroup (Recommended)
Reuse clusters across tasks:

```python
from airflow import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksWorkflowTaskGroup,
    DatabricksNotebookOperator
)
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Define job cluster specifications
job_cluster_spec = [
    {
        "job_cluster_key": "small",
        "new_cluster": {
            "spark_version": "15.4.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 2,
        },
    },
    {
        "job_cluster_key": "large",
        "new_cluster": {
            "spark_version": "15.4.x-scala2.12",
            "node_type_id": "i3.4xlarge",
            "num_workers": 8,
        },
    },
]

# Create DAG
with DAG(
    'databricks_task_group_dag',
    start_date=days_ago(2),
    schedule_interval=None,
) as dag:
    
    # Create task group 1
    task_group_1 = DatabricksWorkflowTaskGroup(
        group_id="workflow_group_1",
        databricks_conn_id='databricks_default',
        job_clusters=job_cluster_spec,
    )
    
    with task_group_1:
        notebook_1 = DatabricksNotebookOperator(
            task_id="notebook_1",
            databricks_conn_id='databricks_default',
            notebook_path="/path/to/notebook1",
            source="WORKSPACE",
            job_cluster_key="small",  # Reference cluster spec
        )
        
        notebook_2 = DatabricksNotebookOperator(
            task_id="notebook_2",
            databricks_conn_id='databricks_default',
            notebook_path="/path/to/notebook2",
            source="WORKSPACE",
            job_cluster_key="small",  # Reuse same cluster
        )
        
        notebook_1 >> notebook_2
    
    # Non-Databricks task
    bash = BashOperator(task_id="bash", bash_command="echo hello")
    
    # Create task group 2
    task_group_2 = DatabricksWorkflowTaskGroup(
        group_id="workflow_group_2",
        databricks_conn_id='databricks_default',
        job_clusters=job_cluster_spec,
    )
    
    with task_group_2:
        notebook_3 = DatabricksNotebookOperator(
            task_id="notebook_3",
            databricks_conn_id='databricks_default',
            notebook_path="/path/to/notebook3",
            source="WORKSPACE",
            job_cluster_key="large",  # Use larger cluster
        )
        
        notebook_4 = DatabricksNotebookOperator(
            task_id="notebook_4",
            databricks_conn_id='databricks_default',
            notebook_path="/path/to/notebook4",
            source="WORKSPACE",
            job_cluster_key="large",  # Reuse large cluster
        )
        
        notebook_3 >> notebook_4
    
    # Combine task groups
    task_group_1 >> bash >> task_group_2
```

## Common Patterns

### Pattern 1: Cost Optimization with Jobs Clusters
Use Jobs clusters instead of All-Purpose:

```python
# OLD WAY: DatabricksSubmitRunOperator (uses All-Purpose clusters)
# Expensive and creates/destroys clusters per task
submit_run = DatabricksSubmitRunOperator(
    task_id='expensive_task',
    new_cluster={...},  # Creates All-Purpose cluster
    # Cluster terminated after task completes
)

# NEW WAY: DatabricksWorkflowTaskGroup (uses Jobs clusters)
# Cost-effective: Jobs clusters are cheaper
# Cluster reused across all tasks in group
task_group = DatabricksWorkflowTaskGroup(
    group_id="cost_optimized_group",
    job_clusters=[
        {
            "job_cluster_key": "shared_cluster",
            "new_cluster": {
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2,
            }
        }
    ]
)

with task_group:
    # All tasks reuse same cluster
    task1 = DatabricksNotebookOperator(..., job_cluster_key="shared_cluster")
    task2 = DatabricksNotebookOperator(..., job_cluster_key="shared_cluster")
    task3 = DatabricksNotebookOperator(..., job_cluster_key="shared_cluster")
```

### Pattern 2: Performance Optimization
Avoid cluster startup overhead:

```python
# Problem: Each task starts/stops cluster
# - Task 1: Start cluster (2 min) → Run (5 min) → Stop (1 min) = 8 min
# - Task 2: Start cluster (2 min) → Run (5 min) → Stop (1 min) = 8 min
# Total: 16 minutes

# Solution: Reuse cluster across tasks
# - Start cluster once (2 min)
# - Task 1: Run (5 min)
# - Task 2: Run (5 min)
# - Stop cluster once (1 min)
# Total: 13 minutes (19% faster)

task_group = DatabricksWorkflowTaskGroup(
    group_id="performance_optimized",
    job_clusters=job_cluster_spec
)

with task_group:
    # Cluster started once at beginning of group
    # Reused for all tasks
    # Stopped once at end of group
    task1 >> task2 >> task3
```

### Pattern 3: Multiple Cluster Sizes
Use different cluster sizes for different workloads:

```python
job_cluster_spec = [
    {
        "job_cluster_key": "small",
        "new_cluster": {
            "spark_version": "15.4.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 2,
        },
    },
    {
        "job_cluster_key": "medium",
        "new_cluster": {
            "spark_version": "15.4.x-scala2.12",
            "node_type_id": "i3.2xlarge",
            "num_workers": 4,
        },
    },
    {
        "job_cluster_key": "large",
        "new_cluster": {
            "spark_version": "15.4.x-scala2.12",
            "node_type_id": "i3.4xlarge",
            "num_workers": 8,
        },
    },
]

task_group = DatabricksWorkflowTaskGroup(
    group_id="multi_cluster_group",
    job_clusters=job_cluster_spec
)

with task_group:
    # Lightweight tasks use small cluster
    light_task = DatabricksNotebookOperator(
        ...,
        job_cluster_key="small"
    )
    
    # Medium tasks use medium cluster
    medium_task = DatabricksNotebookOperator(
        ...,
        job_cluster_key="medium"
    )
    
    # Heavy tasks use large cluster
    heavy_task = DatabricksNotebookOperator(
        ...,
        job_cluster_key="large"
    )
```

### Pattern 4: Existing Cluster Reuse
Reuse existing All-Purpose cluster:

```python
# For DatabricksSubmitRunOperator
submit_run = DatabricksSubmitRunOperator(
    task_id='reuse_cluster',
    existing_cluster_id='1234-567890-cluster123',  # Reuse existing
    notebook_task={...}
)

# Note: DatabricksWorkflowTaskGroup always creates new Jobs clusters
# Use DatabricksSubmitRunOperator with existing_cluster_id for All-Purpose reuse
```

## Reference Files

- [Airflow Databricks Provider](https://airflow.apache.org/docs/apache-airflow-providers-databricks/) - Operator documentation
- [DatabricksWorkflowTaskGroup](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/operators/databricks_workflow_task_group.html) - Task group operator
- [Task Groups](https://airflow.apache.org/docs/apache-airflow/stable/concepts/task-groups.html) - Airflow task groups

## Common Issues

| Issue | Solution |
|-------|----------|
| **High costs** | Use DatabricksWorkflowTaskGroup with Jobs clusters instead of All-Purpose |
| **Slow performance** | Reuse clusters across tasks to avoid startup overhead |
| **Cluster not reused** | Ensure tasks reference same job_cluster_key |
| **Tasks fail** | Verify cluster spec matches task requirements |
| **DAG too complex** | Use multiple task groups to organize workflows |

## Key Takeaways

1. **DatabricksWorkflowTaskGroup**: Reuses clusters across tasks in group
2. **Cost Savings**: Jobs clusters cheaper than All-Purpose clusters
3. **Performance**: Avoid cluster startup overhead by reusing clusters
4. **Cluster Spec**: Define multiple cluster sizes, reference by job_cluster_key
5. **Task Groups**: Organize related Databricks tasks together
6. **Lifecycle**: Cluster starts at group start, stops at group end

## Cost Comparison

```python
def cost_comparison_analysis():
    """Compare costs between approaches"""
    
    # Scenario: 10 tasks, each runs 5 minutes
    
    # Approach 1: DatabricksSubmitRunOperator (All-Purpose clusters)
    # - 10 clusters created/destroyed
    # - All-Purpose cluster cost: $X/hour
    # - Cluster startup: 2 min × 10 = 20 min overhead
    # - Total cost: 10 × (5 min runtime + 2 min startup) × $X/hour
    
    # Approach 2: DatabricksWorkflowTaskGroup (Jobs clusters)
    # - 1 cluster created/destroyed
    # - Jobs cluster cost: $Y/hour (typically 30-50% cheaper)
    # - Cluster startup: 2 min × 1 = 2 min overhead
    # - Total cost: (50 min runtime + 2 min startup) × $Y/hour
    
    savings_percentage = ((10 * 7 * 1) - (52 * 0.7)) / (10 * 7 * 1) * 100
    
    print(f"Estimated cost savings: {savings_percentage:.1f}%")
    print("Plus 18 minutes faster execution!")

# Usage
cost_comparison_analysis()
```

## When to Use This Skill

- Orchestrating multiple Databricks tasks
- Optimizing workflow costs
- Improving workflow performance
- Organizing complex DAGs
- Reusing compute resources

## Related Skills

- airflow-dag-design
- databricks-cluster-management
- workflow-optimization
- cost-optimization-patterns