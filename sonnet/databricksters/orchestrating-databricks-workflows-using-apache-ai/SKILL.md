---
name: "Orchestrate Databricks with Airflow"
description: "Trigger and monitor Databricks jobs from Apache Airflow using DatabricksRunNowOperator and DatabricksSubmitRunOperator."
author: "Databricksters"
url: "https://www.databricksters.com/p/orchestrating-databricks-workflows-using-apache-ai"
date: "2025"
tags: ["airflow", "orchestration", "jobs", "workflows", "databricks"]
---

# Orchestrate Databricks with Airflow

## Overview

Apache Airflow orchestrates complex workflows spanning multiple systems. Integrate Databricks using operators: DatabricksRunNowOperator (existing jobs), DatabricksSubmitRunOperator (ad-hoc jobs), and DatabricksSqlOperator (SQL queries).

## Quick Start

```python
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

# Trigger existing Databricks job
with DAG("databricks_pipeline", schedule_interval="@daily") as dag:
    run_etl = DatabricksRunNowOperator(
        task_id="run_databricks_job",
        databricks_conn_id="databricks_default",
        job_id=12345,
        notebook_params={"date": "{{ ds }}"}
    )
```

## Common Patterns

### Pattern 1: Submit Ad-Hoc Job

```python
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

submit_task = DatabricksSubmitRunOperator(
    task_id="submit_notebook",
    databricks_conn_id="databricks_default",
    notebook_task={
        "notebook_path": "/Users/me/etl_notebook",
        "base_parameters": {"env": "prod"}
    },
    existing_cluster_id="cluster-id"
)
```

### Pattern 2: SQL Query Execution

```python
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

run_sql = DatabricksSqlOperator(
    task_id="run_aggregation",
    databricks_conn_id="databricks_default",
    sql="INSERT INTO agg_table SELECT * FROM raw_table",
    warehouse_id="warehouse-id"
)
```

## FAQ

**Q: How to pass Airflow variables to Databricks?**  
A: Use `notebook_params` or `spark_submit_params` to pass runtime parameters.

**Q: Can Airflow wait for long-running jobs?**  
A: Yes, operators poll job status until completion.
