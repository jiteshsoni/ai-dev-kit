---
name: "dlt-vs-jobs-comparison"
description: "Choose between Delta Live Tables (DLT) and Databricks Jobs for streaming workloads."
question: "Should I use Delta Live Tables (DLT) or Databricks Jobs for my streaming pipeline?"
answer: |
  ## Delta Live Tables (DLT)
  
  **Best for**:
  - Multi-stage pipelines (bronze → silver → gold)
  - Data quality enforcement
  - Automatic dependency management
  - Built-in monitoring and lineage
  
  **Benefits**:
  - Declarative pipeline definition
  - Automatic orchestration
  - Built-in quality expectations
  - Automatic recovery and retry
  
  **Limitations**:
  - Requires SQL or Python API (not arbitrary code)
  - Less control over execution
  
  ## Databricks Jobs
  
  **Best for**:
  - Custom streaming logic
  - Complex transformations
  - Integration with external systems
  - Fine-grained control
  
  **Benefits**:
  - Full Spark API access
  - Custom error handling
  - Flexible scheduling
  - Direct control over clusters
  
  **Limitations**:
  - Manual dependency management
  - Self-managed quality checks
  
  ## Decision Matrix
  
  | Factor | Use DLT | Use Jobs |
  |--------|---------|----------|
  | Standard medallion architecture | ✓ | |
  | Complex custom logic | | ✓ |
  | Need data quality enforcement | ✓ | |
  | External API calls | | ✓ |
  | Multiple interdependent streams | ✓ | |
  | Fine-grained cost control | | ✓ |
  
  ## Hybrid Approach
  
  Use DLT for standard medallion layers, Jobs for custom
  preprocessing or external integrations.
tags: ["dlt", "jobs", "streaming", "architecture"]
related_links:
  - https://docs.databricks.com/en/dlt/index.html
  - skills/spark-structured-streaming/
---

# DLT vs Jobs Comparison FAQ