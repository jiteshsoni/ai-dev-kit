---
name: "unity-catalog-streaming"
description: "Integrate Unity Catalog with Spark Streaming for governance and access control."
question: "How do I use Unity Catalog with Spark Streaming?"
answer: |
  ## Unity Catalog Benefits for Streaming
  
  - **Unified Access Control**: Row/column-level security on streaming tables
  - **Lineage Tracking**: Automatic data lineage for streams
  - **Audit Logging**: Complete audit trail
  - **Cross-Workspace Sharing**: Share streams across workspaces
  
  ## Checkpoint in UC Volumes
  
  ```python
  # Store checkpoint in Unity Catalog volume
  checkpoint_path = "/Volumes/catalog/volume/checkpoints/stream_name"
  
  (df.writeStream
      .option("checkpointLocation", checkpoint_path)
      .start("catalog.schema.table")
  )
  ```
  
  ## Streaming Table Access Control
  
  ```sql
  -- Grant access to streaming table
  GRANT SELECT ON catalog.schema.stream_table TO group analysts;
  
  -- Grant write for streaming job
  GRANT MODIFY ON catalog.schema.target_table TO user streaming_job;
  
  -- Row-level security applies to streaming reads
  ```
  
  ## Managed vs External Tables
  
  | Type | Checkpoint | Data | Use Case |
  |------|------------|------|----------|
  | Managed | UC Volume | UC Storage | Default |
  | External | UC Volume | External | Existing data |
  
  ## Lineage
  
  Unity Catalog automatically captures:
  - Source → Target relationships
  - Column-level lineage
  - Transformation logic
  
  View in Catalog Explorer or query via API.
  
  ## Best Practices
  
  1. Use UC volumes for checkpoints (not DBFS)
  2. Set appropriate permissions on checkpoint volumes
  3. Use service principals for production jobs
  4. Leverage lineage for impact analysis
tags: ["unity-catalog", "governance", "streaming", "security"]
related_links:
  - https://docs.databricks.com/en/data-governance/unity-catalog/index.html
  - skills/common-questions/governance/
---

# Unity Catalog Streaming FAQ