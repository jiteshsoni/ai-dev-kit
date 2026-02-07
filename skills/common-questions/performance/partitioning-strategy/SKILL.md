---
name: "partitioning-strategy-streaming"
description: "Design optimal partitioning for streaming tables."
question: "How should I partition my streaming tables?"
answer: |
  ## Partitioning Strategies
  
  ### Time-Based Partitioning
  
  ```python
  # Most common for streaming
  (df
      .withColumn("date", col("timestamp").cast("date"))
      .writeStream
      .partitionBy("date")
      .start("/delta/table")
  )
  
  # Good for: Time-series queries, retention policies
  ```
  
  ### No Partitioning (Liquid Clustering)
  
  ```python
  # For high-cardinality columns
  # Let Liquid Clustering handle optimization
  
  spark.sql("""
    CREATE TABLE events (
      user_id STRING,
      event_type STRING,
      timestamp TIMESTAMP
    ) USING DELTA
    CLUSTER BY (user_id)
  """)
  ```
  
  ### Low Cardinality Partitioning
  
  ```python
  # For columns with < 1000 distinct values
  # Region, country, status, etc.
  
  .partitionBy("region", "date")
  ```
  
  ## Decision Matrix
  
  | Data Pattern | Strategy | Example |
  |--------------|----------|---------|
  | Time-series | date partitioning | IoT, logs |
  | High cardinality | Liquid Clustering | user_id, device_id |
  | Low cardinality | Partition by value | region, status |
  | Mixed queries | Both | date + Liquid |
  
  ## Anti-Patterns
  
  **DON'T**:
  - Partition by high-cardinality column (UUID, user_id)
  - Create too many small partitions (< 1MB)
  - Partition columns not used in queries
  
  ## File Size Target
  
  ```sql
  -- Aim for 128MB files in partitions
  ALTER TABLE table SET TBLPROPERTIES (
    'delta.targetFileSize' = '134217728'
  );
  ```
  
  ## Query Pattern Alignment
  
  Partition by columns used in WHERE:
  ```sql
  -- Good: Partition by date
  SELECT * FROM table WHERE date = '2024-01-01'
  
  -- Bad: Partition by user_id, filter by date
  -- (scans all partitions)
  ```
  
  ## Streaming-Specific Considerations
  
  1. **Small files**: Streaming creates many small files
     - Solution: Enable auto-optimize or periodic OPTIMIZE
  
  2. **Late arriving data**: May write to old partitions
     - Solution: Liquid clustering handles this better than static partitioning
  
  3. **Checkpoint growth**: More partitions = more metadata
     - Monitor checkpoint size
tags: ["partitioning", "streaming", "performance", "design"]
related_links:
  - https://docs.databricks.com/en/delta/partitioning.html
  - skills/blogs/canadiandataguy/liquid-dv-rlc-streaming-merges/
---

# Partitioning Strategy for Streaming FAQ