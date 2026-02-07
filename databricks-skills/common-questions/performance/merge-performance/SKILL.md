---
name: "merge-performance-tuning"
description: "Optimize Delta MERGE operations for streaming workloads."
question: "How do I improve MERGE performance in my streaming job?"
answer: |
  ## Optimization Strategies
  
  ### 1. Liquid Clustering + DV + RLC
  
  ```sql
  ALTER TABLE target_table SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = true,
    'delta.enableRowLevelConcurrency' = true,
    'delta.liquid.clustering' = true
  );
  ```
  
  Enables concurrent merges without conflicts.
  
  ### 2. File Size Tuning
  
  ```sql
  -- Target file size for optimal merge
  ALTER TABLE target_table 
  SET TBLPROPERTIES (
    'delta.targetFileSize' = '128mb'
  );
  ```
  
  ### 3. Z-Ordering on Match Key
  
  ```sql
  -- Co-locate data by merge key
  OPTIMIZE target_table ZORDER BY (merge_key);
  ```
  
  ### 4. Partition Pruning
  
  ```python
  # Include partition columns in merge condition
  spark.sql("""
    MERGE INTO target t
    USING source s 
    ON t.key = s.key AND t.date = s.date  -- partition column
    ...
  """)
  ```
  
  ### 5. Batch Size Control
  
  ```python
  # Control microbatch size
  (spark.readStream
      .format("kafka")
      .option("maxOffsetsPerTrigger", 10000)
      .load()
  )
  ```
  
  ## Performance Comparison
  
  | Optimization | Impact |
  |--------------|--------|
  | Liquid + DV + RLC | Eliminates conflicts, lower P99 |
  | Z-Ordering | 5-10x faster for targeted lookups |
  | Right file size | Better parallelism |
  | Partition pruning | Skips irrelevant partitions |
  
  ## Monitoring
  
  Check merge performance in Spark UI:
  - Look for broadcast hash joins
  - Check file pruning effectiveness
  - Monitor task skew
tags: ["merge", "performance", "delta", "streaming"]
related_links:
  - skills/blogs/canadiandataguy/liquid-dv-rlc-streaming-merges/
  - https://docs.databricks.com/en/delta/merge.html
---

# Merge Performance Tuning FAQ