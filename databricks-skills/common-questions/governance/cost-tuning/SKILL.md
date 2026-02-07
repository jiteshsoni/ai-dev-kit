---
name: "cost-tuning-streaming"
description: "Optimize costs for Spark Streaming workloads on Databricks."
question: "How do I reduce costs for my streaming jobs?"
answer: |
  ## Cost Optimization Strategies
  
  ### 1. Trigger Interval Tuning
  
  ```python
  # Shorter interval = higher cost
  .trigger(processingTime="5 seconds")   # Expensive
  
  # Longer interval = lower cost
  .trigger(processingTime="5 minutes")   # Cheaper
  
  # Use availableNow for batch-style
  .trigger(availableNow=True)            # Cheapest
  ```
  
  ### 2. Cluster Right-Sizing
  
  ```python
  # Don't oversize:
  # - Monitor CPU utilization (target 60-80%)
  # - Check for idle time
  # - Use fixed-size clusters (no autoscaling for streaming)
  
  # Scale test:
  # - Start small
  # - Monitor lag
  # - Scale up if falling behind
  ```
  
  ### 3. Multi-Stream Clusters
  
  ```python
  # Run multiple streams on one cluster
  # Tested: 100 streams on 8-core single-node
  # Cost: ~$20/day for 100 tables
  ```
  
  ### 4. Storage Optimization
  
  ```sql
  -- VACUUM old files
  VACUUM table RETAIN 24 HOURS;
  
  -- Use compression
  ALTER TABLE table SET TBLPROPERTIES (
    'delta.checkpointInterval' = '100'
  );
  ```
  
  ### 5. Scheduled vs Continuous
  
  | Pattern | Cost | Use Case |
  |---------|------|----------|
  | Continuous | $$$ | Real-time requirements |
  | 15-min schedule | $$ | Near real-time |
  | 4-hour schedule | $ | Batch-style SLA |
  
  ## Cost Monitoring
  
  Track per-stream costs:
  ```python
  # Tag jobs with stream name
  # Use DBU consumption metrics
  # Monitor by workspace/cluster
  ```
  
  ## Cost Formula
  
  ```
  Daily Cost = 
    (Cluster DBU/hour × Hours running) +
    (Storage GB × Storage rate) +
    (Network egress if applicable)
  
  Optimization levers:
  - Reduce hours running (scheduled triggers)
  - Reduce cluster size (right-sizing)
  - Reduce storage (VACUUM, compression)
  ```
  
  ## Quick Wins
  
  1. Change from continuous to 15-minute schedule
  2. Run multiple streams per cluster
  3. Enable auto-optimize to reduce storage
  4. Use Spot instances for non-critical streams
  5. Archive old data to cheaper storage
  
  ## Trade-offs
  
  | Cost Reduction | Impact |
  |----------------|--------|
  | Longer trigger | Higher latency |
  | Smaller cluster | May fall behind |
  | Aggressive VACUUM | Less time travel |
  | Spot instances | Possible interruptions |
tags: ["cost", "optimization", "streaming", "governance"]
related_links:
  - skills/blogs/canadiandataguy/scaling-spark-streaming-jobs/
  - skills/blogs/canadiandataguy/futureproof-your-data-engineering-skills/
---

# Cost Tuning for Streaming FAQ