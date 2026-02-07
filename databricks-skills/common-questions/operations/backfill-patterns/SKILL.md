---
name: "streaming-backfill-patterns"
description: "Implement backfill and replay patterns for Spark Streaming."
question: "How do I backfill or replay data in my streaming pipeline?"
answer: |
  ## Backfill Scenarios
  
  ### Scenario 1: Specific Time Range
  
  ```python
  # Backfill from specific Kafka offset
  (spark.readStream
      .format("kafka")
      .option("startingOffsets", """{"topic": {"0": 10000, "1": 10000}}""")
      .option("endingOffsets", """{"topic": {"0": 20000, "1": 20000}}""")
      .load()
  )
  ```
  
  ### Scenario 2: Time-Based Backfill
  
  ```python
  # Backfill Auto Loader from specific date
  (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.startingFiles", "2024-01-01")
      .load("/path/to/files")
  )
  ```
  
  ### Scenario 3: Full Replay
  
  ```python
  # Delete checkpoint and restart from beginning
  # 1. Stop stream
  # 2. Delete checkpoint location
  # 3. Restart with startingOffsets=earliest
  # 4. Delta sink handles deduplication
  ```
  
  ## Backfill Job Pattern
  
  ```python
  # Separate job for backfill
  backfill_df = (spark
      .read  # Note: read (batch), not readStream
      .format("kafka")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()
  )
  
  # Process as batch
  backfill_df.transform(process_logic).write.format("delta").save("target")
  ```
  
  ## Checkpoint Management
  
  ```python
  # Save checkpoint before major changes
  dbutils.fs.cp(
      "/Volumes/cat/vol/checkpoints/stream",
      "/Volumes/cat/vol/checkpoints/stream_backup_20240101"
  )
  
  # Restore if needed
  dbutils.fs.cp(
      "/Volumes/cat/vol/checkpoints/stream_backup_20240101",
      "/Volumes/cat/vol/checkpoints/stream"
  )
  ```
  
  ## Best Practices
  
  1. Use batch reads for one-time backfills (faster)
  2. Keep checkpoint backups before changes
  3. Use Delta time travel to verify backfill results
  4. Monitor cluster capacity (backfills are bursty)
  5. Consider separate cluster for large backfills
  
  ## Idempotency
  
  Always ensure idempotent writes:
  - Use Delta with txnVersion
  - Or use MERGE with idempotent keys
  - Prevents duplicates on reprocessing
tags: ["backfill", "replay", "streaming", "operations"]
related_links:
  - skills/blogs/canadiandataguy/spark-streaming-recovery/
  - skills/spark-structured-streaming/
---

# Streaming Backfill Patterns FAQ