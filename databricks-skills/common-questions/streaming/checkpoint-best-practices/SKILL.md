---
name: "checkpoint-location-best-practices"
description: "Configure and manage checkpoint locations for reliable streaming."
question: "Where should I store my checkpoint location and how do I manage it?"
answer: |
  ## Checkpoint Storage
  
  **DO**:
  - Use Unity Catalog volumes (S3/ADLS-backed)
  - Use target-tied organization
  - Ensure unique checkpoint per stream
  
  **DON'T**:
  - Use DBFS (ephemeral, workspace-local)
  - Share checkpoints between streams
  - Store in temporary locations
  
  ## Best Practice Pattern
  
  ```python
  def get_checkpoint_location(table_name):
      """Checkpoint tied to target table"""
      return f"/Volumes/catalog/checkpoints/{table_name}"
  
  # Example:
  # Table: prod.analytics.orders
  # Checkpoint: /Volumes/prod/checkpoints/orders
  ```
  
  ## Why Target-Tied?
  
  - Checkpoint already contains source information
  - Systematic organization
  - Easy backup and restore
  - Clear ownership
  
  ## Recovery Scenarios
  
  **Lost checkpoint**:
  1. Delete checkpoint folder
  2. Restart stream with startingOffsets=earliest
  3. Stream reprocesses from beginning
  4. Delta sink handles deduplication
  
  **Corrupted checkpoint**:
  - Same as lost checkpoint
  - Or restore from backup if available
  
  ## Monitoring
  
  - Track checkpoint folder size
  - Alert on checkpoint access failures
  - Monitor state store growth (stateful jobs)
tags: ["checkpoint", "streaming", "best-practices", "storage"]
related_links:
  - skills/blogs/canadiandataguy/mastering-checkpoints-in-spark-streaming/
  - skills/spark-structured-streaming/
---

# Checkpoint Location Best Practices FAQ