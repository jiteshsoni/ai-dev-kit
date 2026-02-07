---
name: "auto-loader-schema-drift"
description: "Handle schema drift in Auto Loader with schema evolution and rescue options."
question: "How do I handle schema drift when using Auto Loader?"
answer: |
  Auto Loader handles schema drift through several mechanisms:
  
  1. **schemaEvolutionMode**: Choose how to handle new columns
     - 'addNewColumns' (default): Add new columns to schema
     - 'rescue': Put unexpected data in _rescued_data column
     - 'failOnNewColumns': Fail when new columns detected
  
  2. **rescuedDataColumn**: Capture data that doesn't match schema
  
  3. **cloudFiles.schemaHints**: Provide hints for type inference
  
  Example configuration:
  ```python
  (spark
      .readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .option("rescuedDataColumn", "_rescued_data")
      .load("/path/to/data")
  )
  ```
  
  Best practice: Use 'addNewColumns' for evolving schemas, 
  'rescue' for strict schema enforcement with audit trail.
tags: ["auto-loader", "schema", "evolution", "streaming"]
related_links:
  - https://docs.databricks.com/en/ingestion/auto-loader/schema.html
  - skills/blogs/canadiandataguy/spark-streaming-master-class-kafka-to-delta/
---

# Auto Loader Schema Drift FAQ