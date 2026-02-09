---
name: file-read-optimization
description: "Optimize file reads with maxPartitionBytes and other configurations."
tags: ["performance", "file-reads", "spark", "optimization"]
---

# File Read Optimization

## Overview

Optimize Spark file reads for better performance and resource utilization.

## Quick Start

```python
# Control partition size for reads
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB

# For high-performance reads
spark.conf.set("spark.sql.files.openCostInBytes", "4194304")  # 4MB

# For many small files
spark.conf.set("spark.sql.files.minPartitionBytes", "67108864")  # 64MB
```

## Common Issues

| Issue | Solution |
|-------|----------|
| Many small files | Increase minPartitionBytes |
| Large partition imbalance | Adjust maxPartitionBytes |
| Slow file listing | Use manifest files |
| High memory usage | Reduce maxPartitionBytes |
