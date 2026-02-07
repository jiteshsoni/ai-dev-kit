---
name: "spark-maxpartitionbytes-tuning"
description: "Master Spark file reading performance with maxPartitionBytes tuning for small, large, and mixed file sizes - achieve 15x speedups."
---

# Spark File Reads at Warp Speed: maxPartitionBytes Tuning

## Overview

This skill covers optimizing Apache Spark file reading performance through strategic tuning of `spark.sql.files.maxPartitionBytes`. Learn how this critical parameter affects data partitioning, parallelism, and overall query performance across different file size scenarios. Includes practical tuning strategies for small files, large files, and mixed datasets, with performance monitoring techniques and compression considerations for achieving optimal Spark file reading speeds.

## Quick Start

### Analyze Current File Distribution
Understand your data before tuning:

```python
# Analyze file sizes in your data lake
file_stats = spark.sql("""
SELECT 
    COUNT(*) as total_files,
    AVG(size) as avg_file_size_mb,
    MIN(size) as min_file_size_mb,
    MAX(size) as max_file_size_mb,
    STDDEV(size) as file_size_stddev_mb,
    -- File size distribution
    SUM(CASE WHEN size < 2 THEN 1 ELSE 0 END) as small_files_lt_2mb,
    SUM(CASE WHEN size BETWEEN 2 AND 128 THEN 1 ELSE 0 END) as medium_files_2_128mb,
    SUM(CASE WHEN size > 128 THEN 1 ELSE 0 END) as large_files_gt_128mb
FROM (
    SELECT 
        size / (1024*1024) as size,  -- Convert to MB
        path
    FROM (
        SELECT 
            size, 
            input_file_name() as path
        FROM delta.`/path/to/your/table`
        DISTRIBUTE BY input_file_name()
    )
)
""")

file_stats.display()

# Current maxPartitionBytes setting
current_setting = spark.conf.get("spark.sql.files.maxPartitionBytes", "128mb")
print(f"Current maxPartitionBytes: {current_setting}")
```

### Quick Performance Test
Benchmark different maxPartitionBytes values:

```python
def benchmark_maxpartitionbytes(test_values, query):
    """Benchmark different maxPartitionBytes settings"""
    
    results = []
    for max_bytes in test_values:
        # Set configuration
        spark.conf.set("spark.sql.files.maxPartitionBytes", max_bytes)
        
        # Clear cache
        spark.catalog.clearCache()
        
        # Time the query
        start_time = time.time()
        result = spark.sql(query)
        count = result.count()  # Trigger execution
        end_time = time.time()
        
        results.append({
            "maxPartitionBytes": max_bytes,
            "execution_time": end_time - start_time,
            "record_count": count
        })
        
        print(f"{max_bytes}: {end_time - start_time:.2f}s")
    
    return results

# Test different values
test_values = ["1mb", "4mb", "16mb", "32mb", "64mb", "128mb", "256mb"]
query = "SELECT COUNT(*) FROM your_table WHERE date_col >= '2024-01-01'"

results = benchmark_maxpartitionbytes(test_values, query)

# Find optimal setting
optimal = min(results, key=lambda x: x["execution_time"])
print(f"Optimal maxPartitionBytes: {optimal['maxPartitionBytes']}")
```

## Common Patterns

### Pattern 1: Small Files Optimization (< 2MB)
For datasets with many small files, reduce maxPartitionBytes to increase parallelism:

```python
# Configuration for small files
spark.conf.set("spark.sql.files.maxPartitionBytes", "4mb")

# Additional optimizations for small files
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64mb")

# Example: IoT sensor data with many small files
small_files_query = """
SELECT 
    sensor_id,
    AVG(temperature) as avg_temp,
    COUNT(*) as readings
FROM iot_sensor_data 
WHERE date_col >= CURRENT_DATE() - INTERVAL 1 DAY
GROUP BY sensor_id
"""

# This will create many small partitions for high parallelism
result = spark.sql(small_files_query)
print(f"Partitions created: {result.rdd.getNumPartitions()}")

# For streaming jobs with small files
streaming_config = {
    "spark.sql.files.maxPartitionBytes": "1mb",  # Very small for streaming
    "spark.sql.streaming.fileStreamSink.log.cleanupDelay": "1m",
    "spark.sql.streaming.fileStreamSink.log.deletion": "true"
}
```

### Pattern 2: Large Files Optimization (> 1GB)
For large files, use default or slightly higher maxPartitionBytes:

```python
# Configuration for large files
spark.conf.set("spark.sql.files.maxPartitionBytes", "256mb")  # Increase for large files

# Example: Large analytical tables
large_files_query = """
SELECT 
    customer_id,
    SUM(amount) as total_spend,
    COUNT(DISTINCT order_id) as order_count,
    AVG(amount) as avg_order_value
FROM large_orders_table 
WHERE order_date >= '2024-01-01'
GROUP BY customer_id
HAVING total_spend > 1000
ORDER BY total_spend DESC
"""

# Monitor partition distribution
result = spark.sql(large_files_query)
partitions = result.rdd.glom().map(len).collect()
print(f"Partition sizes: {partitions}")
print(f"Average partition size: {sum(partitions) / len(partitions):.0f} records")

# For very large files, consider pre-partitioning
spark.sql("""
CREATE TABLE large_table_partitioned 
USING DELTA
PARTITIONED BY (year, month)
AS SELECT 
    *,
    YEAR(date_col) as year,
    MONTH(date_col) as month
FROM large_unpartitioned_table
""")
```

### Pattern 3: Mixed File Sizes Optimization
For datasets with varying file sizes, find the sweet spot:

```python
def optimize_mixed_filesizes(file_size_distribution):
    """
    Find optimal maxPartitionBytes for mixed file sizes
    """
    small_files_pct = file_size_distribution['small_files_lt_2mb'] / file_size_distribution['total_files']
    large_files_pct = file_size_distribution['large_files_gt_128mb'] / file_size_distribution['total_files']
    
    # Decision logic based on distribution
    if small_files_pct > 0.7:  # Mostly small files
        return "4mb"
    elif large_files_pct > 0.3:  # Significant large files
        return "128mb"
    else:  # Mixed distribution
        return "32mb"  # Start low, tune up

# Analyze your data distribution
file_dist = spark.sql("""
SELECT 
    COUNT(*) as total_files,
    SUM(CASE WHEN size_mb < 2 THEN 1 ELSE 0 END) as small_files_lt_2mb,
    SUM(CASE WHEN size_mb > 128 THEN 1 ELSE 0 END) as large_files_gt_128mb
FROM (
    SELECT size / (1024*1024) as size_mb
    FROM (
        SELECT DISTINCT size, input_file_name() as path
        FROM your_table
    )
)
""").collect()[0]

optimal_setting = optimize_mixed_filesizes(file_dist.asDict())
print(f"Recommended maxPartitionBytes: {optimal_setting}")

# Apply the setting
spark.conf.set("spark.sql.files.maxPartitionBytes", optimal_setting)
```

### Pattern 4: Streaming Jobs Optimization
Special considerations for streaming workloads:

```python
# Configuration for streaming jobs with small files
streaming_optimizations = {
    "spark.sql.files.maxPartitionBytes": "2mb",  # Small for high parallelism
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
    "spark.sql.files.minPartitionNum": "1",
    "spark.sql.files.openCostInBytes": "4194304"  # 4MB
}

# Apply streaming optimizations
for key, value in streaming_optimizations.items():
    spark.conf.set(key, value)

# Example streaming query
streaming_query = spark.readStream \
    .format("delta") \
    .load("/path/to/streaming/table") \
    .groupBy(
        window("timestamp", "1 hour"),
        "sensor_id"
    ) \
    .agg(avg("temperature").alias("avg_temp")) \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .outputMode("complete") \
    .start()

# Monitor streaming performance
streaming_query.recentProgress.foreach(lambda progress: 
    print(f"Processed {progress['numInputRows']} rows in {progress['durationMs']}ms")
)
```

## Reference Files

- [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html#spark-sql) - maxPartitionBytes documentation
- [Adaptive Query Execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution) - AQE tuning
- [File-Based Data Sources](https://spark.apache.org/docs/latest/sql-data-sources.html) - Data source optimization

## Common Issues

| Issue | Solution |
|-------|----------|
| **Too many small partitions** | Increase maxPartitionBytes or enable AQE coalescing |
| **Too few large partitions** | Decrease maxPartitionBytes or increase minPartitionNum |
| **Data skew in partitions** | Use ZORDER or repartitioning to balance data distribution |
| **Gzip compressed files** | Switch to splittable compression (bzip2, lzo) or pre-split files |
| **Streaming lag** | Reduce maxPartitionBytes for higher parallelism in micro-batch processing |
| **Memory pressure** | Monitor executor memory usage and adjust partition sizes accordingly |

## Key Takeaways

1. **File Size Matters** - Small files (<2MB): decrease to 1-16MB; Large files (>1GB): keep default 128MB
2. **Streaming Optimization** - Use 1-4MB for streaming jobs to maximize parallelism
3. **Mixed Datasets** - Start with 32MB and tune based on file distribution analysis
4. **Compression Impact** - Avoid gzip; use bzip2/lzo for splittable compression
5. **Monitoring Required** - Always measure performance impact of changes
6. **AQE Interaction** - Adaptive Query Execution can modify partition counts

## Performance Monitoring

### Partition Analysis
```python
def analyze_partition_distribution(df):
    """Analyze partition sizes and distribution"""
    
    partition_sizes = df.rdd.glom().map(len).collect()
    
    stats = {
        "total_partitions": len(partition_sizes),
        "avg_partition_size": sum(partition_sizes) / len(partition_sizes),
        "max_partition_size": max(partition_sizes),
        "min_partition_size": min(partition_sizes),
        "skew_ratio": max(partition_sizes) / (sum(partition_sizes) / len(partition_sizes))
    }
    
    print("Partition Analysis:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # Warning for skewed distributions
    if stats["skew_ratio"] > 3:
        print("⚠️  High data skew detected! Consider repartitioning or ZORDER.")
    
    return stats

# Usage
result_df = spark.sql("SELECT * FROM your_table WHERE date_col >= '2024-01-01'")
analyze_partition_distribution(result_df)
```

### Query Performance Tracking
```python
def benchmark_query_performance(query, settings_list):
    """Benchmark query performance across different settings"""
    
    baseline_time = None
    results = []
    
    for settings in settings_list:
        # Apply settings
        for key, value in settings.items():
            spark.conf.set(key, value)
        
        # Clear caches
        spark.catalog.clearCache()
        
        # Execute query with timing
        start_time = time.time()
        result = spark.sql(query)
        _ = result.count()  # Trigger execution
        execution_time = time.time() - start_time
        
        # Calculate improvement
        improvement = "baseline" if baseline_time is None else f"{baseline_time/execution_time:.1f}x faster"
        if baseline_time is None:
            baseline_time = execution_time
        
        results.append({
            "settings": settings,
            "execution_time": execution_time,
            "improvement": improvement
        })
        
        print(f"Settings: {settings} -> {execution_time:.2f}s ({improvement})")
    
    return results

# Benchmark different maxPartitionBytes settings
settings_to_test = [
    {"spark.sql.files.maxPartitionBytes": "1mb"},
    {"spark.sql.files.maxPartitionBytes": "4mb"},
    {"spark.sql.files.maxPartitionBytes": "16mb"},
    {"spark.sql.files.maxPartitionBytes": "64mb"},
    {"spark.sql.files.maxPartitionBytes": "128mb"}
]

query = "SELECT COUNT(*), AVG(amount) FROM sales WHERE date >= '2024-01-01'"
performance_results = benchmark_query_performance(query, settings_to_test)
```

## Advanced Tuning Strategies

### Dynamic Partitioning Based on Workload
```python
def dynamic_maxpartitionbytes_tuning(table_path, query_complexity="medium"):
    """
    Dynamically tune maxPartitionBytes based on table characteristics and query type
    """
    
    # Analyze table characteristics
    table_stats = spark.sql(f"""
        DESCRIBE DETAIL delta.`{table_path}`
    """).select("numFiles", "sizeInBytes", "partitionColumns").collect()[0]
    
    total_size_gb = table_stats["sizeInBytes"] / (1024**3)
    num_files = table_stats["numFiles"]
    avg_file_size_mb = (total_size_gb * 1024) / num_files
    
    # Decision logic
    if query_complexity == "streaming" or avg_file_size_mb < 2:
        return "2mb"  # High parallelism for small files
    elif avg_file_size_mb > 1000:  # Very large files
        return "256mb"  # Fewer, larger partitions
    elif num_files > 10000:  # Many files
        return "8mb"  # Balance parallelism with overhead
    else:
        return "64mb"  # Moderate setting

# Usage
table_path = "/path/to/your/delta/table"
optimal_setting = dynamic_maxpartitionbytes_tuning(table_path, "streaming")
spark.conf.set("spark.sql.files.maxPartitionBytes", optimal_setting)
```

### Compression and File Format Optimization
```python
# Best practices for different file formats
file_format_configs = {
    "parquet": {
        "compression": "snappy",  # Fast compression/decompression
        "spark.sql.files.maxPartitionBytes": "128mb"
    },
    "delta": {
        "compression": "snappy",  # Delta default
        "spark.sql.files.maxPartitionBytes": "128mb",
        "spark.databricks.delta.optimizeWrite.enabled": "true"
    },
    "json": {
        "compression": "gzip",  # High compression for JSON
        "spark.sql.files.maxPartitionBytes": "64mb"  # Smaller for JSON
    },
    "csv": {
        "compression": "gzip",
        "spark.sql.files.maxPartitionBytes": "32mb"
    }
}

def optimize_for_file_format(file_format, file_size_pattern):
    """Optimize maxPartitionBytes for specific file format and size pattern"""
    
    base_config = file_format_configs.get(file_format, {})
    
    # Adjust for file size patterns
    if file_size_pattern == "small_files":
        base_config["spark.sql.files.maxPartitionBytes"] = "8mb"
    elif file_size_pattern == "large_files":
        base_config["spark.sql.files.maxPartitionBytes"] = "256mb"
    
    # Apply settings
    for key, value in base_config.items():
        if key.startswith("spark."):
            spark.conf.set(key, value)
    
    return base_config

# Usage
config = optimize_for_file_format("delta", "small_files")
print(f"Applied configuration: {config}")
```

## When to Use This Skill

- Optimizing Spark query performance on file-based data sources
- Tuning streaming jobs with small file ingestion
- Managing mixed file size datasets in data lakes
- Troubleshooting slow Spark reads and excessive parallelism
- Implementing cost-effective data processing pipelines
- Scaling Spark applications for large-scale analytics

## Performance Benchmarking Template

```python
def comprehensive_performance_test(table_path, test_queries):
    """Comprehensive performance testing template"""
    
    settings_combinations = [
        {"maxPartitionBytes": "1mb", "adaptiveEnabled": True},
        {"maxPartitionBytes": "4mb", "adaptiveEnabled": True},
        {"maxPartitionBytes": "16mb", "adaptiveEnabled": True},
        {"maxPartitionBytes": "64mb", "adaptiveEnabled": False},
        {"maxPartitionBytes": "128mb", "adaptiveEnabled": False}
    ]
    
    all_results = []
    
    for settings in settings_combinations:
        # Apply settings
        spark.conf.set("spark.sql.files.maxPartitionBytes", settings["maxPartitionBytes"])
        spark.conf.set("spark.sql.adaptive.enabled", str(settings["adaptiveEnabled"]).lower())
        
        for query_name, query in test_queries.items():
            # Run query multiple times for stability
            times = []
            for _ in range(3):
                spark.catalog.clearCache()
                start = time.time()
                result = spark.sql(query)
                _ = result.count()
                times.append(time.time() - start)
            
            avg_time = sum(times) / len(times)
            
            result_record = {
                "settings": settings,
                "query": query_name,
                "avg_execution_time": avg_time,
                "min_time": min(times),
                "max_time": max(times),
                "partitions_created": result.rdd.getNumPartitions()
            }
            
            all_results.append(result_record)
    
    return all_results

# Usage
test_queries = {
    "simple_count": "SELECT COUNT(*) FROM delta.`/path/to/table`",
    "group_by_agg": "SELECT category, SUM(amount) FROM delta.`/path/to/table` GROUP BY category",
    "complex_filter": "SELECT * FROM delta.`/path/to/table` WHERE date >= '2024-01-01' AND amount > 100"
}

results = comprehensive_performance_test("/path/to/table", test_queries)

# Find best settings per query type
for query_name in test_queries.keys():
    query_results = [r for r in results if r["query"] == query_name]
    best_result = min(query_results, key=lambda x: x["avg_execution_time"])
    print(f"Best for {query_name}: {best_result['settings']} -> {best_result['avg_execution_time']:.2f}s")
```

## Related Skills

- spark-performance-tuning
- delta-lake-optimization
- adaptive-query-execution
- data-lake-performance
- spark-streaming-optimization