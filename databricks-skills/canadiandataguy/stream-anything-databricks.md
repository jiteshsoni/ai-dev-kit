---
name: stream-anything-databricks
description: Use Auto Loader with custom UDFs to stream any file format into Spark, including unsupported formats like ROS bag files, video files, or custom binary formats. Use when processing non-standard file types, maintaining file order, or implementing custom file parsers in streaming pipelines.
---

# Stream Anything on Databricks

## Overview

Auto Loader can process any file type, not just natively supported formats (JSON, CSV, Parquet). Use Auto Loader's `binaryfile` format with custom UDFs to parse unsupported file types like ROS bag files, video files, or any custom binary format.

## Quick Start

### Basic Pattern

```python
from pyspark.sql.functions import udf, explode
from pyspark.sql.types import ArrayType, StructType

# 1. Use Auto Loader with binaryfile format
stream_df = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "binaryfile")  # Read as binary
    .option("cloudFiles.includeExistingFiles", "true")
    .load("s3://bucket/path/to/files/")
)

# 2. Create custom UDF to parse file
@udf(returnType=ArrayType(StructType([...])))
def parse_custom_format(file_path: str, file_content: bytes) -> list:
    # Parse binary content
    # Return list of rows
    pass

# 3. Parse and explode
parsed_df = stream_df.select(
    col("path"),
    explode(parse_custom_format(col("path"), col("content"))).alias("row")
)

# 4. Write to Delta
parsed_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .start("target_table")
```

## Common Patterns

### Pattern 1: ROS Bag File Processing

```python
import rosbag
import tempfile
import boto3
from typing import List, Dict

@udf(returnType=ArrayType(StructType([
    StructField("timestamp", LongType()),
    StructField("topic", StringType()),
    StructField("data", StringType())
])))
def parse_rosbag(s3_path: str) -> List[Dict]:
    """Parse ROS bag file from S3"""
    # Extract bucket and key
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    
    # Download to temp file
    s3 = boto3.client("s3")
    with tempfile.NamedTemporaryFile() as tmp:
        s3.download_fileobj(bucket, key, tmp)
        tmp.seek(0)
        
        # Parse ROS bag
        bag = rosbag.Bag(tmp.name)
        results = []
        for topic, msg, t in bag.read_messages():
            results.append({
                "timestamp": t.to_nsec(),
                "topic": topic,
                "data": str(msg)
            })
        return results

# Use in stream
stream_df = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "binaryfile")
    .load("s3://bucket/rosbag/")
)

parsed = stream_df.select(
    col("path"),
    explode(parse_rosbag(col("path"))).alias("row")
).select("row.*")
```

### Pattern 2: Video File Metadata Extraction

```python
import cv2
import tempfile

@udf(returnType=StructType([
    StructField("duration", DoubleType()),
    StructField("fps", DoubleType()),
    StructField("width", IntegerType()),
    StructField("height", IntegerType()),
    StructField("frame_count", LongType())
]))
def extract_video_metadata(file_path: str, content: bytes) -> Dict:
    """Extract metadata from video file"""
    with tempfile.NamedTemporaryFile(suffix=".mp4") as tmp:
        tmp.write(content)
        tmp.flush()
        
        cap = cv2.VideoCapture(tmp.name)
        fps = cap.get(cv2.CAP_PROP_FPS)
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        duration = frame_count / fps if fps > 0 else 0
        
        cap.release()
        
        return {
            "duration": duration,
            "fps": fps,
            "width": width,
            "height": height,
            "frame_count": frame_count
        }

# Stream video files
video_df = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "binaryfile")
    .load("s3://bucket/videos/")
)

metadata_df = video_df.select(
    col("path"),
    extract_video_metadata(col("path"), col("content")).alias("metadata")
).select("path", "metadata.*")
```

### Pattern 3: Custom Binary Format

```python
import struct

@udf(returnType=ArrayType(StructType([
    StructField("id", LongType()),
    StructField("value", DoubleType()),
    StructField("timestamp", LongType())
])))
def parse_custom_binary(file_path: str, content: bytes) -> List[Dict]:
    """Parse custom binary format"""
    results = []
    offset = 0
    
    # Read records (each record: 8 bytes id + 8 bytes value + 8 bytes timestamp)
    record_size = 24
    while offset + record_size <= len(content):
        record = struct.unpack(">QdQ", content[offset:offset+record_size])
        results.append({
            "id": record[0],
            "value": record[1],
            "timestamp": record[2]
        })
        offset += record_size
    
    return results

# Use in stream
binary_df = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "binaryfile")
    .load("s3://bucket/binary/")
)

parsed = binary_df.select(
    col("path"),
    explode(parse_custom_binary(col("path"), col("content"))).alias("row")
).select("row.*")
```

## Reference Files

### Auto Loader Binary File Format

When using `binaryfile` format, Auto Loader provides:
- `path`: Full file path
- `content`: File content as bytes
- `length`: File size in bytes
- `modificationTime`: File modification timestamp

### File Processing Flow

```
1. Auto Loader detects new files
   ↓
2. Reads files as binary (binaryfile format)
   ↓
3. Custom UDF parses binary content
   ↓
4. Returns structured data (rows)
   ↓
5. Explode to create one row per record
   ↓
6. Write to Delta table
```

### Use Cases

- **ROS bag files**: Robot Operating System data
- **Video files**: Extract metadata or frames
- **Custom binary**: Proprietary formats
- **Audio files**: Extract metadata or transcribe
- **Image files**: Extract EXIF data or process
- **Archive files**: Extract contents (ZIP, TAR)

## Common Issues

| Issue | Solution |
|-------|----------|
| **Large files causing OOM** | Process files in chunks; use Pandas UDF for batch processing |
| **File order matters** | Auto Loader processes in file order; use timestamp for ordering |
| **UDF too slow** | Use Pandas UDF for vectorized processing |
| **Memory issues** | Reduce maxFilesPerTrigger; process smaller batches |
| **File format errors** | Add error handling in UDF; use rescuedDataColumn |

## Advanced Tips

### Maintaining File Order

```python
# Auto Loader processes files in order
# Add file timestamp for explicit ordering
from pyspark.sql.functions import input_file_name, file_metadata

ordered_df = stream_df.withColumn(
    "file_timestamp",
    file_metadata("modificationTime", input_file_name())
).orderBy("file_timestamp")
```

### Error Handling

```python
@udf(returnType=ArrayType(...))
def safe_parse(file_path: str, content: bytes) -> List[Dict]:
    try:
        return parse_file(content)
    except Exception as e:
        # Log error, return empty list or error record
        print(f"Error parsing {file_path}: {e}")
        return [{"error": str(e), "file": file_path}]
```

### Performance Optimization

```python
# Use Pandas UDF for better performance
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(returnType=ArrayType(...))
def vectorized_parse(paths: pd.Series, contents: pd.Series) -> pd.Series:
    results = []
    for path, content in zip(paths, contents):
        parsed = parse_file(content)
        results.append(parsed)
    return pd.Series(results)

# Process in batches (better than row-by-row)
```

## FAQ

**Q: Can I process any file type?**
A: Yes. Use `binaryfile` format and write a custom UDF to parse the content.

**Q: How do I maintain file processing order?**
A: Auto Loader processes files in order. Use file modification timestamp for explicit ordering.

**Q: What if files are very large?**
A: Process in chunks within UDF, or use `maxFilesPerTrigger` to limit batch size.

**Q: Can I use this for streaming?**
A: Yes. Auto Loader with `binaryfile` works with Structured Streaming.

**Q: How do I handle parsing errors?**
A: Add try/except in UDF. Return error records or use `rescuedDataColumn` option.
