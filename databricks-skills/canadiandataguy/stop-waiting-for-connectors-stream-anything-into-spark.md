---
name: stop-waiting-for-connectors-stream-anything-into-spark
description: Build custom Spark streaming sources for any API using 4 required functions: initialOffset, latestOffset, partitions, and read. Use when streaming from REST APIs, Ethereum/blockchain data, Shopify, Stripe, GitHub, or any data source without native Spark connectors.
---

# Stop Waiting for Connectors: Stream ANYTHING into Spark

## Overview

Spark Streaming allows you to ingest data from virtually any API by implementing just **4 core functions**. This pattern works for REST APIs, Web3/Ethereum, Shopify, Stripe, GitHub, or any data source that exposes an API.

The key insight: Spark doesn't need native connectors if you can define how to read your data source.

## Quick Start

### The 4 Required Functions

```python
class CustomDataSource:
    def initial_offset(self):
        """Returns the starting offset for new streaming queries.
        Called once when checkpoint doesn't exist."""
        return {"partition": 0, "offset": start_block_number}
    
    def latest_offset(self):
        """Returns the latest available offset.
        Called at the start of every microbatch on the driver."""
        return {"partition": 0, "offset": get_latest_from_api()}
    
    def partitions(self, start, end):
        """Creates partitions for parallel processing.
        Called after latest_offset on the driver.
        Determines parallelism for this batch."""
        return create_partition_objects(start, end)
    
    def read(self, partition):
        """Reads actual data for a given partition.
        Called in parallel on executor nodes."""
        return fetch_data_from_api(partition)
```

### Basic Usage

```python
stream_df = (spark
    .readStream
    .format("custom_source")  # Your registered source name
    .option("shufflePartitions", 20)  # Control parallelism
    .option("maxRetries", 3)  # Handle API failures
    .load()
)

# Write to Delta
(stream_df
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/catalog/volume/checkpoints/stream")
    .trigger(availableNow=True)  # Process all available data, then stop
    .start("/path/to/target_table")
)
```

## Common Patterns

### Pattern 1: Ethereum Blockchain Ingestion

```python
# Example: Streaming Ethereum blockchain data
initial_offset = latest_block - 200  # Start from recent history

# Configure shuffle partitions based on API rate limits
# If API allows 20 requests/second, set shufflePartitions=20
stream_df = (spark
    .readStream
    .format("ethereum")
    .option("ethereumURI", "https://api.ethereum-node.com")
    .option("shufflePartitions", 20)  # Match rate limits
    .option("maxRetries", 3)
    .load()
)
```

### Pattern 2: Generic REST API Pattern

```python
# Works for Shopify, Stripe, GitHub, etc.
# Key insight: The pattern is identical regardless of API

stream_df = (spark
    .readStream
    .format("custom_api")
    .option("apiEndpoint", "https://api.service.com/v1/data")
    .option("authToken", dbutils.secrets.get("scope", "token"))
    .option("shufflePartitions", 10)
    .load()
)
```

### Pattern 3: Resuming from Existing Data

```python
# If you've already ingested data to block 1M and want to switch tools:
# Set initial offset to 1,000,001 to continue from where you left off

initial_offset = 1000001  # Skip already-ingested data
```

## Reference Files

### Checkpoint Structure

The checkpoint location stores:
- **offsets/**: What has been processed (max offset + 1 for next run)
- **commits/**: Confirmation that processing completed
- **sources/**: Source metadata (topic, partitions, starting positions)

### Key Configuration Options

| Option | Purpose | Example |
|--------|---------|---------|
| shufflePartitions | Parallelism | Match to API rate limits |
| maxRetries | Fault tolerance | 3 retries for transient failures |
| trigger(availableNow=True) | Batch-style processing | Process backlog, then stop |
| checkpointLocation | State persistence | Unity Catalog volume path |

## Common Issues

| Issue | Solution |
|-------|----------|
| **Rate limiting errors** | Set shufflePartitions to match API limits (e.g., 20 req/s = 20 partitions) |
| **First microbatch too large** | Python API limitation: processes start→end offset. Use Scala if this is a bottleneck |
| **Checkpoint in DBFS** | Always use Unity Catalog volumes (S3/ADLS-backed), not DBFS |
| **Duplicate data** | Ensure deterministic inputs: same offset always returns same data |
| **API failures** | Implement retry logic in your read() function |

## Advanced Tips

### Determinism is Critical

Your source must be deterministic: `f(offset) = same_data` every time.
- Good: Block number → block data (immutable)
- Bad: `GET /latest` → different results each call

### Performance Targets

- Aim for processing rate 10x input rate (minimum 3x)
- 3x = recover from 3-day outage in 1 day
- Monitor `inputRate` vs `processingRate` in Spark UI

### Recovery After Outages

If you have a large gap (e.g., AWS outage for hours):
1. Spark reads checkpoint offset (e.g., block 65,850)
2. Gets latest offset from API (e.g., block 72,000)
3. Processes 6,000 blocks across shuffle partitions
4. Checkpoint updated to 72,001

## FAQ

**Q: Can I stream from any REST API using this pattern?**
A: Yes. As long as you implement the 4 functions, Spark can stream from any API source.

**Q: How do I handle APIs with different rate limits?**
A: Tune `shufflePartitions` to match your rate limit. 20 req/s → 20 partitions.

**Q: What's the difference between initialOffset and latestOffset?**
A: `initialOffset` runs once (first startup). `latestOffset` runs every microbatch.

**Q: Can I use this for production workloads?**
A: Yes. The Ethereum demo ran continuously for 7+ days processing real blockchain data.