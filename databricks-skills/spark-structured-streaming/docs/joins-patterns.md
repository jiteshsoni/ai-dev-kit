---
name: joins-patterns
description: Stream join patterns - stream-stream joins with watermarks and stream-static joins with Delta dimension tables.
tags: ["streaming", "joins", "watermark", "enrichment", "delta"]
---

# Stream Joins

Join streaming data with other streams or static dimension tables.

## Quick Decision Matrix

| Join Type | When to Use | Requirements |
|-----------|-------------|--------------|
| [Stream-Stream](#stream-stream-joins) | Correlate events from different sources | Watermarks on both sides, time bounds |
| [Stream-Static](#stream-static-joins) | Enrich with dimension/reference data | Delta table (for refresh), left join recommended |

---

## Stream-Stream Joins

Join two streaming sources to correlate events arriving at different times.

### Basic Pattern

```python
from pyspark.sql.functions import expr

# Read two streaming sources
orders = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "orders")
    .load()
    .select(from_json(col("value").cast("string"), order_schema).alias("data"))
    .select("data.*")
    .withWatermark("order_time", "10 minutes")
)

payments = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "payments")
    .load()
    .select(from_json(col("value").cast("string"), payment_schema).alias("data"))
    .select("data.*")
    .withWatermark("payment_time", "10 minutes")
)

# Join with time bounds
matched = (orders
    .join(
        payments,
        expr("""
            orders.order_id = payments.order_id AND
            payments.payment_time >= orders.order_time - interval 5 minutes AND
            payments.payment_time <= orders.order_time + interval 10 minutes
        """),
        "leftOuter"  # Include orders without payments
    )
)

matched.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/orders_payments") \
    .start("/delta/order_payments")
```

### Why Watermarks Are Required

Stream-stream joins are stateful - both sides buffer events until matches are found or state expires:

```python
# Watermark = latest_event_time - delay_threshold
.withWatermark("event_time", "10 minutes")

# Events with timestamp < watermark are considered "too late"
# State is automatically cleaned up after watermark expires
```

### Join Types

| Join Type | Matches | Late Events Behavior |
|-----------|---------|---------------------|
| **Inner** | Both sides match | May still match if other side hasn't expired |
| **Left Outer** | All left + matched right | Dropped from left after watermark expires |
| **Full Outer** | All events from both | Dropped after watermark on respective side |

### Common Patterns

**Order-Payment Matching:**
```python
matched = (orders
    .join(
        payments,
        expr("""
            orders.order_id = payments.order_id AND
            payments.payment_time >= orders.order_time - interval 5 minutes AND
            payments.payment_time <= orders.order_time + interval 10 minutes
        """),
        "leftOuter"
    )
    .withColumn("matched", col("payment_id").isNotNull())
)
```

**Click-Conversion Attribution:**
```python
impressions = (spark.readStream
    .format("kafka")
    .option("subscribe", "impressions")
    .load()
    .withWatermark("impression_time", "1 hour")
)

conversions = (spark.readStream
    .format("kafka")
    .option("subscribe", "conversions")
    .load()
    .withWatermark("conversion_time", "2 hours")
)

attributed = (impressions
    .join(
        conversions,
        expr("""
            impressions.user_id = conversions.user_id AND
            conversions.conversion_time BETWEEN impressions.impression_time AND
                                                impressions.impression_time + interval 1 day
        """),
        "leftOuter"
    )
)
```

### Watermark Tuning for Joins

Different watermarks for streams with different latencies:

```python
# Fast source: shorter watermark
impressions = stream1.withWatermark("event_time", "5 minutes")

# Slower source: longer watermark  
clicks = stream2.withWatermark("event_time", "15 minutes")

# Effective watermark = max(5, 15) = 15 minutes
```

---

## Stream-Static Joins

Join a stream with a static (batch) dimension table for enrichment.

### Basic Pattern

```python
# Streaming source
stream = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "events")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

# Static dimension table (Delta)
dim = spark.table("users.dimension")

# Enrich stream with dimension data
enriched = stream.join(dim, "user_id", "left")
```

### Why Delta Tables Are Recommended

| Aspect | Delta Table | Non-Delta (Parquet/CSV) |
|--------|-------------|------------------------|
| Refresh | Per microbatch | Once at startup only |
| Consistency | Versioned reads | Stale data |
| Performance | Optimized | Full scan each time |

```python
# Delta table - refreshes each microbatch with versioning
dim = spark.table("users.dimension")  # Current version each batch

# Non-Delta - reads once at startup
dim = spark.read.parquet("/path/to/dim")  # Never refreshes!
```

### Production Pattern (Left Join)

```python
# Left join recommended - preserves all stream events
enriched = (stream
    .join(dim, "user_id", "left")
    .select(
        col("event_id"),
        col("user_id"),
        col("event_type"),
        col("user_name"),        # From dimension (null if no match)
        col("user_segment"),     # From dimension (null if no match)
        col("timestamp")
    )
)

(enriched
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/enriched_events")
    .start("/delta/enriched_events")
)
```

### Inner Join (Filtering)

Use inner join when you only want events with matching dimension data:

```python
# Only events with valid user records
filtered = stream.join(dim, "user_id", "inner")
```

### Multi-Table Enrichment

Chain multiple enrichments:

```python
# Enrich with users
with_users = stream.join(user_dim, "user_id", "left")

# Enrich with products
with_products = with_users.join(product_dim, "product_id", "left")

# Enrich with geography
enriched = with_products.join(geo_dim, "zip_code", "left")
```

---

## Best Practices

### Stream-Stream Joins
- ✅ Always use watermarks on both sides
- ✅ Use time bounds in join condition
- ✅ Start with left outer to avoid data loss
- ✅ Monitor state store size
- ❌ Don't use inner joins if you need all events

### Stream-Static Joins
- ✅ Use Delta tables for auto-refresh
- ✅ Use left joins for production (preserve all events)
- ✅ Keep dimension tables small (< 1GB broadcasted)
- ✅ Consider caching for frequently-accessed dimensions
- ❌ Don't use non-Delta formats (stale data)

### Performance Tips

**Broadcast Hint for Small Dimensions:**
```python
from pyspark.sql.functions import broadcast

# Spark auto-broadcasts small tables, but explicit hint helps
enriched = stream.join(broadcast(dim), "user_id", "left")
```

**Partition Alignment:**
```python
# If both stream and dim are partitioned on join key, 
# avoid shuffle by repartitioning stream
stream_repartitioned = stream.repartition("user_id")
enriched = stream_repartitioned.join(dim, "user_id", "left")
```

---

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| Growing state store | Watermark too long | Reduce watermark duration |
| Missing matches | Events too late | Increase watermark or check clock sync |
| Stale dimension data | Non-Delta format | Convert to Delta table |
| Slow joins | Large shuffle | Use broadcast hints, partition alignment |
| Null dimension values | Missing keys in dim | Use left join, handle nulls in downstream |
