---
name: kafka-architecture-concepts
description: Core Kafka concepts including topics, partitions, brokers, consumers, consumer groups, and how Kafka achieves high throughput and durability. Use when understanding Kafka architecture, designing Kafka-based data pipelines, or explaining Kafka's distributed system design.
---

# Kafka Architecture and Concepts

## Overview

Apache Kafka is a distributed streaming platform designed for high throughput, scalability, and durability. Understanding its core components—topics, partitions, brokers, consumers, and consumer groups—is essential for building reliable data pipelines.

## Quick Start

### Core Components

```python
# Kafka Cluster Structure:
# - Topics: Categories for messages
# - Partitions: Unit of parallelism
# - Brokers: Servers storing messages
# - Producers: Write messages
# - Consumers: Read messages
# - Consumer Groups: Coordinate consumers

# Example: Producer → Topic → Partitions → Consumers
```

### Basic Concepts

```python
# Topic: Named stream of messages
topic = "user-events"

# Partition: Ordered sequence within topic
# Messages ordered within partition, unordered across partitions

# Offset: Sequential ID within partition
# offset 100 = 100th message in partition

# Consumer Group: Set of consumers sharing workload
group_id = "analytics-consumers"
```

## Common Patterns

### Pattern 1: Topic and Partition Design

```python
# Create topic with partitions
# Partitions = unit of parallelism

# Example: 3 partitions for topic
# - Partition 0: offsets 0, 3, 6, ...
# - Partition 1: offsets 1, 4, 7, ...
# - Partition 2: offsets 2, 5, 8, ...

# More partitions = more parallelism
# But: Too many partitions = overhead
```

### Pattern 2: Consumer Groups

```python
# Consumer Group: Multiple consumers sharing workload
# Each consumer reads from different partitions

# Example: Topic with 3 partitions, group with 2 consumers
# - Consumer 1: Reads partitions 0, 1
# - Consumer 2: Reads partition 2

# Max consumers = number of partitions
# More consumers than partitions = idle consumers
```

### Pattern 3: Message Structure

```python
# Kafka message contains:
message = {
    "key": "user123",        # Optional: Used for partitioning
    "value": {...},          # Payload: JSON, Avro, etc.
    "timestamp": 1234567890, # Auto-generated
    "headers": {...}         # Optional metadata
}

# Key determines partition (hash(key) % num_partitions)
# No key = round-robin distribution
```

## Reference Files

### Kafka Architecture

```
Kafka Cluster
├── Broker 1
│   ├── Topic A (Partition 0) [Leader]
│   └── Topic B (Partition 1) [Follower]
├── Broker 2
│   ├── Topic A (Partition 1) [Leader]
│   └── Topic B (Partition 0) [Follower]
└── Broker 3
    ├── Topic A (Partition 0) [Follower]
    └── Topic B (Partition 1) [Leader]
```

### Key Concepts

| Concept | Description |
|---------|-------------|
| **Topic** | Category/stream of messages |
| **Partition** | Ordered sequence within topic (unit of parallelism) |
| **Broker** | Kafka server storing messages |
| **Producer** | Application writing messages |
| **Consumer** | Application reading messages |
| **Consumer Group** | Set of consumers sharing workload |
| **Offset** | Sequential ID within partition |
| **Replication** | Copies of partitions across brokers |

### Consumer Group Behavior

- **One consumer per partition**: Each partition consumed by one consumer in group
- **Rebalancing**: When consumers join/leave, partitions redistributed
- **Offset tracking**: Kafka tracks last consumed offset per group
- **Multiple groups**: Different groups can read same topic independently

## Common Issues

| Issue | Solution |
|-------|----------|
| **Consumer lag** | Add more consumers or increase processing speed |
| **Uneven partition distribution** | Use message keys for even distribution |
| **Rebalancing pauses** | Minimize consumer joins/leaves; use static assignment |
| **Offset management** | Let Kafka manage offsets (auto-commit) or commit manually |
| **Duplicate messages** | Handle at-least-once delivery; use idempotent consumers |

## Advanced Tips

### Partitioning Strategy

```python
# Use message key for partitioning
# Hash(key) % num_partitions = partition

# Good keys:
# - user_id (even distribution)
# - customer_id
# - session_id

# Bad keys:
# - timestamp (all go to same partition)
# - null (round-robin, no ordering)
```

### Replication and Durability

```python
# Replication factor: Number of copies
# Example: factor=3 means 3 copies (1 leader + 2 followers)

# Leader: Handles reads/writes
# Followers: Replicate from leader

# If leader fails: Follower becomes leader
# No data loss (durability)
```

### Consumer Group Coordination

```python
# Group Coordinator: Manages consumer group
# - Assigns partitions to consumers
# - Tracks offsets
# - Handles rebalancing

# Rebalancing triggers:
# - Consumer joins group
# - Consumer leaves group
# - New partitions added
# - Coordinator changes
```

### Offset Management

```python
# Auto-commit (default):
# - Kafka commits offsets periodically
# - Risk: Messages processed but not committed → reprocessed

# Manual commit:
# - Commit after processing
# - Risk: Crash before commit → reprocessed

# Exactly-once: Use idempotent consumers + transactional producers
```

## FAQ

**Q: How many partitions should I have?**
A: Start with number of consumers. Can increase later. Too many = overhead. Rule of thumb: 1-2x consumer count.

**Q: Can multiple consumer groups read same topic?**
A: Yes. Each group maintains its own offsets. Different applications can process independently.

**Q: What happens if a broker fails?**
A: Partitions replicated to other brokers. Follower becomes leader. No data loss (if replication factor > 1).

**Q: How does Kafka ensure ordering?**
A: Messages ordered within partition. Across partitions: no ordering guarantee. Use key to ensure related messages in same partition.

**Q: What's the difference between Kafka and Kinesis?**
A: Kafka: Open source, more flexible. Kinesis: AWS managed, simpler setup. Max consumers: Kafka = 2x partitions, Kinesis = 1x partitions.
