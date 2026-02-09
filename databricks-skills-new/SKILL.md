---
name: databricks-skills
description: "Comprehensive Databricks skills library organized by topic. Quick reference for Spark Streaming, Delta Lake, AI/ML, Performance Tuning, and Data Ingestion patterns."
tags: ["databricks", "spark", "delta-lake", "streaming", "ai-ml", "performance", "reference"]
---

# Databricks Skills Library

A consolidated collection of Databricks patterns, best practices, and code examples organized by topic area.

## 📚 Skill Categories

| Category | Description | Skills |
|----------|-------------|--------|
| **[Streaming](./streaming/)** | Spark Structured Streaming, Kafka, real-time pipelines | 15+ skills |
| **[Delta Lake](./delta-lake/)** | Delta tables, VACUUM, clustering, optimization | 8+ skills |
| **[AI/ML](./ai-ml/)** | Agents, Model Serving, MLflow, Vector Search | 12+ skills |
| **[Performance](./performance/)** | Tuning, optimization, cost reduction | 10+ skills |
| **[Ingestion](./ingestion/)** | Auto Loader, batch ingestion, data loading | 6+ skills |
| **[Common Questions](./common-questions/)** | FAQ-style quick answers | 8+ skills |

## 🚀 Quick Start by Use Case

### Building a Streaming Pipeline?
1. Start with [Streaming Fundamentals](./streaming/SKILL.md)
2. [Kafka to Delta](./streaming/kafka-to-delta.md) - Ingest from Kafka
3. [Stream-Stream Joins](./streaming/stream-stream-joins.md) - Join streaming sources
4. [Streaming Best Practices](./streaming/best-practices.md) - Production checklist

### Optimizing Delta Tables?
1. [Delta Lake Fundamentals](./delta-lake/SKILL.md)
2. [VACUUM Strategies](./delta-lake/vacuum-strategies.md) - Storage cost management
3. [Liquid Clustering](./delta-lake/liquid-clustering.md) - Auto-optimization
4. [Streaming Merges](./delta-lake/streaming-merges.md) - Concurrent updates

### Deploying AI Agents?
1. [AI/ML Fundamentals](./ai-ml/SKILL.md)
2. [Agent Bricks](./ai-ml/agent-bricks.md) - Low-code agent building
3. [Model Serving](./ai-ml/model-serving.md) - Deploy endpoints
4. [Agent Evaluation](./ai-ml/agent-evaluation.md) - Evaluate with MLflow

### Tuning Performance?
1. [Performance Fundamentals](./performance/SKILL.md)
2. [Streaming Cost Optimization](./performance/streaming-cost-optimization.md)
3. [Query Tuning](./performance/query-tuning.md)
4. [Checkpoints & State](./performance/checkpoint-optimization.md)

## 📖 Detailed Skill Index

### Streaming
| Skill | Description |
|-------|-------------|
| [SKILL.md](./streaming/SKILL.md) | Streaming overview and category guide |
| [kafka-to-delta.md](./streaming/kafka-to-delta.md) | Kafka ingestion to Delta Lake |
| [kafka-to-kafka.md](./streaming/kafka-to-kafka.md) | Real-time streaming between Kafka topics |
| [stream-stream-joins.md](./streaming/stream-stream-joins.md) | Join two streaming sources |
| [stream-static-joins.md](./streaming/stream-static-joins.md) | Enrich streams with dimension tables |
| [checkpoint-management.md](./streaming/checkpoint-management.md) | Checkpoint internals and recovery |
| [foreachbatch-patterns.md](./streaming/foreachbatch-patterns.md) | Advanced ForEachBatch usage |
| [real-time-mode.md](./streaming/real-time-mode.md) | Sub-second latency with RTM |
| [state-management.md](./streaming/state-management.md) | Stateful operations and watermarks |
| [deduplication.md](./streaming/deduplication.md) | Exactly-once and dedup strategies |
| [error-recovery.md](./streaming/error-recovery.md) | Failure handling and recovery |
| [best-practices.md](./streaming/best-practices.md) | Production streaming checklist |

### Delta Lake
| Skill | Description |
|-------|-------------|
| [SKILL.md](./delta-lake/SKILL.md) | Delta Lake overview and category guide |
| [vacuum-strategies.md](./delta-lake/vacuum-strategies.md) | Storage cleanup and cost management |
| [liquid-clustering.md](./delta-lake/liquid-clustering.md) | Automatic data clustering |
| [streaming-merges.md](./delta-lake/streaming-merges.md) | Concurrent streaming merges with DV/RLC |
| [deletion-vectors.md](./delta-lake/deletion-vectors.md) | Soft deletes and merge optimization |
| [time-travel.md](./delta-lake/time-travel.md) | Query historical versions |
| [schema-evolution.md](./delta-lake/schema-evolution.md) | Handle schema changes |
| [cdc-patterns.md](./delta-lake/cdc-patterns.md) | Change data capture patterns |

### AI/ML
| Skill | Description |
|-------|-------------|
| [SKILL.md](./ai-ml/SKILL.md) | AI/ML overview and category guide |
| [agent-bricks.md](./ai-ml/agent-bricks.md) | Low-code AI agent development |
| [model-serving.md](./ai-ml/model-serving.md) | Deploy models to endpoints |
| [vector-search.md](./ai-ml/vector-search.md) | Semantic search and RAG |
| [agent-evaluation.md](./ai-ml/agent-evaluation.md) | Evaluate agents with MLflow |
| [custom-scorers.md](./ai-ml/custom-scorers.md) | Build custom LLM judges |
| [genie-spaces.md](./ai-ml/genie-spaces.md) | Natural language data querying |
| [synthetic-data.md](./ai-ml/synthetic-data.md) | Generate test data |
| [mlflow-tracing.md](./ai-ml/mlflow-tracing.md) | Instrument and debug AI apps |
| [batch-inference.md](./ai-ml/batch-inference.md) | Scale model inference |
| [multi-agent-systems.md](./ai-ml/multi-agent-systems.md) | Orchestrate agent teams |

### Performance
| Skill | Description |
|-------|-------------|
| [SKILL.md](./performance/SKILL.md) | Performance overview and category guide |
| [streaming-cost-optimization.md](./performance/streaming-cost-optimization.md) | Reduce streaming S3 costs |
| [query-tuning.md](./performance/query-tuning.md) | Spark query optimization |
| [checkpoint-optimization.md](./performance/checkpoint-optimization.md) | Checkpoint tuning |
| [partitioning-strategy.md](./performance/partitioning-strategy.md) | Data layout optimization |
| [storage-optimization.md](./performance/storage-optimization.md) | File compaction and layout |
| [cache-warming.md](./performance/cache-warming.md) | SQL warehouse cache optimization |
| [autoscaling-guide.md](./performance/autoscaling-guide.md) | Cluster autoscaling patterns |
| [file-read-optimization.md](./performance/file-read-optimization.md) | maxPartitionBytes tuning |

### Ingestion
| Skill | Description |
|-------|-------------|
| [SKILL.md](./ingestion/SKILL.md) | Ingestion overview and category guide |
| [auto-loader.md](./ingestion/auto-loader.md) | Incremental data loading |
| [auto-loader-schema.md](./ingestion/auto-loader-schema.md) | Handle schema drift |
| [dlt-patterns.md](./ingestion/dlt-patterns.md) | Delta Live Tables patterns |
| [batch-ingestion.md](./ingestion/batch-ingestion.md) | Batch loading patterns |
| [salesforce-ingestion.md](./ingestion/salesforce-ingestion.md) | Salesforce connector patterns |
| [schema-inference.md](./ingestion/schema-inference.md) | Schema detection strategies |

### Common Questions (FAQ)
Quick answers to frequent questions:
- [Checkpoint Best Practices](./common-questions/streaming/checkpoint-best-practices/)
- [Auto Loader Schema Drift](./common-questions/ingestion/auto-loader-schema-drift/)
- [DLT vs Jobs](./common-questions/ingestion/dlt-vs-jobs/)
- [Merge Performance](./common-questions/performance/merge-performance/)
- [Partitioning Strategy](./common-questions/performance/partitioning-strategy/)
- [Unity Catalog Streaming](./common-questions/governance/unity-catalog-streaming/)
- [Cost Tuning](./common-questions/governance/cost-tuning/)
- [Backfill Patterns](./common-questions/operations/backfill-patterns/)

## 🔄 Original Sources

Skills consolidated from:
- **Canadian Data Guy** YouTube/blog content - 17 skills
- **Databricksters** blog content - 43 skills
- **YouTube** tutorials - 25 skills
- **Common Questions** FAQ - 8 skills
- **Expert Packs** - 10 comprehensive guides

## 📝 Contributing

To add new skills:
1. Choose appropriate category folder
2. Create SKILL.md with YAML frontmatter
3. Include: Overview, Quick Start, Common Patterns, Original Sources
4. Update category SKILL.md index

## 📚 Related Resources

- [Databricks Documentation](https://docs.databricks.com/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [MLflow Documentation](https://mlflow.org/docs/latest/)
