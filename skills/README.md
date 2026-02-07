# Skills Library Documentation

## Overview

This Skills Library is automatically generated from local blog posts, YouTube transcripts, and community demand. It provides structured, searchable knowledge for Databricks and Spark development.

## Folder Structure

```
skills/
├── blogs/
│   └── canadiandataguy/          # Blog-based skills (1 per post)
│       ├── stop-waiting-for-connectors-stream-anything-into-spark/
│       ├── your-low-code-shortcut-to-production-grade-agent/
│       ├── spark-streaming-master-class-kafka-to-delta/
│       ├── mastering-checkpoints-in-spark-streaming/
│       ├── unlocking-sub-second-latency/
│       ├── futureproof-your-data-engineering-skills/
│       ├── spark-stream-static-joins/
│       ├── spark-streaming-recovery/
│       ├── parallel-writes-foreachbatch/
│       ├── liquid-dv-rlc-streaming-merges/
│       ├── scaling-spark-streaming-jobs/
│       ├── introduction-to-foreachbatch/
│       ├── vacuum-storage-optimization/
│       ├── personal-knowledge-agent/
│       └── synthetic-data-generation/
│   └── databricksters/            # Reserved for future content
├── youtube/                       # Reserved for YouTube-specific skills
├── spark-structured-streaming/    # Expert pack (consolidated reference)
│   ├── SKILL.md
│   └── merges-and-optimizations.md
└── common-questions/              # FAQ skills (community demand)
    ├── ingestion/
    │   ├── auto-loader-schema-drift/
    │   └── dlt-vs-jobs/
    ├── streaming/
    │   └── checkpoint-best-practices/
    ├── delta/
    ├── performance/
    │   ├── merge-performance/
    │   └── partitioning-strategy/
    ├── governance/
    │   ├── unity-catalog-streaming/
    │   └── cost-tuning/
    └── operations/
        └── backfill-patterns/
```

## Skill Schema

Each `SKILL.md` follows this frontmatter structure:

```yaml
---
name: "Exact Blog Title or Skill Name"
description: "Brief one-sentence description"
author: "Author Name"              # For blog skills
url: "Source URL"                  # For blog skills
date: "YYYY-MM-DD"                 # For blog skills
question: "For FAQ skills"         # FAQ only
answer: |                         # FAQ only
  Multi-line answer
tags: ["tag1", "tag2"]
related_links:                     # Optional
  - url_or_path
---
```

### Required Sections

All skills include:
- **Overview**: What this skill covers
- **Quick Start**: Minimal working example
- **Common Patterns**: 3+ patterns with code
- **Reference Files**: Tables, configs, formats
- **Common Issues**: Problem/solution table
- **FAQ**: (Optional) Additional Q&A

## Regeneration Process

### Prerequisites

```bash
# Content sources (must exist locally)
/Users/AIroommate/Documents/YouTube/CanadianDataGuy/*.md
```

### Regeneration Steps

1. **Read Content Sources**
   ```bash
   # Blog posts with date prefixes
   ls /Users/AIroommate/Documents/YouTube/CanadianDataGuy/*.md
   ```

2. **Generate Blog Skills**
   - One skill per blog post
   - Skill name = exact blog title
   - Extract: code, patterns, issues, tips

3. **Generate Expert Pack**
   - Consolidate streaming knowledge
   - Cross-reference blog skills
   - Add advanced patterns

4. **Generate FAQ Skills**
   - Mine community questions
   - Map to existing content
   - Create question-answer format

5. **Validate Structure**
   ```bash
   # Check all SKILL.md files exist
   find skills -name "SKILL.md" | wc -l
   
   # Verify frontmatter
   grep -r "^---$" skills/*/SKILL.md
   ```

6. **Commit Changes**
   ```bash
   git add skills/
   git commit -m "Regenerate skills from local blogs $(date +%Y-%m-%d)"
   ```

## Content Sources

### Blog Posts Processed

| Date | Title | Skill Path |
|------|-------|------------|
| 2025-11-28 | Stop Waiting for Connectors: Stream ANYTHING into Spark | blogs/canadiandataguy/stop-waiting-for-connectors... |
| 2025-11-28 | Your Low-Code Shortcut to Production-Grade Agent | blogs/canadiandataguy/your-low-code-shortcut... |
| 2025-11-29 | Spark Streaming Master Class: Ingest data from Kafka | blogs/canadiandataguy/spark-streaming-master-class... |
| 2025-11-29 | Mastering Checkpoints in Spark Streaming | blogs/canadiandataguy/mastering-checkpoints... |
| 2025-11-29 | A Deep Dive into Spark Stream-Static Joins | blogs/canadiandataguy/spark-stream-static-joins |
| 2025-11-29 | How Spark Structured Streaming Recovers After Failures | blogs/canadiandataguy/spark-streaming-recovery |
| 2025-11-29 | Speeding Up Spark Streaming: Mastering Parallel Writes | blogs/canadiandataguy/parallel-writes-foreachbatch |
| 2025-11-29 | How Liquid, DV & RLC help improve Streaming Merges | blogs/canadiandataguy/liquid-dv-rlc-streaming-merges |
| 2025-11-29 | How Many Spark Streaming Jobs Can You REALLY Run | blogs/canadiandataguy/scaling-spark-streaming-jobs |
| 2025-11-29 | Harnessing Spark Streaming: Introduction to ForEachBatch | blogs/canadiandataguy/introduction-to-foreachbatch |
| 2025-11-29 | How to generate synthetic data at scale | blogs/canadiandataguy/synthetic-data-generation |
| 2025-12-02 | Your Storage Bill Is Too High. Here Are 3 Levels of VACUUM | blogs/canadiandataguy/vacuum-storage-optimization |
| 2026-01-13 | How you can turn your notes into a personal Knowledge Agent | blogs/canadiandataguy/personal-knowledge-agent |
| 2026-01-23 | Unlocking Sub-Second Latency with Databricks | blogs/canadiandataguy/unlocking-sub-second-latency |
| 2026-01-27 | FutureProof Your Data Engineering Skills | blogs/canadiandataguy/futureproof-your-data-engineering-skills |

### FAQ Topics Covered

| Category | Topics |
|----------|--------|
| Ingestion | Auto Loader schema drift, DLT vs Jobs |
| Streaming | Checkpoint best practices |
| Performance | Merge tuning, Partitioning strategy |
| Governance | Unity Catalog streaming, Cost tuning |
| Operations | Backfill patterns |

## Maintenance

### Adding New Content

1. Add blog post to source directory
2. Run regeneration process
3. New skill created automatically
4. Update documentation

### Updating Existing Skills

1. Edit source blog post
2. Re-run regeneration
3. Skill updated with new content

### Gaps to Fill

- [ ] YouTube-specific skills (youtube/ folder)
- [ ] Databricksters blog content
- [ ] More FAQ topics from Reddit/StackOverflow
- [ ] Code examples in Scala
- [ ] Unity Catalog deep-dive
- [ ] MLflow + Streaming integration

## Usage

### For AI Agents

Reference skills by path:
```
skills/blogs/canadiandataguy/spark-streaming-master-class-kafka-to-delta/
skills/spark-structured-streaming/SKILL.md
skills/common-questions/performance/merge-performance/
```

### For Humans

Browse by category:
- **Blog Skills**: Specific topic deep-dives
- **Expert Pack**: Consolidated reference
- **FAQ Skills**: Quick answers to common questions

## License

Content derived from Canadian Data Guy / Databricksters blogs under original license.
Skills structure and organization: ai-dev-kit project.