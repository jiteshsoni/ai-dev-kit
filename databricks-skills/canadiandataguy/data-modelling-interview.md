---
name: data-modelling-interview
description: Structured approach to data modeling interviews covering requirements gathering, entity relationships, fact/dimension design, and normalization decisions. Use when preparing for data engineering interviews, designing data models, or structuring your approach to modeling problems.
---

# Data Modeling Interview Guide

## Overview

Ace data modeling interviews by following a structured approach: understand requirements, define entities and relationships, design fact and dimension tables, and make informed normalization decisions. This skill provides a framework for tackling data modeling questions systematically.

## Quick Start

### Interview Framework

```python
# Step-by-step approach:
# 1. Understand requirements (functional + non-functional)
# 2. Define entities and relationships
# 3. Design fact and dimension tables
# 4. Decide normalization vs denormalization
# 5. Design aggregation tables (if needed)
# 6. Discuss partitioning strategy
```

### Key Questions to Ask

```python
# Functional Requirements:
# - What questions will this model answer?
# - Who are the users? (business users, analysts, data scientists)
# - What are the key metrics?

# Non-Functional Requirements:
# - Data freshness: Real-time, near real-time, or batch?
# - Data volumes: Expected size and growth?
# - Retention: How long to keep data?
# - Query patterns: What are common queries?
```

## Common Patterns

### Pattern 1: Entity-Relationship Design

```python
# Identify key entities:
# - Customers (dimension)
# - Products (dimension)
# - Orders (fact)
# - Transactions (fact)

# Define relationships:
# - Customer → Orders (one-to-many)
# - Product → Orders (one-to-many)
# - Order → Transactions (one-to-many)

# Use ER diagrams to visualize
# Validate with interviewer as you go
```

### Pattern 2: Fact and Dimension Tables

```python
# Fact Table Design:
# - Captures events/transactions
# - Contains measures (amounts, quantities)
# - Foreign keys to dimensions
# - Granularity: One row per event

# Example: Orders Fact Table
fact_orders = {
    "order_id": "PK",
    "customer_id": "FK → dim_customers",
    "product_id": "FK → dim_products",
    "order_date": "FK → dim_date",
    "order_amount": "Measure",
    "quantity": "Measure"
}

# Dimension Table Design:
# - Provides context/attributes
# - Slowly-changing dimensions (SCD)
# - Surrogate keys for consistency
```

### Pattern 3: Normalization Decision

```python
# Normalized (3NF):
# - Eliminates redundancy
# - Good for transactional systems
# - More joins required for queries

# Denormalized (Star Schema):
# - Reduces joins
# - Good for analytical systems
# - Faster queries, more storage

# Decision factors:
# - Query patterns (many joins? → denormalize)
# - Update frequency (frequent updates? → normalize)
# - Storage vs performance trade-off
```

## Reference Files

### Common Metrics to Support

| Metric | Description | Fact/Dimension |
|--------|-------------|----------------|
| **DAU/MAU** | Daily/Monthly Active Users | Fact: User events |
| **Churn Rate** | Users who stopped using service | Fact: User lifecycle events |
| **CLV** | Customer Lifetime Value | Fact: Transactions, Dimension: Customers |
| **NPS** | Net Promoter Score | Fact: Survey responses |
| **Revenue** | Total sales | Fact: Transactions |

### Partitioning Strategy

```python
# Choose partition columns based on:
# - Query patterns (frequently filtered?)
# - Data distribution (even distribution?)
# - Cardinality (< 10,000 distinct values)
# - Immutability (won't change)

# Common choices:
# - Date columns (event_date, order_date)
# - Geographic regions (country, region)
# - Business units (department, division)

# Avoid:
# - High cardinality (user_id, timestamp)
# - Skewed distribution
# - Frequently updated columns
```

### Aggregation Tables

```python
# Pre-computed aggregations for reporting
# Naming: agg_*, summary_*

# Example:
agg_daily_sales = {
    "sale_date": "PK",
    "product_category": "PK",
    "total_revenue": "SUM(amount)",
    "total_quantity": "SUM(quantity)",
    "order_count": "COUNT(DISTINCT order_id)"
}

# Benefits:
# - Faster reporting queries
# - Reduced compute cost
# - Trade-off: Storage and maintenance
```

## Common Issues

| Issue | Solution |
|-------|----------|
| **Over-normalization** | Denormalize for analytical queries |
| **Under-normalization** | Normalize for transactional systems |
| **Wrong granularity** | Match fact table to business events |
| **Missing dimensions** | Identify all contextual attributes |
| **Poor partitioning** | Choose based on query patterns |

## Advanced Tips

### Interview Best Practices

```python
# 1. Ask clarifying questions first
# - Don't assume requirements
# - Understand use cases
# - Clarify NFRs

# 2. Draw diagrams as you go
# - ER diagrams
# - Star schema diagrams
# - Validate with interviewer

# 3. Explain trade-offs
# - Normalization vs denormalization
# - Partitioning choices
# - Aggregation strategies

# 4. Think about scalability
# - How will model handle growth?
# - What about new requirements?
# - How to handle schema evolution?
```

### Common Modeling Patterns

```python
# Star Schema:
# - One fact table
# - Multiple dimension tables
# - Denormalized dimensions
# - Fast queries

# Snowflake Schema:
# - Normalized dimensions
# - More joins
# - Less storage
# - Slower queries

# Data Vault:
# - Hub, Link, Satellite tables
# - Historical tracking
# - Complex but flexible
```

## FAQ

**Q: Should I normalize or denormalize?**
A: For analytical systems: denormalize (star schema). For transactional: normalize. Explain trade-offs.

**Q: How do I choose fact table granularity?**
A: Match to business events. One row per transaction, per order, per user action, etc.

**Q: What if requirements change?**
A: Design for flexibility. Use surrogate keys, version dimensions, or separate fact tables for new events.

**Q: How do I handle slowly-changing dimensions?**
A: Use SCD Type 2 (historical tracking) or Type 1 (overwrite). Explain choice based on business needs.

**Q: What partitioning strategy should I use?**
A: Partition by frequently filtered columns with low cardinality. Date columns are common choice.
