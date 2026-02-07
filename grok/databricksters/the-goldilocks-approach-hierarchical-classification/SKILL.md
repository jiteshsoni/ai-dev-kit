---
name: "hierarchical-classification-ai-query"
description: "Implement hierarchical classification using AI_QUERY in Databricks: two-stage approach for complex multi-level taxonomies, dynamic prompt construction, and evaluation strategies."
---

# Hierarchical Classification with AI_QUERY: The Goldilocks Approach

## Overview

This skill covers implementing hierarchical classification using AI_QUERY in Databricks SQL. Learn the "Goldilocks" approach that balances between single-shot classification (too complex) and individual level classification (too slow/expensive). Includes building hierarchy tables, two-stage classification (Level 1 standalone, Levels 2-4 dynamic), evaluation dataset creation, and cost optimization strategies.

## Quick Start

### Two-Stage Hierarchical Classification
Implement the Goldilocks approach:

```sql
-- Stage 1: Classify Level 1 (Domain) - Simple prompt with all options
CREATE OR REPLACE TABLE catalog.schema.call_transcripts_l1_predictions AS
SELECT
    call_id,
    transcript_text,
    AI_QUERY('databricks-meta-llama-3-3-70b-instruct',
        CONCAT(
            'Classify the transcript into one of the following categories:\n',
            '- Network & Connectivity\n',
            '- Billing & Payments\n',
            '- Account Management\n',
            '- Device & Equipment\n',
            '- Service Provisioning\n',
            '- Technical Support\n',
            '- Mobile Services\n',
            '- Internet Services\n',
            '- TV & Streaming\n',
            '- Voice Services\n',
            '- Security & Privacy\n',
            '- Sales & Orders\n\n',
            'Transcript: ', transcript_text, '\n\n',
            'Return only the category name.'
        )
    ) AS level_1_classification
FROM catalog.schema.call_transcripts_raw;

-- Stage 2: Classify Levels 2-4 - Dynamic prompt with filtered hierarchy
CREATE OR REPLACE TABLE catalog.schema.call_transcripts_all_classifications AS
SELECT 
    call_id,
    transcript_text,
    l1.level_1_classification,
    AI_QUERY('databricks-meta-llama-3-3-70b-instruct',
        CONCAT(
            'Classify the call transcript into Level 2, Level 3, and Level 4 subcategories. ',
            'Strictly adhere to the hierarchies as listed below.\n\n',
            'Transcript: ', l1.transcript_text, '\n',
            'Level 1: ', l1.level_1_classification, '\n\n',
            'Valid Level 2 options: ', hier.level_2_map, '\n\n',
            'Valid Level 3 options by Level 2: ', hier.level_3_map, '\n\n',
            'Valid Level 4 options by Level 3: ', hier.level_4_map, '\n\n',
            'Return ONLY valid JSON (no markdown): {"level_2": "X", "level_3": "Y", "level_4": "Z"}\n'
        )
    ) AS classification_json
FROM catalog.schema.call_transcripts_l1_predictions l1
INNER JOIN catalog.schema.transcript_classification_hierarchy_table hier
    ON l1.level_1_classification = hier.level_1;
```

### Build Hierarchy Table
Create the source of truth for valid classification paths:

```python
def create_hierarchy_table():
    """Create hierarchy table with all valid classification paths"""
    
    hierarchy_data = [
        {
            "level_1": "Billing & Payments",
            "level_2_map": "Auto-Pay, Invoice Issues, Payment Plans, Payment Processing, Refunds",
            "level_3_map": '{"Invoice Issues": ["Cannot Access Invoice", "Incorrect Charges", "Missing Credits"], "Payment Processing": ["Duplicate Payment", "Payment Declined", "Payment Not Posted"]}',
            "level_4_map": '{"Incorrect Charges": ["Double Billing", "Proration Error", "Service Not Ordered"], "Payment Declined": ["Bank Fraud Detection", "Card Expired", "Incorrect Card Information"]}'
        },
        {
            "level_1": "Network & Connectivity",
            "level_2_map": "Connection Issues, Speed Problems, Coverage Issues",
            "level_3_map": '{"Connection Issues": ["Cannot Connect", "Frequent Disconnects"], "Speed Problems": ["Slow Download", "Slow Upload"]}',
            "level_4_map": '{"Cannot Connect": ["Router Issue", "Modem Issue", "Cable Problem"], "Slow Download": ["Network Congestion", "Plan Limitations"]}'
        }
        # Add more domains...
    ]
    
    df = spark.createDataFrame(hierarchy_data)
    df.write.mode("overwrite").saveAsTable("catalog.schema.transcript_classification_hierarchy_table")
    
    print("Hierarchy table created with valid classification paths")
    return df

# Usage
hierarchy_df = create_hierarchy_table()
```

## Common Patterns

### Pattern 1: Build Evaluation Dataset
Create gold-standard evaluation data:

```python
def create_evaluation_dataset():
    """Create evaluation dataset with expert-labeled examples"""
    
    evaluation_data = [
        {
            "call_id": "CALL001",
            "transcript_text": "I can't access my invoice online and I was charged twice this month.",
            "level_1": "Billing & Payments",
            "level_2": "Invoice Issues",
            "level_3": "Cannot Access Invoice",
            "level_4": "Portal Login Issue"
        },
        {
            "call_id": "CALL002",
            "transcript_text": "My internet keeps disconnecting every few minutes.",
            "level_1": "Network & Connectivity",
            "level_2": "Connection Issues",
            "level_3": "Frequent Disconnects",
            "level_4": "Router Issue"
        }
        # Add 50-100 more examples covering major categories
    ]
    
    df = spark.createDataFrame(evaluation_data)
    df.write.mode("overwrite").saveAsTable("catalog.schema.evaluation_dataset")
    
    print(f"Evaluation dataset created with {len(evaluation_data)} examples")
    return df

# Usage
eval_df = create_evaluation_dataset()
```

### Pattern 2: Evaluate Classification Accuracy
Measure performance at each level:

```python
def evaluate_hierarchical_classification():
    """Evaluate classification accuracy at each level"""
    
    # Compare predictions with ground truth
    evaluation_query = """
    SELECT 
        e.call_id,
        e.level_1 as true_level_1,
        p.level_1_classification as pred_level_1,
        CASE WHEN e.level_1 = p.level_1_classification THEN 1 ELSE 0 END as l1_correct,
        
        e.level_2 as true_level_2,
        JSON_EXTRACT(p.classification_json, '$.level_2') as pred_level_2,
        CASE WHEN e.level_2 = JSON_EXTRACT(p.classification_json, '$.level_2') THEN 1 ELSE 0 END as l2_correct,
        
        e.level_3 as true_level_3,
        JSON_EXTRACT(p.classification_json, '$.level_3') as pred_level_3,
        CASE WHEN e.level_3 = JSON_EXTRACT(p.classification_json, '$.level_3') THEN 1 ELSE 0 END as l3_correct,
        
        e.level_4 as true_level_4,
        JSON_EXTRACT(p.classification_json, '$.level_4') as pred_level_4,
        CASE WHEN e.level_4 = JSON_EXTRACT(p.classification_json, '$.level_4') THEN 1 ELSE 0 END as l4_correct
        
    FROM catalog.schema.evaluation_dataset e
    INNER JOIN catalog.schema.call_transcripts_all_classifications p
        ON e.call_id = p.call_id
    """
    
    results = spark.sql(evaluation_query)
    
    # Calculate accuracy metrics
    accuracy_metrics = spark.sql("""
        SELECT 
            AVG(l1_correct) as level_1_accuracy,
            AVG(l2_correct) as level_2_accuracy,
            AVG(l3_correct) as level_3_accuracy,
            AVG(l4_correct) as level_4_accuracy,
            AVG(l1_correct * l2_correct * l3_correct * l4_correct) as full_path_accuracy
        FROM ({}) results
    """.format(evaluation_query))
    
    metrics = accuracy_metrics.collect()[0]
    
    print("Classification Accuracy Metrics:")
    print(f"  Level 1 (Domain): {metrics['level_1_accuracy']:.2%}")
    print(f"  Level 2 (Category): {metrics['level_2_accuracy']:.2%}")
    print(f"  Level 3 (Problem Type): {metrics['level_3_accuracy']:.2%}")
    print(f"  Level 4 (Root Cause): {metrics['level_4_accuracy']:.2%}")
    print(f"  Full Path Accuracy: {metrics['full_path_accuracy']:.2%}")
    
    return metrics

# Usage
metrics = evaluate_hierarchical_classification()
```

### Pattern 3: Cost Optimization
Minimize token costs through prompt optimization:

```python
def optimize_classification_costs():
    """Optimize token costs for hierarchical classification"""
    
    cost_optimizations = {
        "level_1_prompt": {
            "strategy": "Minimal descriptions",
            "example": "Just list category names, no descriptions",
            "token_savings": "~50% vs detailed prompts"
        },
        "level_2_4_prompt": {
            "strategy": "Dynamic hierarchy filtering",
            "example": "Only include valid paths for predicted Level 1",
            "token_savings": "~80% vs including all 900 paths"
        },
        "model_selection": {
            "strategy": "Use cheaper models for Level 1",
            "example": "Small model for L1, larger for L2-4",
            "cost_reduction": "30-50% overall"
        },
        "batch_processing": {
            "strategy": "Process in batches",
            "example": "Group similar transcripts together",
            "efficiency": "Better token utilization"
        }
    }
    
    print("Cost Optimization Strategies:")
    for strategy, details in cost_optimizations.items():
        print(f"\n{strategy.replace('_', ' ').title()}:")
        print(f"  Strategy: {details['strategy']}")
        print(f"  Example: {details['example']}")
        if 'token_savings' in details:
            print(f"  Savings: {details['token_savings']}")
        if 'cost_reduction' in details:
            print(f"  Reduction: {details['cost_reduction']}")
    
    return cost_optimizations

# Usage
optimizations = optimize_classification_costs()
```

### Pattern 4: Handle Classification Errors
Implement error handling and fallback logic:

```python
def classify_with_error_handling():
    """Classify with error handling and validation"""
    
    classification_query = """
    SELECT 
        call_id,
        transcript_text,
        level_1_classification,
        -- Validate JSON and extract levels
        CASE 
            WHEN classification_json IS NULL THEN NULL
            WHEN JSON_EXTRACT(classification_json, '$.level_2') IS NULL THEN NULL
            ELSE JSON_EXTRACT(classification_json, '$.level_2')
        END as level_2,
        CASE 
            WHEN classification_json IS NULL THEN NULL
            WHEN JSON_EXTRACT(classification_json, '$.level_3') IS NULL THEN NULL
            ELSE JSON_EXTRACT(classification_json, '$.level_3')
        END as level_3,
        CASE 
            WHEN classification_json IS NULL THEN NULL
            WHEN JSON_EXTRACT(classification_json, '$.level_4') IS NULL THEN NULL
            ELSE JSON_EXTRACT(classification_json, '$.level_4')
        END as level_4,
        -- Flag invalid classifications
        CASE 
            WHEN classification_json IS NULL THEN 'ERROR_NO_RESPONSE'
            WHEN JSON_EXTRACT(classification_json, '$.level_2') IS NULL THEN 'ERROR_INVALID_JSON'
            ELSE 'VALID'
        END as validation_status
    FROM catalog.schema.call_transcripts_all_classifications
    """
    
    results = spark.sql(classification_query)
    
    # Identify errors for review
    errors = spark.sql(f"""
        SELECT * FROM ({classification_query}) 
        WHERE validation_status != 'VALID'
    """)
    
    error_count = errors.count()
    print(f"Found {error_count} classification errors - review required")
    
    return results, errors

# Usage
results, errors = classify_with_error_handling()
```

## Reference Files

- [AI_QUERY Documentation](https://docs.databricks.com/en/sql/language-manual/functions/ai_query.html) - SQL function reference
- [Batch Inference](https://docs.databricks.com/en/machine-learning/ai-functions/ai-query.html) - Batch inference patterns

## Common Issues

| Issue | Solution |
|-------|----------|
| **Poor accuracy** | Use two-stage approach, reduce choices per stage |
| **High token costs** | Filter hierarchy dynamically, optimize prompts |
| **Invalid classifications** | Validate against hierarchy table, add error handling |
| **Slow processing** | Use appropriate model size, batch processing |
| **JSON parsing errors** | Add validation, handle malformed responses |

## Key Takeaways

1. **Goldilocks Approach**: Two queries (L1 standalone, L2-4 dynamic) balances complexity and cost
2. **Hierarchy Table**: Single source of truth for valid classification paths
3. **Evaluation Dataset**: 50-100 expert-labeled examples for continuous improvement
4. **Dynamic Filtering**: Only include valid options for predicted Level 1 (reduces from 900 to ~75 paths)
5. **Cost Optimization**: Minimal L1 prompts, dynamic L2-4 prompts, model selection
6. **Error Handling**: Validate JSON, check against hierarchy, flag invalid classifications

## Complete Workflow Example

```python
def complete_hierarchical_classification_workflow():
    """Complete workflow for hierarchical classification"""
    
    # Step 1: Create hierarchy table
    print("Step 1: Creating hierarchy table...")
    create_hierarchy_table()
    
    # Step 2: Create evaluation dataset
    print("Step 2: Creating evaluation dataset...")
    create_evaluation_dataset()
    
    # Step 3: Classify Level 1
    print("Step 3: Classifying Level 1 (Domain)...")
    spark.sql("""
        CREATE OR REPLACE TABLE catalog.schema.call_transcripts_l1_predictions AS
        SELECT
            call_id,
            transcript_text,
            AI_QUERY('databricks-meta-llama-3-3-70b-instruct',
                CONCAT(
                    'Classify the transcript into one of the following categories:\n',
                    '- Network & Connectivity\n',
                    '- Billing & Payments\n',
                    '- Account Management\n',
                    '- Device & Equipment\n',
                    '- Service Provisioning\n',
                    '- Technical Support\n',
                    '- Mobile Services\n',
                    '- Internet Services\n',
                    '- TV & Streaming\n',
                    '- Voice Services\n',
                    '- Security & Privacy\n',
                    '- Sales & Orders\n\n',
                    'Transcript: ', transcript_text, '\n\n',
                    'Return only the category name.'
                )
            ) AS level_1_classification
        FROM catalog.schema.call_transcripts_raw
    """)
    
    # Step 4: Classify Levels 2-4
    print("Step 4: Classifying Levels 2-4...")
    spark.sql("""
        CREATE OR REPLACE TABLE catalog.schema.call_transcripts_all_classifications AS
        SELECT 
            call_id,
            transcript_text,
            l1.level_1_classification,
            AI_QUERY('databricks-meta-llama-3-3-70b-instruct',
                CONCAT(
                    'Classify the call transcript into Level 2, Level 3, and Level 4 subcategories. ',
                    'Strictly adhere to the hierarchies as listed below.\n\n',
                    'Transcript: ', l1.transcript_text, '\n',
                    'Level 1: ', l1.level_1_classification, '\n\n',
                    'Valid Level 2 options: ', hier.level_2_map, '\n\n',
                    'Valid Level 3 options by Level 2: ', hier.level_3_map, '\n\n',
                    'Valid Level 4 options by Level 3: ', hier.level_4_map, '\n\n',
                    'Return ONLY valid JSON (no markdown): {"level_2": "X", "level_3": "Y", "level_4": "Z"}\n'
                )
            ) AS classification_json
        FROM catalog.schema.call_transcripts_l1_predictions l1
        INNER JOIN catalog.schema.transcript_classification_hierarchy_table hier
            ON l1.level_1_classification = hier.level_1
    """)
    
    # Step 5: Evaluate
    print("Step 5: Evaluating classification accuracy...")
    metrics = evaluate_hierarchical_classification()
    
    # Step 6: Handle errors
    print("Step 6: Identifying classification errors...")
    results, errors = classify_with_error_handling()
    
    print("\nWorkflow completed!")
    print(f"Level 1 Accuracy: {metrics['level_1_accuracy']:.2%}")
    print(f"Full Path Accuracy: {metrics['full_path_accuracy']:.2%}")
    print(f"Errors to review: {errors.count()}")
    
    return results, metrics

# Usage
results, metrics = complete_hierarchical_classification_workflow()
```

## When to Use This Skill

- Complex hierarchical classification (3+ levels)
- Historical data is untrustworthy or inconsistent
- Categories are flexible and change frequently
- Nuanced, context-dependent classifications
- Need to add categories without retraining
- 900+ possible classification paths

## Related Skills

- ai-query-batch-inference
- hierarchical-classification-patterns
- llm-classification-optimization
- prompt-engineering-strategies