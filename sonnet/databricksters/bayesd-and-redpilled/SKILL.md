---
name: "Bayesian Statistical Approaches in Data Analysis"
description: "Apply Bayesian methods for A/B testing, uncertainty quantification, and probabilistic decision-making in data engineering."
author: "Databricksters"
url: "https://www.databricksters.com/p/bayesd-and-redpilled"
date: "2025"
tags: ["statistics", "bayesian", "ab-testing", "data-science", "databricks"]
---

# Bayesian Statistical Approaches

## Overview

Bayesian statistics provides probabilistic framework for decision-making under uncertainty. Applications: A/B testing with early stopping, prior knowledge incorporation, and uncertainty quantification. Particularly useful when frequentist approaches require impractical sample sizes or when prior information is available.

**Use this skill when:** Running A/B tests with limited data, incorporating domain knowledge, or quantifying uncertainty in predictions.

## Quick Start

```python
import numpy as np
from scipy import stats

# Bayesian A/B test
def bayesian_ab_test(conversions_a, trials_a, conversions_b, trials_b):
    """
    Compare two conversion rates using Bayesian approach
    Returns probability that B > A
    """
    # Beta distribution (conjugate prior for Bernoulli)
    alpha_a = conversions_a + 1  # Prior: Beta(1,1) = Uniform
    beta_a = trials_a - conversions_a + 1
    
    alpha_b = conversions_b + 1
    beta_b = trials_b - conversions_b + 1
    
    # Sample from posterior distributions
    samples_a = np.random.beta(alpha_a, beta_a, 100000)
    samples_b = np.random.beta(alpha_b, beta_b, 100000)
    
    # Probability B > A
    prob_b_better = (samples_b > samples_a).mean()
    
    return prob_b_better

# Example: A/B test on Databricks
conversions_a, trials_a = 120, 1000
conversions_b, trials_b = 145, 1000

prob = bayesian_ab_test(conversions_a, trials_a, conversions_b, trials_b)
print(f"Probability B is better: {prob:.2%}")
```

## Common Patterns

### Pattern 1: Bayesian A/B Test with Spark

```python
from pyspark.sql import functions as F

# Aggregate test results
ab_results = spark.sql("""
    SELECT 
        variant,
        COUNT(*) as trials,
        SUM(converted) as conversions
    FROM ab_test_results
    WHERE test_date >= CURRENT_DATE() - INTERVAL 7 DAYS
    GROUP BY variant
""").toPandas()

# Apply Bayesian analysis
prob = bayesian_ab_test(
    ab_results.loc[ab_results.variant == 'A', 'conversions'].values[0],
    ab_results.loc[ab_results.variant == 'A', 'trials'].values[0],
    ab_results.loc[ab_results.variant == 'B', 'conversions'].values[0],
    ab_results.loc[ab_results.variant == 'B', 'trials'].values[0]
)
```

### Pattern 2: Incorporating Prior Knowledge

```python
def bayesian_ab_test_with_prior(conv_a, trials_a, conv_b, trials_b, 
                                 prior_mean=0.1, prior_strength=10):
    """
    Incorporate prior knowledge about conversion rates
    prior_strength: How confident in prior (higher = stronger)
    """
    # Convert prior to Beta parameters
    prior_alpha = prior_mean * prior_strength
    prior_beta = (1 - prior_mean) * prior_strength
    
    # Posterior with prior
    alpha_a = conv_a + prior_alpha
    beta_a = (trials_a - conv_a) + prior_beta
    
    alpha_b = conv_b + prior_alpha
    beta_b = (trials_b - conv_b) + prior_beta
    
    # Sample and compare
    samples_a = np.random.beta(alpha_a, beta_a, 100000)
    samples_b = np.random.beta(alpha_b, beta_b, 100000)
    
    return (samples_b > samples_a).mean()
```

## FAQ

**Q: When to use Bayesian vs frequentist A/B tests?**  
A: Bayesian when you have prior information, need early stopping, or want probability statements.

**Q: What's a good stopping rule?**  
A: Stop when P(B > A) > 0.95 or P(A > B) > 0.95. Or when expected loss is small.

**Q: How to choose priors?**  
A: Use historical data or weak priors (Beta(1,1) = uniform). Strong priors require domain expertise.
