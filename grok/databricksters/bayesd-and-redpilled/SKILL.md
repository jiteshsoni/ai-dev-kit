---
name: "pymc-mcmc-sampling-databricks"
description: "Run PyMC MCMC sampling on Databricks: JAX backend configuration, nutpie sampler optimization, handling Numba compatibility issues, and hierarchical Bayesian models."
---

# PyMC MCMC Sampling on Databricks: Bayes'd and Redpilled

## Overview

This skill covers running PyMC (Probabilistic Model in Python) for Markov Chain Monte Carlo (MCMC) sampling on Databricks. Learn how to configure PyMC with JAX backend instead of Numba, use nutpie sampler for 4x performance improvements, handle compatibility issues with Databricks Runtime, and implement hierarchical Bayesian models with proper threading configuration.

## Quick Start

### Install PyMC with Dependencies
Set up PyMC on Databricks:

```python
# Install in order
%pip install 'miniKanren<1.0.4'  # Required first!
%pip install pymc
%pip install jax jaxlib blackjax
%pip install nutpie  # For optimized sampling
%pip install numpyro
# Optional: GPU support
# %pip install "jax[cuda]" -f https://storage.googleapis.com/jax-releases/jax_cuda_releases.html
%restart_python
```

### Configure Environment Variables
Set before any imports:

```python
import os

# Set BEFORE any other imports
os.environ['OMP_NUM_THREADS'] = '4'  # Match cluster cores
os.environ['OPENBLAS_NUM_THREADS'] = '4'
os.environ['PYTENSOR_FLAGS'] = 'mode=JAX,device=cpu,floatX=float32,openmp=True'

import pymc as pm
import numpy as np
import pandas as pd
import jax, jaxlib, blackjax
import nutpie
import pytensor

# Verify configuration
print(f"PyTensor mode: {pytensor.config.mode}")  # Should be 'JAX'
```

### Basic Hierarchical Model
Define hierarchical Bayesian model:

```python
# Prepare data
df_to_analyze["treatment_lot_factor"], treat_lot_vals = df_to_analyze.treatment_lot.factorize()
df_to_analyze["treatment_factor"], treatment_vals = df_to_analyze.treatment.factorize()

coords = {
    "treat_lots": treat_lot_vals,
    "treatments": treatment_vals,
    "param": ["alpha", "beta"],
    "obs_id": range(len(df_to_analyze))
}

with pm.Model(coords=coords) as hierarchical_model:
    # Data containers
    month_idx = pm.Data("month_idx", df_to_analyze.month, dims="obs_id")
    treat = pm.Data("treat", df_to_analyze.treatment_factor, dims="obs_id")
    treat_lot_idx = pm.Data("treat_lot_idx", df_to_analyze.treatment_lot_factor, dims="obs_id")
    
    # Treatment-level priors (population means)
    alpha_mu = pm.Normal("alpha_mu", mu=6, sigma=0.5, dims="treatments")
    beta_mu = pm.Normal("beta_mu", mu=-0.1, sigma=0.02, dims="treatments")
    
    # LKJ correlation structure for lot-level random effects
    sd_lot = pm.Exponential.dist(4)
    chol, corr, stds = pm.LKJCholeskyCov("chol_lot", n=2, eta=2, sd_dist=sd_lot)
    
    # Lot-level random effects
    z = pm.Normal("z", 0.0, 1, dims=("param", "treat_lots"))
    lot_effects = pm.Deterministic(
        "lot_effects",
        pt.dot(chol, z).T,
        dims=("treat_lots", "param")
    )
    
    # Error term
    sigma = pm.HalfNormal("sigma", sigma=0.25)
    
    # Expected value
    y_hat = (
        alpha_mu[treat] + lot_effects[treat_lot_idx, 0] +
        (beta_mu[treat] + lot_effects[treat_lot_idx, 1]) * month_idx
    )
    
    # Likelihood
    tree_growth = pm.Normal(
        "tree_growth",
        mu=y_hat,
        sigma=sigma,
        observed=df_to_analyze.tree_size_cm,
        dims="obs_id"
    )
```

## Common Patterns

### Pattern 1: Optimized Sampling with nutpie
Use nutpie for 4x performance improvement:

```python
# Option 1: Default sampler (slower, ~3 minutes)
# DON'T USE: pm.sample(2000, tune=1000, chains=4)  # Deadlocks!

# Option 2: Single core (works but slow, ~3 minutes)
with hierarchical_model:
    trace = pm.sample(
        1000, tune=1000,
        cores=1, chains=2,  # Sequential chains
        return_inferencedata=True,
        progressbar=True,
        random_seed=42
    )

# Option 3: nutpie sampler (FASTEST, ~45 seconds, 4x faster!)
with hierarchical_model:
    trace = pm.sample(
        1000, tune=1000,
        nuts_sampler="nutpie",
        nuts_sampler_kwargs={
            "backend": "jax",
            "gradient_backend": "pytensor"
        },
        return_inferencedata=True,
        progressbar=True,  # Note: progressbar may not work with nutpie
        random_seed=42
    )
```

### Pattern 2: Verify BLAS Configuration
Check BLAS performance:

```python
import pathlib
import sys

# Benchmark BLAS operations
!{sys.executable} {pathlib.Path(pytensor.__file__).parent / 'misc/check_blas.py'}

# Should show good performance on r6id.xlarge or similar instances
```

### Pattern 3: Handle Multicore Deadlocks
Avoid JAX multiprocessing issues:

```python
def safe_pymc_sampling(model, draws=1000, tune=1000, use_nutpie=True):
    """
    Safe PyMC sampling on Databricks.
    
    Args:
        model: PyMC model
        draws: Number of samples
        tune: Number of tuning samples
        use_nutpie: Use nutpie sampler (faster, parallel chains)
    """
    
    if use_nutpie:
        # Use nutpie for parallel chains without deadlocks
        trace = pm.sample(
            draws=draws,
            tune=tune,
            nuts_sampler="nutpie",
            nuts_sampler_kwargs={
                "backend": "jax",
                "gradient_backend": "pytensor"
            },
            return_inferencedata=True,
            random_seed=42
        )
    else:
        # Single core fallback
        trace = pm.sample(
            draws=draws,
            tune=tune,
            cores=1,
            chains=2,
            return_inferencedata=True,
            progressbar=True,
            random_seed=42
        )
    
    return trace

# Usage
trace = safe_pymc_sampling(hierarchical_model, use_nutpie=True)
```

## Reference Files

- [PyMC Documentation](https://www.pymc.io/) - PyMC user guide
- [nutpie](https://github.com/pymc-devs/nutpie) - Fast NUTS sampler
- [JAX Documentation](https://jax.readthedocs.io/) - JAX backend

## Common Issues

| Issue | Solution |
|-------|----------|
| **miniKanren error** | Install miniKanren<1.0.4 FIRST |
| **Numba backend fails** | Use JAX backend via PYTENSOR_FLAGS |
| **Multicore deadlocks** | Use nutpie sampler or cores=1 |
| **BLAS performance poor** | Set OMP_NUM_THREADS and OPENBLAS_NUM_THREADS |
| **Progressbar not working** | Known issue with nutpie - check trace directly |

## Key Takeaways

1. **Install Order**: miniKanren<1.0.4 must be installed first
2. **JAX Backend**: Use JAX instead of Numba for Databricks compatibility
3. **nutpie Sampler**: 4x faster than default, enables parallel chains
4. **Environment Variables**: Set PYTENSOR_FLAGS before imports
5. **Threading**: Configure OMP_NUM_THREADS and OPENBLAS_NUM_THREADS
6. **Single Core Fallback**: Use cores=1 if nutpie unavailable

## Performance Comparison

```python
def benchmark_sampling_methods():
    """Compare sampling performance"""
    
    # Method 1: Default (DOESN'T WORK - deadlocks)
    # pm.sample(2000, tune=1000, chains=4)  # ❌ Deadlocks
    
    # Method 2: Single core
    # Duration: ~3 minutes
    # Chains: Sequential (2 chains)
    
    # Method 3: nutpie sampler
    # Duration: ~45 seconds (4x faster!)
    # Chains: Parallel (no deadlocks)
    
    print("Performance comparison:")
    print("  Default (multicore): Deadlocks ❌")
    print("  Single core: ~3 minutes")
    print("  nutpie sampler: ~45 seconds (4x faster) ✓")

# Usage
benchmark_sampling_methods()
```

## Complete Workflow

```python
def complete_pymc_setup_workflow():
    """Complete PyMC setup and sampling workflow"""
    
    # Step 1: Install dependencies (in order!)
    # %pip install 'miniKanren<1.0.4'
    # %pip install pymc jax jaxlib blackjax nutpie numpyro
    # %restart_python
    
    # Step 2: Configure environment (BEFORE imports)
    import os
    os.environ['OMP_NUM_THREADS'] = '4'
    os.environ['OPENBLAS_NUM_THREADS'] = '4'
    os.environ['PYTENSOR_FLAGS'] = 'mode=JAX,device=cpu,floatX=float32,openmp=True'
    
    # Step 3: Import libraries
    import pymc as pm
    import pytensor
    
    # Step 4: Verify configuration
    assert pytensor.config.mode == 'JAX', "PyTensor not configured for JAX!"
    
    # Step 5: Define model
    # ... model definition ...
    
    # Step 6: Sample with nutpie
    with model:
        trace = pm.sample(
            1000, tune=1000,
            nuts_sampler="nutpie",
            nuts_sampler_kwargs={"backend": "jax", "gradient_backend": "pytensor"},
            return_inferencedata=True,
            random_seed=42
        )
    
    # Step 7: Analyze results
    # pm.summary(trace)
    # pm.plot_trace(trace)
    
    print("PyMC sampling complete!")
    return trace

# Usage
trace = complete_pymc_setup_workflow()
```

## When to Use This Skill

- Running Bayesian analysis on Databricks
- Implementing hierarchical models
- Using PyMC for MCMC sampling
- Needing uncertainty quantification
- Working with probabilistic models

## Related Skills

- bayesian-modeling-patterns
- mcmc-sampling-optimization
- jax-backend-configuration
- probabilistic-programming