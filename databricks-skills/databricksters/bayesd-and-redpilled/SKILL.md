---
name: bayesd-and-redpilled
description: 
---

# Bayes’d and Redpilled

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/bayesd-and-redpilled

## Tags
performance, databricks, ai, spark

## Full Content

# Bayes’d and Redpilled

**Source:** https://www.databricksters.com/p/bayesd-and-redpilled

**Blog:** Databricks

---



Bayes’d and Redpilled - by Austin - Databricksters

Databricksters

Subscribe

Sign in

Bayes’d and Redpilled

Markov Chain Monte Carlo Sampling using PyMC on Databricks

Austin

Aug 12, 2025

4

Share

Many people who work in statistics-adjacent occupations - looking at you BI analyst, DS, or ML eng - were visited early on in their careers by Laurence Fishburne offering them two options. Choose the frequentist pill and all your parameters will have fixed quantities, your p-values will give binary answers, and well supported libraries will abound with computationally cheap optimizations. Choose the probabilistic pill, and you’ll see just how much you’ve swept under the “assume everything is IID” rug.

Naturally we almost all picked the blue pill, which has left our braver coworkers with worse documentation and less seamless support for their libraries, including PyMC on Databricks. So today I’m going to offer up some time out of my happy little sklearn Bob Ross painting of a career to help our Bayesian friends get MCMC sampling working on Databricks.

https://xkcd.com/1132/

If you try to 

%pip

 install PyMC right now on Databricks Runtime 15.4 LTS, you’ll get an error, but if you 

%pip install 'miniKanren&lt;1.0.4'

 first, then it seems to work fine. Great, shortest blog ever.

Subscribe

Unfortunately, most commands will still not work depending on your compiler. For example, Databricks MLR has its own rabbit hole of compatibility issues with the Numba backend, which is needed to speed up the gradient computations and MCMC bottlenecks by JIT compiling the python into machine code. We can get around this by using a JAX backend, which proves much more reliable and performant in Databricks environments due to its native replacement of NumPy (

jax.numpy

), automatic gradients (just wrap 

jax.grad()

 around a function), and built in GPU support. It’s not perfect, but it’s serviceable.

Let’s look at an example. Imagine we own a beautiful forest full of tall trees and woodland creatures. Then we cut it down because trees only have value as timber or toilet paper and we’ve ground all the woodland interlopers into hotdogs. Much better, but we need to repeat this cycle as fast as possible to keep churning out the maximum profitability on our land. So we’ve hired some scientists to experiment with soil amendments that maximize tree growth and we observe the effects of the various treatments.

Our dataset looks something like this:

\(\begin{array}{|c|c|c|c|}
\hline
\text{treatment} &amp; \text{treatment_lot} &amp; \text{month} &amp; \text{tree_size_cm} \\
\hline
\text{control} &amp; \text{control_1} &amp; 1 &amp; 165.677522 \\
\text{control} &amp; \text{control_1} &amp; 2 &amp; 168.576281 \\
\text{control} &amp; \text{control_1} &amp; 3 &amp; 168.800478 \\
\text{control} &amp; \text{control_2} &amp; 1 &amp; 151.458208 \\
\hline
\end{array}\)

And we also have control_2, _3, and _4 for months 1, 2, and 3 and we have the same for the organic fertilizer group and the synthetic fertilizer group. So only 36 records in total, after all this is a toy dataset, not the LHC’s particle physics dataset.

Let’s read in that data now with the appropriate imports and then we’ll talk a little more about the model and sampling:

%pip install 'miniKanren&lt;1.0.4'
%pip install pymc
%pip install jax jaxlib blackjax
## Uncomment if you want to try this on GPU
# %pip install &quot;jax[cuda]&quot; -f https://storage.googleapis.com/jax-releases/jax_cuda_releases.html
%pip install nutpie
%pip install numpyro
%restart_python

We’re already at first potential hang-up; be sure to set 

PYTENSOR_FLAGS

 via environment variables BEFORE any other imports, as attempting to modify pytensor config post-initialization throws exceptions. Threading configuration through 

OMP_NUM_THREADS

 and 

OPENBLAS_NUM_THREADS

 also helps optimize BLAS (Basic Linear Algebra Subprograms) operations later on, so we’ll set those now too.

import os

# Set before any other imports
os.environ['OMP_NUM_THREADS'] = '4' # this cluster is 4 core
os.environ['OPENBLAS_NUM_THREADS'] = '4'
os.environ['PYTENSOR_FLAGS'] = 'mode=JAX,device=cpu,floatX=float32,openmp=True'

import pymc as pm
import numpy as np
import pandas as pd
import jax, jaxlib, blackjax
import nutpie
import pytensor

We can quickly validate that PyMC is working and verify the BLAS are working well by benchmarking them:

import pathlib, pytensor, sys

!{sys.executable} {pathlib.Path(pytensor.__file__).parent / 'misc/check_blas.py'}

Great, I’m getting pretty good results on an r6id.xlarge instance. Let’s also test the pytensor backend configuration:

pytensor.config.mode

This should print ‘JAX’.

We can now read in and view our sample data:

df_to_analyze = spark.sql(f&quot;SELECT * FROM &lt;catalog>.&lt;schema>.&lt;table>&quot;)
df_to_analyze = df_to_analyze.toPandas()
display(df_to_analyze)

Great, we’re now able to turn our attention to the actual model definition. I’m assuming you already know what model you want to use, because you’re the expert on your data and your use case, so going to describe the model below only briefly.

# Prepare data
df_to_analyze[&quot;treatment_lot_factor&quot;], treat_lot_vals = df_to_analyze.treatment_lot.factorize()
df_to_analyze[&quot;treatment_factor&quot;], treatment_vals = df_to_analyze.treatment.factorize()

coords = {
 &quot;treat_lots&quot;: treat_lot_vals,
 &quot;treatments&quot;: treatment_vals,
 &quot;param&quot;: [&quot;alpha&quot;, &quot;beta&quot;],
 &quot;obs_id&quot;: range(len(df_to_analyze))
}

with pm.Model(coords=coords) as simple_hierarchical_model:

 # Data containers
 month_idx = pm.Data(&quot;month_idx&quot;, df_to_analyze.month, dims=&quot;obs_id&quot;)
 treat = pm.Data(&quot;treat&quot;, df_to_analyze.treatment_factor, dims=&quot;obs_id&quot;)
 treat_lot_idx = pm.Data(&quot;treat_lot_idx&quot;, df_to_analyze.treatment_lot_factor, dims=&quot;obs_id&quot;)

 # Treatment-level priors (population means)
 alpha_mu = pm.Normal(&quot;alpha_mu&quot;, mu=6, sigma=0.5, dims=&quot;treatments&quot;)
 beta_mu = pm.Normal(&quot;beta_mu&quot;, mu=-0.1, sigma=0.02, dims=&quot;treatments&quot;)

 # LKJ correlation structure for lot-level random effects
 sd_lot = pm.Exponential.dist(4)
 chol, corr, stds = pm.LKJCholeskyCov(&quot;chol_lot&quot;, n=2, eta=2, sd_dist=sd_lot)

 # Lot-level random effects (centered parameterization)
 z = pm.Normal(&quot;z&quot;, 0.0, 1, dims=(&quot;param&quot;, &quot;treat_lots&quot;))
 lot_effects = pm.Deterministic(
 &quot;lot_effects&quot;,
 pt.dot(chol, z).T,
 dims=(&quot;treat_lots&quot;, &quot;param&quot;)
 )

 # Simple homoscedastic error
 sigma = pm.HalfNormal(&quot;sigma&quot;, sigma=0.25)

 # Expected value: population mean + lot-specific deviations
 y_hat = (
 alpha_mu[treat] + lot_effects[treat_lot_idx, 0] +
 (beta_mu[treat] + lot_effects[treat_lot_idx, 1]) * month_idx
 )

 # Likelihood, tree_growth_cm generated from normal distribution, with means and sigma specified above
 tree_growth = pm.Normal(
 &quot;tree_growth&quot;,
 mu=y_hat,
 sigma=sigma,
 observed=df_to_analyze.tree_size_cm,
 dims=&quot;obs_id&quot;
 )

This hierarchical Bayesian model helps us process more trees and fury freeloaders by analyzing tree growth over time across different treatment groups and treatment lots, capturing the nested structure of individual lots grouped within broader categories. The model estimates population-level growth patterns (intercept and slope over time) for each treatment, while allowing individual lots to deviate from these population means through correlated random effects. Lot-specific intercepts and slopes are modeled as correlated using an LKJ prior on the Cholesky decomposition - meaning lots that start larger than average for their treatment tend to also grow at a different rate. This approach provides uncertainty quantification at mu



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
