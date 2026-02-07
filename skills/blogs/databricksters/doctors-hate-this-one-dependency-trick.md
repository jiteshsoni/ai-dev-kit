---
name: doctors-hate-this-one-dependency-trick
description: 
---

# Doctors HATE this one dependency trick!

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/doctors-hate-this-one-dependency

## Tags
mlops, data-engineering, ai, databricks

## Full Content

# Doctors HATE this one dependency trick!

**Source:** https://www.databricksters.com/p/doctors-hate-this-one-dependency

**Blog:** Databricks

---



Doctors HATE this one dependency trick! - by Veena

Databricksters

Subscribe

Sign in

AI &amp; ML

Doctors HATE this one dependency trick!

A quick guide to dependency management for machine learning using MLflow 3+. 

Veena

Aug 19, 2025

7

1

Share

Life was promised to be simple. You log your model and then you deploy the model. This is supposed to be easy! But even if everything works perfectly in your notebook, once you deploy it, dependency issues that you have never seen before pop up. 

Image showing what direct dependencies and transitive dependencies are. Direct dependencies are explicitly used in the project, and transitive dependencies are included as they are the dependencies of the direct dependencies.

Most dependency resolution issues are because of conflicting transitive dependencies. Here is what most data scientists are doing today: 

Experiment in notebooks, adding new libraries via pip within a notebook. 

Log models using 

mlflow

 . 

Register a model to UC and deploy. 

Cross your fingers and hope everything works. 

[optional] Get frustrated with Model Serving. 

mlflow

 only infers the direct dependencies when you log a model. It identifies them by examining the flavor and the packages used in the model’s predict function. For more information, check out 

mlflow.models.infer_pip_requirements()

. 

Usually, this means that dependencies are only resolved during Model Serving, which means dependency errors are not caught until then. Waiting for the Serverless compute and the container to build will only extend the developer loop, making it harder to iterate. 

Image showing that the requirements.txt file in the Registered Model artifacts are used to install all dependencies in the Model Serving environment. 

Lock your dependencies during development

In 

mlflow

 3+, you can enable dependency locking with 

uv

, which would allow you to use the standard 

mlflow

 logging workflow. 

import os
os.environ[&quot;MLFLOW_LOCK_MODEL_DEPENDENCIES&quot;] = &quot;true&quot;

# Now when you log your model, MLflow will capture 
# both direct AND transitive dependencies

mlflow.sklearn.log_model(
 model, 
 &quot;my_model&quot;,
)

Your workflow does not have to change at all. You can still use 

extra_pip_requirements

, 

pip_requirements

, or allow 

mlflow

 to infer all direct dependencies. The environment variable now enables 

uv

 to resolve dependencies during logging time and will capture pinned direct and transitive dependencies. Now, your 

requirements.txt 

file will contain all the dependencies you need. 

Image comparing the requirements.txt file artifact using dependency locking vs not using dependency locking. 

Dependency resolution occurs during model logging time instead of serving time, and we have automatic dependency locking when logging a model. 

However, since 

uv

 resolves all of the dependencies, the transitive dependencies captured are often more recent than the packages installed by default on the DBR. So, we will usually get ‘warnings’ that the dependencies captured by 

uv

 are different from the transitive dependencies in the environment. Why is this a problem? Our training environment and serving environment are still different, which means we could still get behavior differences between our notebook and our deployment. 

Example of a warning after using dependency locking: “Detected one or more mismatches between the model’s dependencies and the current Python environment: - cloudpickle (current: 2.2.1, required: cloudpickle==3.1.1)”

In the above example, we can see that 

cloudpickle

 often resolves to version 3.1.1, but in our recent DBRs, 

cloudpickle

 version 2.2.1 is installed. This is especially important because 

cloudpickle

 will always be a transitive dependency as 

mlflow

 relies on it. 

Using Databricks Asset Bundles

We can resolve the inconsistency between the notebook environment and the serving environment using Databricks Asset Bundles. If we have a `dev` workspace and a `test` workspace, then we can use 

mlflow

 and 

uv

 to generate the requirements lock file in the `dev` workspace and add the requirements lock file as a dependency for the `test` workspace. 

This can all be easily orchestrated using Databricks Asset Bundles and Lakeflow Jobs. By installing the requirements lock file, we can override any conflicting transitive dependencies in the DBR and ensure that the training and serving environments are the exact same. Here is an example job config: 

resources:
 jobs:
 my_job:
 tasks:
 - task_key: train_model
 libraries:
 - requirements: ./requirements.txt # Pre-resolved dependencies

Now the entire pipeline uses the same dependencies from development through production. 

Dependency management is important! 

Dependency management is probably not the most exciting part of MLOps but it can easily become a migraine. The few extra minutes you spend considering dependency management will save you lots of time debugging serving deployment failures. 

As always, let us know if you have any questions! 

7

1

Share

Previous

Next

Discussion about this post

Comments

Restacks

Joshua Eason

Aug 19

Author

Excellent stuff! This is a huge unlock for any team that requires absolute control over their dependencies in their serving environment. 

Reply

Share

Top

Latest

Discussions

No posts

Ready for more?

Subscribe

© 2026 Soni

 · 

Privacy

 ∙ 

Terms

 ∙ 

Collection notice

 Start your Substack

Get the app

Substack
 is the home for great culture

 This site requires JavaScript to run correctly. Please 
turn on JavaScript
 or unblock scripts





---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
