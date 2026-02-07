---
name: juggling-multiple-models-in-a-single-serving-endpo
description: 
---

# Juggling multiple models in a single serving endpoint

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/juggling-a-model-circus-a-pyfuncs

## Tags
mlops, ai, databricks

## Full Content

# Juggling multiple models in a single serving endpoint

**Source:** https://www.databricksters.com/p/juggling-a-model-circus-a-pyfuncs

---



Juggling multiple models in a single serving endpoint

Databricksters

Subscribe

Sign in

AI &amp; ML

Juggling multiple models in a single serving endpoint

How to serve multiple models on a single model serving endpoint in Databricks using pyfunc

Veena

 and 

Debu Sinha

Apr 29, 2025

3

Share

Have you ever found yourself juggling multiple ML models? Imagine this: you're maintaining a prediction service that started with a single model, but now you've got a dozen micro-models serving different business needs. Costs are climbing. You are dreaming of consolidation.

For most scenarios, Databricks Model Serving provides an easy solution. They allow you to deploy multiple models behind a single endpoint, split traffic, and route requests. This approach is perfect for A/B testing and canary deployments, where simple traffic splitting is sufficient. However, there are situations where we can hit limitations: 

routing based on requests (e.g., user attributes) 

routing based on time

managing dozens of micro-models and want to consolidate infrastructure

routing dynamically based on business rules

You could spin up separate endpoints for each, but that means more DBUs, more management overhead, etc. This is where creating a custom PyFunc wrapper can provide a solution. Note that this should be viewed as an edge case and not a default pattern. 

Before diving into the implementation, let’s consider the limitations. 

Individual model metrics are combined, so monitoring is more difficult.

Models are loaded together, so there could be a resource inefficiency.

Routing rules may obscure decision paths. 

Model versioning is less transparent. 

In this deep dive, we will explore a really simple pattern to help solve this issue using PyFunc. By creating a wrapper with PyFunc, we will package various models in one deployable artifact, implement routing logic to direct requests to the right model, and maintain an entry point. 

An diagram of how this router solution would look. 

Let’s quickly create some base models. 

We are training two separate models using the same California Housing dataset. Because both models have the exact same input data schema and the expected output schema, we can expect that the Model Signature for both models will be the same. 

import mlflow
import pandas as pd
import numpy as np
from sklearn.datasets import fetch_california_housing
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor

data = fetch_california_housing()
california_housing = pd.DataFrame(data.data, columns=data.feature_names)
california_housing['target'] = data.target

X_train, X_test, y_train, y_test = train_test_split(
 california_housing.drop('target', axis=1), 
 california_housing['target'], 
 test_size=0.2, 
 random_state=42
)

lr_model = LinearRegression().fit(X_train, y_train)
rf_model = RandomForestRegressor().fit(X_train, y_train)

signature = mlflow.models.infer_signature(X_train, lr_model.predict(X_train))

This represents a standard model development workflow. This pattern builds on existing models and training processes rather than replacing them. In other words, you can adopt this pattern without too much disruption to your current workflows. 

These can now be logged and registered in Unity Catalog. Nothing new here! 

with mlflow.start_run(run_name=&quot;California Housing Models&quot;) as housing_run:
 mlflow.sklearn.log_model(lr_model, &quot;linear_regression_model&quot;, signature=signature)
 mlflow.sklearn.log_model(rf_model, &quot;random_forest_model&quot;, signature=signature)

 mlflow.set_registry_uri(&quot;databricks-uc&quot;)
 mlflow.register_model(
 f&quot;runs:/{housing_run.info.run_id}/linear_regression_model&quot;, 
 &quot;your_catalog.your_schema.california_housing_linear_regression&quot;
 )
 mlflow.register_model(
 f&quot;runs:/{housing_run.info.run_id}/random_forest_model&quot;, 
 &quot;your_catalog.your_schema.california_housing_random_forest&quot;
 )

Create a custom model using pyfunc. 

We are going to use 

pyfunc

 to orchestrate and serve as the main interface for interacting with the base models. The wrapper will load our models and dynamically select which model to use based on the request parameters. 

class ModelRouter(mlflow.pyfunc.PythonModel):
 def load_context(self, context):
 self.linear_model = mlflow.sklearn.load_model(
 context.artifacts[&quot;linear_regression_model&quot;]
 )
 self.forest_model = mlflow.sklearn.load_model(
 context.artifacts[&quot;random_forest_model&quot;]
 )

 def predict(self, context, model_input):
 # The 'model' column specifies which model to use
 if model_input['model'].eq('RandomForest').any():
 return {
 &quot;prediction&quot;: self.forest_model.predict(model_input.drop('model', axis=1))
 }
 elif model_input['model'].eq('LinearRegression').any():
 return {
 &quot;prediction&quot;: self.linear_model.predict(model_input.drop('model', axis=1))
 }
 else:
 raise ValueError(&quot;Unrecognized model type. Use 'RandomForest' or 'LinearRegression'&quot;)

I want to highlight two important aspects of this wrapper. First, in 

load_context

, we are loading the underlying Linear Regression and Random Forest models from the artifacts. When we log and register this wrapper, we will need to specify these artifacts, so that the wrapper will correctly load the models that we trained. Keep in mind that in the model serving environment, 

load_context

 is called once, so loading the models should not affect the serving latency after initialization. 

Second, there is a lot of flexibility here. In the code snippet, we are using an extra column in the model input called 

model

 to select which model to use. But you can implement virtually any routing logic. You can switch between the models based on geographic location or the time the request was submitted.

Registering the wrapper with the model artifacts. 

In order to register the model, we need to create a proper Model Signature. I am going to use the 

infer_signature

 function to do so. You can also manually construct the signature object. The signature will be similar to the signatures used for the base models. Because our wrapper uses an extra column to decide which model to use, we need to take that into consideration. 

input_example = X_train.copy()
input_example['model'] = 'RandomForest'

router_signature = mlflow.models.infer_signature(
 input_example, 
 {&quot;prediction&quot;: rf_model.predict(X_train)}
)

When we log the model, we need to include the base models as artifacts: 

with mlflow.start_run() as run:
 router_model = ModelRouter()
 mlflow.pyfunc.log_model(
 &quot;model_router&quot;,
 python_model=router_model,
 signature=router_signature,
 artifacts={
 &quot;linear_regression_model&quot;: 
 &quot;models:/your_catalog.your_schema.california_housing_linear_regression/1&quot;,
 &quot;random_forest_model&quot;: 
 &quot;models:/your_catalog.your_schema.california_housing_random_forest/1&quot;,
 },
 extra_pip_requirements=[&quot;scikit-learn==1.4.2&quot;, &quot;numpy==1.23.5&quot;, &quot;pandas==1.5.3&quot;]
 )

 # Register the router model
 mlflow.register_model(
 f&quot;runs:/{run.info.run_id}/model_router&quot;, 
 &quot;your_catalog.your_schema.housing_model_router&quot;
 )

Now, we have created a self-contained wrapper that includes everything needed for serving. 

What happens when we are dealing with different inputs?

Imagine your system spans multiple domains. Different data, different tasks, but you still need a unified interface. 

First, let’s train model on a different dataset. 

from sklearn.datasets import load_breast_cancer
from sklearn.ensemble import RandomForestClassifier

cancer_data = load_breast_cancer()
cancer_df = pd.DataFrame(cancer_data.data, columns=cancer_data.fe



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
