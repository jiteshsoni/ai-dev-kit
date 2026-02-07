---
name: its-beaver-time-dont-get-logged-down-with-mlflow-l
description: 
---

# It’s beaver time! Don’t get logged down with mlflow logging.

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/its-beaver-time-dont-get-logged-down

## Tags
ai, spark, performance, data-engineering, deltalake, databricks

## Full Content

# It’s beaver time! Don’t get logged down with mlflow logging.

**Source:** https://www.databricksters.com/p/its-beaver-time-dont-get-logged-down

---



It’s beaver time! Don’t get logged down with mlflow logging.

Databricksters

Subscribe

Sign in

AI &amp; ML

It’s beaver time! Don’t get logged down with mlflow logging.

A simple workaround for when you are training thousands of models and log_model() becomes your worst enemy.

Veena

Jul 22, 2025

2

Share

The beaver is building a dam. It is holding a log that says ‘mlflow.log_model()’.

Have you ever tried to train and log thousands of models with MLFlow? Instead of helping you track your runs, a seemingly innocuous line (

mlflow.log_model()

) has created a bottleneck and stretched your training time. Oh gosh, your training job takes hours now. What a nightmare.

What does mlflow.log_model() do?

Here is a brief overview of what log_model() does behind the scenes:

Serializes the model.

Infers dependencies and manages manually added dependencies to create a requirements file.

Creates model specific assets (like MLmodel).

Handles metadata and versioning.

All of these operations add a lot of overhead, and if you are looking to train thousands of small models within a single run, the logging overhead can often exceed your actual training time. This scenario is common in forecasting pipelines when you need separate models for different customer groups or time series segments.

Instead, log these models to a Delta Table

Let’s get a little creative. We can work around this issue by just not using log_model! We have solved the issue. You can stop reading now.

Just kidding. The idea is this-- we can get most of MLFlow’s benefits while improving performance by storing our models in a Delta Table.

Note

: This is code to demonstrate the concept. Before using this code in production, please implement proper error handling and performance testing. Take a look at the 

entire notebook here.

First, we need to create a Delta Table.

schema = StructType([
 StructField(&quot;group_id&quot;, StringType(), True),
 StructField(&quot;model_type&quot;, StringType(), True),
 StructField(&quot;model_version&quot;, StringType(), True), 
 StructField(&quot;model_binary&quot;, BinaryType(), True),
 StructField(&quot;run_id&quot;, StringType(), True),
 StructField(&quot;run_date&quot;, TimestampType(), True),
 StructField(&quot;mse&quot;, DoubleType(), True),
 StructField(&quot;forecast&quot;, ArrayType(DoubleType()), True),
 StructField(&quot;actual&quot;, ArrayType(DoubleType()), True),
 StructField(&quot;is_latest&quot;, StringType(), True) 
])

spark.createDataFrame([], schema).write.format(&quot;delta&quot;).option(&quot;overwriteSchema&quot;, &quot;true&quot;).saveAsTable(&quot;&lt;CATALOG>.&lt;SCHEMA>.mlflow_runs&quot;)

In this example, I use the same Delta Table to capture all of my models across different runs. I do this so I can implement a model_version column that will capture the latest version of each model. However, if you do not need this information, you can create a new Delta Table for each MLFlow run. Another thing to note is that I am using one type of model (a Random Forest regressor), but you can imagine a world where you have multiple different model types as well. This workaround is easily extendable to a lot of these scenarios.

When I train the model, I capture all of the information I need, like the predictions, actuals, metrics (here, I am using mean squared error). I am also capturing useful information like group_id, run_id, model_type to make it easy for me to search for this specific model after it is logged to the Delta Table. Finally, I have dumped the model binary in as well.

def train_model(group_df, group_id, latest_model_version, run_id, run_date): 
 ...
 # train the model as usual 
 ...

 # return metadata 
 return {
 'group_id': group_id,
 'model_type': 'RandomForestRegressor',
 'model_version': str(latest_model_version + 1),
 'model_binary': cloudpickle.dumps(model), 
 'run_id': run_id,
 'run_date': run_date,
 'mse': mse,
 'forecast': predictions.tolist(),
 'actual': y.tolist(),
 'is_latest': &quot;True&quot;
 }

After training for all models is complete, I can batch-insert all of the models into the Delta Table in one operation:

def save_to_delta(model_results, table_name): 
 df = spark.createDataFrame(model_results)
 df.write.format(&quot;delta&quot;).mode(&quot;append&quot;).saveAsTable(table_name)
 return 

...
# in the mlflow run
...

for group_id in GROUP_IDS: 
 group_df = data[data['group_id'] == group_id] 

 latest_version = current_model_versions.get(group_id, 0)
 model_result = train_model(group_df, group_id, latest_version, run_id, run_date)
 all_model_results.append(model_result)

save_to_delta(all_model_results, table_name)

Depending on how many models you are training, you can also update the Delta Table with the model information immediately after training.

Even though we are storing models in Delta Tables, we still want to maintain the link back to the MLFlow run for reproducibility. We want to log high-level information like, the number of models trained, the training dataset (assuming it's the same across groups), and the Delta Table used for logging. MLFlow also automatically logs other important information, like start-end times, success-failure statistics, and the source notebook version or git commit associated with the run. All of this is incredibly important for reproducibility and auditing. This is why we log the run_id with the model-- we want to be able to cross reference between the Delta Table and the MLFlow experiment to get the best of both worlds.

But if you recall the beginning of this post, you would remember there is still one thing left to cover: tracking dependencies. Without log_model(), MLFlow does not infer dependencies or create the MLModel artifacts, which contains important information such as the Python version used. After saving all of the models to a Delta Table, we can use log_model() once to create these artifacts. Now, we can save all of the required dependencies.

def log_models_to_mlflow(data, table_name=&quot;&lt;CATALOG>.&lt;SCHEMA>.mlflow_runs&quot;): 
 with mlflow.start_run() as run: 
 run_id = run.info.run_id
 run_date = datetime.now()

 # log high level parameters
 mlflow.log_param(&quot;num_groups&quot;, len(data['group_id'].unique()))
 mlflow.log_param(&quot;delta_table_name&quot;, table_name)

 current_model_versions = get_latest_model_versions(table_name) # {'group_id': version #}
 all_model_results = []

 for group_id in GROUP_IDS: 
 group_df = data[data['group_id'] == group_id] 

 latest_version = current_model_versions.get(group_id, 0)
 model_result = train_model(group_df, group_id, latest_version, run_id, run_date)
 all_model_results.append(model_result)

 save_to_delta(all_model_results, table_name)

 mlflow.pyfunc.log_model(
 &quot;dummy_model&quot;,
 input_example=main_df,
 extra_pip_requirements=[...], 
 python_model=DummyWrapper()
 )

You can also do this by logging a requirements.txt file directly via 

mlflow.log_artifact().

But how do we load these models?

It is quite straightforward. Since we have saved the model binaries in the Delta Table, we can directly load this and use it for inference. In this example, remember I have saved all of the models across runs, so I have multiple versions of each model in the same Delta Table. You can easily search and get specific versions of the model or simply retrieve the latest one.

class MultiModelWrapper(): 
 def __init__(self, table_name): 
 self.table = table_name

 def load_model_from_delta(self, group_id, table_name, model_type=None, run_id=None, version=None): 
 query = f&quot;select * from {table_name} where group_id = '{group_id}'&quot;
 if model_type: 
 query += f&quot; and model_type = '{model_type}'&quot;
 if run_id: 
 query += f&quot; and run_id = '{run_id}'&quot;
 if version: 
 query += f&quot; and model_version = '{version}'&quot;
 el



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
