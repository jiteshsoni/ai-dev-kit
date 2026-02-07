---
name: pyfunc-it-wex27ll-do-it-live-by-austin-databrickst
description: 
---

# PyFunc it! We&#x27;ll do it Live! - by Austin - Databricksters

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/pyfunc-it-well-do-it-live

## Tags
streaming, mlops, joins, ai, data-engineering, databricks

## Full Content

# PyFunc it! We&#x27;ll do it Live! - by Austin - Databricksters

**Source:** https://www.databricksters.com/p/pyfunc-it-well-do-it-live

---



PyFunc it! We&#x27;ll do it Live! - by Austin - Databricksters

Databricksters

Subscribe

Sign in

AI &amp; ML

PyFunc it! We'll do it Live! 

Real-Time Data Preprocessing for Custom Databricks Model Serving Endpoints

Austin

Apr 22, 2025

8

Share

When performing real time inference, you rarely get all of the data needed to make your prediction exactly how your model requires it inside the 

POST

 request. More commonly, one or both of the following are true:

The data received requires significant preprocessing in the form of parsing, encoding, reformatting, etc.

The data received is incomplete and must be combined with another data set in order to perform accurate predictions

Today’s blog will focus on the first use case, and we will revisit the second one next quarter. I originally wrote Part 2 using 

Online Tables

, which is still a possibility, but there have been some API changes I want to make sure settle before publishing. 

Thanks for reading Databricksters! Subscribe for free to receive new posts and support my work.

Subscribe

Bill gets frustrated with pre-processing pipelines

The Power of Pipelines

If we have an 

sklearn

 model, adding preprocessing steps - even more advanced custom preprocessing classes - is a straightforward task:

import mlflow
import mlflow.sklearn
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.datasets import load_iris

# Yes this dataset is simplistic, but we're just proving a point right now
iris = load_iris()
X = iris.data
y = iris.target

# Create the pipeline with whatever preprocessing you may need, Pipeline also accepts custom classes
pipeline = Pipeline([
 ('scaler', StandardScaler()),
 ('model', LogisticRegression())
])

# Start the MLflow run with autologging for even more quality of life features
with mlflow.start_run():
 mlflow.sklearn.autolog()
 pipeline.fit(X, y)

mlflow.end_run()

Not exactly groundbreaking code here. But what if our custom parsing and preprocessing logic was 

really

 complex and we wanted to maintain our own separate python scripts for this logic to maintain modularity? What if we wanted to use a model type that doesn't fit nicely into 

sklearn

? Regardless of the specific motivation, the time may come when this pattern will no longer serve our needs. Calling a separate 

.py

 file from within a 

PyFunc

 Python model and serving the custom pipeline is an extremely powerful and flexible pattern for multipart inference pipelines. 

Note:

 You can still use an 

sklearn

 Pipeline for this without using the 

mlflow.sklearn

 flavor, because 

XGBoost

 provides an 

sklearn

 compatible API. I've run the code below both ways, and while you don't have to use a single 

sklearn

 package in order to leverage this pattern, it will make for a simpler to follow demo.

Defining a Custom Preprocessing Script

The below 

.py

 file contains two relatively simple preprocessing classes, one that flattens nested JSON strings and one that extracts the domain from email addresses.

## custom_transformers.py
## We could break these out, but for simplicity of the demo, I'm just making one external .py file
from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd

class JSONFlattener(BaseEstimator, TransformerMixin):
 &quot;&quot;&quot;
 Transforms the DataFrame by flattening the specified JSON column into a tabular format.
 &quot;&quot;&quot;
 def __init__(self, json_column, record_prefix=''):
 self.json_column = json_column
 self.record_prefix = record_prefix

 def fit(self, X, y=None):
 return self

 def flatten_dict(self, d, parent_key='', sep='.'):
 items = []
 for k, v in d.items():
 new_key = f&quot;{parent_key}{sep}{k}&quot; if parent_key else k
 if isinstance(v, dict):
 items.extend(self.flatten_dict(v, new_key, sep=sep).items())
 else:
 if isinstance(v, list):
 v = ';'.join(map(str, v))
 items.append((new_key, v))
 return dict(items)

 def transform(self, X):
 X = X.copy()
 flattened = X[self.json_column].apply(lambda x: self.flatten_dict(x, self.record_prefix, sep='.'))
 json_df = pd.DataFrame(flattened.tolist())
 X = X.drop(columns=[self.json_column])
 X = pd.concat([X.reset_index(drop=True), json_df.reset_index(drop=True)], axis=1)
 return X

class EmailDomainExtractor(BaseEstimator, TransformerMixin):
 &quot;&quot;&quot;
 Transforms the DataFrame by adding a new column 'email_domain' containing the extracted domains.
 &quot;&quot;&quot;
 def __init__(self, email_column):
 self.email_column = email_column

 def fit(self, X, y=None):
 return self

 def transform(self, X):
 X = X.copy()
 if self.email_column not in X.columns:
 raise ValueError(f&quot;Column '{{self.email_column}}' not found in input data.&quot;)
 X['email_domain'] = X[self.email_column].apply(
 lambda x: x.split('@')[-1] if isinstance(x, str) and '@' in x else 'unknown'
 )
 return X

Being a fully self-specified data format, JSON strings are an extremely popular method for sending data over the internet, and in order to suit the wide variety of use cases that leverage JSON, they can get quite complex. 

Databricks model serving

 limits the payload of an individual request to 

16MB

, which a single JSON could hypothetically occupy all of. Needless to say, our two level JSON flattener class is only meant to be a placeholder to showcase a broader pattern.

Now that we have our 

.py

 file defined, let's open a notebook in the same folder and turn our cluster on if we don't already have one. For this code I used an 

r6i.xlarge

 single node CPU cluster on Databricks ML Runtime 15.4 LTS on AWS, but a rough equivalent in Azure would be the 

E4s_v4

, since both feature 4 vCPUs with 8 GiB of RAM, each powered by Intel Xeon Ice Lake processors. These are both fast and low cost, with the AWS one coming in at 1.02 DBU/hr.

Synthetic Data Generation

To keep the code in this blog fully functional out of the box, we're going to generate some synthetic data using one of my favorite python packages, 

Faker

.

# Note: as we said above, this pattern is not dependent on sklearn Pipelines
%pip install faker==18.11.2
%pip install scikit-learn==1.2.2
%pip install databricks-sdk --upgrade
%pip install mlflow==2.17.0
dbutils.library.restartPython()

# We'll use a non-sklearn ML package for our main model, XGBoost
import pandas as pd
import numpy as np
from faker import Faker
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import classification_report
import mlflow
import mlflow.pyfunc
from mlflow.models.signature import infer_signature
import xgboost as xgb
import joblib
import os
import sys

We'll generate 1,000 rows for now, but you could generate far more if you wanted to experiment with the scalability of your model.

# Faker makes synthetic data generation easy
fake = Faker()
Faker.seed(42)
np.random.seed(42)

def generate_data(num_rows=1000):
 data = []
 for _ in range(num_rows):
 customer_id = fake.unique.uuid4()
 name = fake.name()
 address = {
 'street': fake.street_address(),
 'city': fake.city(),
 'state': fake.state_abbr(),
 'zip_code': fake.zipcode()
 }
 email = fake.email()
 phone_number = fake.phone_number()

 transaction = {
 'transaction_id': fake.unique.uuid4(),
 'amount': round(np.random.uniform(10.0, 1000.0), 2),
 'transaction_type': np.random.choice(['online', 'in-store', 'cash withdrawal', 'mobile']),
 'account_age_days': np.random.randint(30, 3650),
 'customer_info': {
 'customer_id': customer_id,
 'name': name,
 'address': address,
 'email': email,
 'phone_number': phone_number
 },
 'fraud': np.random.choice([0, 1]



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
