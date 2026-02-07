---
name: "pyfunc-real-time-preprocessing"
description: "Implement real-time data preprocessing in PyFunc models for Databricks Model Serving: custom transformers, external Python scripts, code_paths, and mlflow.models.predict testing."
---

# PyFunc Real-Time Preprocessing: It'll Do It Live!

## Overview

This skill covers implementing real-time data preprocessing in PyFunc models for Databricks Model Serving endpoints. Learn how to use custom preprocessing classes from external Python scripts, package them with code_paths, handle complex transformations like JSON flattening, and test models using mlflow.models.predict() to simulate serving environment before deployment.

## Quick Start

### Define Custom Preprocessing Classes
Create external preprocessing script:

```python
# custom_transformers.py
from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import json

class JSONFlattener(BaseEstimator, TransformerMixin):
    """Flatten nested JSON columns into tabular format"""
    
    def __init__(self, json_column, record_prefix=''):
        self.json_column = json_column
        self.record_prefix = record_prefix
    
    def fit(self, X, y=None):
        return self
    
    def flatten_dict(self, d, parent_key='', sep='.'):
        """Recursively flatten nested dictionary"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            else:
                if isinstance(v, list):
                    v = ';'.join(map(str, v))
                items.append((new_key, v))
        return dict(items)
    
    def transform(self, X):
        X = X.copy()
        flattened = X[self.json_column].apply(
            lambda x: self.flatten_dict(x, self.record_prefix, sep='.')
        )
        json_df = pd.DataFrame(flattened.tolist())
        X = X.drop(columns=[self.json_column])
        X = pd.concat([X.reset_index(drop=True), json_df.reset_index(drop=True)], axis=1)
        return X

class EmailDomainExtractor(BaseEstimator, TransformerMixin):
    """Extract domain from email addresses"""
    
    def __init__(self, email_column):
        self.email_column = email_column
    
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        X = X.copy()
        if self.email_column not in X.columns:
            raise ValueError(f"Column '{self.email_column}' not found")
        X['email_domain'] = X[self.email_column].apply(
            lambda x: x.split('@')[-1] if isinstance(x, str) and '@' in x else 'unknown'
        )
        return X
```

### Create PyFunc Model with Custom Preprocessing
Wrap model with preprocessing pipeline:

```python
import mlflow
import mlflow.pyfunc
from mlflow.models.signature import infer_signature
import xgboost as xgb
import joblib
import os

# Get path to custom transformers
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_dir = os.path.dirname(notebook_path)
dbfs_path = '/Workspace' + notebook_dir
custom_transformers_path = dbfs_path + '/custom_transformers.py'

# Import custom transformers
from custom_transformers import JSONFlattener, EmailDomainExtractor

# Define PyFunc model
class FraudDetectionModel(mlflow.pyfunc.PythonModel):
    """PyFunc model with custom preprocessing"""
    
    def load_context(self, context):
        """Load model and preprocessor"""
        import xgboost as xgb
        import joblib
        import pandas as pd
        # Import custom transformers (must be in load_context)
        from custom_transformers import JSONFlattener, EmailDomainExtractor
        
        # Load artifacts
        self.preprocessor = joblib.load(context.artifacts["preprocessor_path"])
        self.booster = xgb.Booster()
        self.booster.load_model(context.artifacts["model_path"])
    
    def predict(self, context, model_input):
        """Predict with preprocessing"""
        # Apply preprocessing
        processed_input = self.preprocessor.transform(model_input)
        
        # Convert to numpy if DataFrame
        if isinstance(processed_input, pd.DataFrame):
            processed_input = processed_input.values
        
        # Predict
        dmatrix = xgb.DMatrix(processed_input)
        predictions = self.booster.predict(dmatrix)
        
        return predictions

# Prepare artifacts
artifacts = {
    "preprocessor_path": "preprocessor.joblib",
    "model_path": "model.xgb"
}

# Define conda environment
conda_env = {
    'name': 'mlflow-env',
    'channels': ['defaults'],
    'dependencies': [
        'python=3.11.0',
        'pip',
        {
            'pip': [
                'mlflow==2.17.0',
                'xgboost==2.0.3',
                'joblib==1.2.0',
                'scikit-learn==1.2.2',
                'numpy==1.23.5',
                'pandas==2.0.3',
                'cloudpickle==2.2.1',
            ],
        },
    ],
}

# Log model with code_paths
with mlflow.start_run() as run:
    mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=FraudDetectionModel(),
        artifacts=artifacts,
        conda_env=conda_env,
        code_paths=[custom_transformers_path],  # CRITICAL: Include custom code
        signature=signature,
        input_example=sample_input
    )
    
    model_uri = f"runs:/{run.info.run_id}/model"
    mlflow.register_model(model_uri, "fraud_detection_model")
```

## Common Patterns

### Pattern 1: Test with mlflow.models.predict()
Simulate serving environment:

```python
import mlflow.models
import pandas as pd

def test_model_before_deployment(model_uri: str, test_data: pd.DataFrame):
    """
    Test model using mlflow.models.predict().
    
    This creates a lightweight virtual environment matching conda_env,
    making it a closer proxy for serving endpoint than load_model().
    """
    
    # Test 1: load_model() - Fast but uses notebook environment
    loaded_model = mlflow.pyfunc.load_model(model_uri)
    result1 = loaded_model.predict(test_data)
    print("load_model() test passed")
    
    # Test 2: mlflow.models.predict() - Slower but tests dependencies
    # This is MUCH better for catching deployment issues!
    output_path = "/tmp/mlflow_predictions.json"
    mlflow.models.predict(model_uri, test_data, output_path=output_path)
    
    import json
    with open(output_path, "r") as f:
        result2 = json.load(f)
    
    print("mlflow.models.predict() test passed")
    print("✓ Model ready for deployment!")
    
    return result1, result2

# Usage
model_uri = "models:/fraud_detection_model/1"
test_data = pd.DataFrame({
    "amount": [100.0, 500.0],
    "customer_info": [
        {"email": "user1@example.com", "name": "John"},
        {"email": "user2@example.com", "name": "Jane"}
    ]
})

test_model_before_deployment(model_uri, test_data)
```

### Pattern 2: Complex Preprocessing Pipeline
Combine multiple custom transformers:

```python
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder

# Build preprocessing pipeline with custom transformers
preprocessor = Pipeline(steps=[
    ('json_flattener', JSONFlattener(json_column='customer_info', record_prefix='customer_info')),
    ('email_domain_extractor', EmailDomainExtractor(email_column='customer_info.email')),
    ('column_transformer', ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), ['amount', 'account_age_days']),
            ('cat', OneHotEncoder(), ['transaction_type', 'email_domain'])
        ]
    ))
])

# Use in PyFunc model
class ModelWithPipeline(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        import joblib
        from custom_transformers import JSONFlattener, EmailDomainExtractor
        
        self.preprocessor = joblib.load(context.artifacts["preprocessor_path"])
        # Load model...
    
    def predict(self, context, model_input):
        processed = self.preprocessor.transform(model_input)
        # Predict...
```

### Pattern 3: Handle Large Payloads
Process 16MB limit efficiently:

```python
class LargeJSONProcessor(BaseEstimator, TransformerMixin):
    """Handle large JSON payloads efficiently"""
    
    def __init__(self, json_column, max_size_mb=15):
        self.json_column = json_column
        self.max_size_mb = max_size_mb
    
    def transform(self, X):
        """Process JSON with size checks"""
        X = X.copy()
        
        for idx, row in X.iterrows():
            json_str = row[self.json_column]
            
            # Check size (approximate)
            size_mb = len(str(json_str).encode('utf-8')) / (1024 * 1024)
            
            if size_mb > self.max_size_mb:
                # Truncate or sample for very large JSONs
                # Databricks Model Serving limit: 16MB per request
                raise ValueError(f"JSON too large: {size_mb:.2f}MB (max: {self.max_size_mb}MB)")
        
        # Process normally
        flattened = X[self.json_column].apply(self.flatten_dict)
        # ... rest of processing
        
        return X
```

## Reference Files

- [PyFunc Models](https://mlflow.org/docs/latest/traditional-ml/creating-custom-pyfunc/part2-pyfunc-components.html) - PyFunc documentation
- [Model Serving](https://docs.databricks.com/en/machine-learning/model-serving/index.html) - Serving documentation
- [mlflow.models.predict](https://mlflow.org/docs/latest/python_api/mlflow.models.html#mlflow.models.predict) - Testing API

## Common Issues

| Issue | Solution |
|-------|----------|
| **Module not found** | Include code_paths in log_model(), import in load_context() |
| **Container build fails** | Test with mlflow.models.predict() before deployment |
| **Payload too large** | Check 16MB limit, optimize JSON processing |
| **Dependency conflicts** | Specify exact versions in conda_env |
| **Preprocessing slow** | Optimize transformers, consider caching |

## Key Takeaways

1. **code_paths Parameter**: Include custom Python files via code_paths in log_model()
2. **Import in load_context()**: Import custom modules inside load_context() for packaging
3. **mlflow.models.predict()**: Use for testing - simulates serving environment
4. **conda_env**: Specify exact package versions to avoid conflicts
5. **Payload Limit**: 16MB per request in Model Serving
6. **Testing**: Test with both load_model() and mlflow.models.predict()

## Complete Workflow

```python
def complete_pyfunc_preprocessing_workflow():
    """Complete workflow from development to deployment"""
    
    # Step 1: Define custom transformers (custom_transformers.py)
    # ... custom transformer classes ...
    
    # Step 2: Build preprocessing pipeline
    preprocessor = Pipeline([
        ('json_flattener', JSONFlattener(...)),
        ('email_extractor', EmailDomainExtractor(...)),
        # ... more steps ...
    ])
    
    # Step 3: Train model
    preprocessor.fit(X_train, y_train)
    model.fit(preprocessor.transform(X_train), y_train)
    
    # Step 4: Save artifacts
    joblib.dump(preprocessor, "preprocessor.joblib")
    model.save("model.pkl")
    
    # Step 5: Define PyFunc wrapper
    class MyModel(mlflow.pyfunc.PythonModel):
        def load_context(self, context):
            from custom_transformers import JSONFlattener, EmailDomainExtractor
            # Load artifacts...
        
        def predict(self, context, model_input):
            # Preprocess and predict...
            pass
    
    # Step 6: Log model with code_paths
    mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=MyModel(),
        artifacts={"preprocessor": "preprocessor.joblib", "model": "model.pkl"},
        code_paths=["custom_transformers.py"],  # Include custom code
        conda_env=conda_env
    )
    
    # Step 7: Test before deployment
    model_uri = "models:/my_model/1"
    test_model_before_deployment(model_uri, test_data)
    
    # Step 8: Deploy to Model Serving
    # Model ready for production!
    
    print("Workflow complete!")

# Usage
complete_pyfunc_preprocessing_workflow()
```

## When to Use This Skill

- Real-time preprocessing in Model Serving
- Complex data transformations
- JSON parsing and flattening
- Custom feature engineering
- Maintaining modular preprocessing code
- Testing models before deployment

## Related Skills

- mlflow-pyfunc-models
- model-serving-patterns
- custom-transformers
- real-time-inference