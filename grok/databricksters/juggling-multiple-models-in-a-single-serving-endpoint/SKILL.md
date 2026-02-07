---
name: "multiple-models-single-endpoint-pyfunc"
description: "Serve multiple ML models through a single Databricks Model Serving endpoint using PyFunc wrappers, dynamic routing, and consolidated infrastructure."
---

# Multiple Models in a Single Serving Endpoint: PyFunc Router Pattern

## Overview

This skill covers serving multiple ML models through a single Databricks Model Serving endpoint using PyFunc wrappers. Learn how to implement dynamic routing logic, handle different input schemas, consolidate infrastructure costs, and manage model versioning when standard traffic splitting isn't sufficient. Includes patterns for request-based routing, time-based routing, and multi-domain model consolidation.

## Quick Start

### Basic Model Router with PyFunc
Create a simple router for multiple models:

```python
import mlflow
import pandas as pd
from mlflow.pyfunc import PythonModel

class ModelRouter(mlflow.pyfunc.PythonModel):
    """Router to serve multiple models from a single endpoint"""
    
    def load_context(self, context):
        """Load all models once during initialization"""
        self.linear_model = mlflow.sklearn.load_model(
            context.artifacts["linear_regression_model"]
        )
        self.forest_model = mlflow.sklearn.load_model(
            context.artifacts["random_forest_model"]
        )
    
    def predict(self, context, model_input):
        """Route requests to appropriate model"""
        # Use 'model' column to select which model to use
        if model_input['model'].eq('RandomForest').any():
            return {
                "prediction": self.forest_model.predict(
                    model_input.drop('model', axis=1)
                )
            }
        elif model_input['model'].eq('LinearRegression').any():
            return {
                "prediction": self.linear_model.predict(
                    model_input.drop('model', axis=1)
                )
            }
        else:
            raise ValueError("Unrecognized model type. Use 'RandomForest' or 'LinearRegression'")

# Register router model
def register_router_model():
    """Register the router model with artifacts"""
    
    router_model = ModelRouter()
    
    input_example = pd.DataFrame({
        'feature1': [1.0, 2.0],
        'feature2': [3.0, 4.0],
        'model': ['RandomForest', 'LinearRegression']
    })
    
    router_signature = mlflow.models.infer_signature(
        input_example,
        {"prediction": [1.5, 2.5]}
    )
    
    with mlflow.start_run() as run:
        mlflow.pyfunc.log_model(
            "model_router",
            python_model=router_model,
            signature=router_signature,
            artifacts={
                "linear_regression_model": "models:/catalog.schema.california_housing_linear_regression/1",
                "random_forest_model": "models:/catalog.schema.california_housing_random_forest/1"
            },
            extra_pip_requirements=["scikit-learn==1.4.2", "numpy==1.23.5", "pandas==1.5.3"]
        )
        
        mlflow.register_model(
            f"runs:/{run.info.run_id}/model_router",
            "catalog.schema.housing_model_router"
        )

# Usage
register_router_model()
```

## Common Patterns

### Pattern 1: Request-Based Routing
Route based on request attributes:

```python
class RequestBasedRouter(mlflow.pyfunc.PythonModel):
    """Route based on request attributes (user, region, etc.)"""
    
    def load_context(self, context):
        self.us_model = mlflow.sklearn.load_model(context.artifacts["us_model"])
        self.eu_model = mlflow.sklearn.load_model(context.artifacts["eu_model"])
        self.apac_model = mlflow.sklearn.load_model(context.artifacts["apac_model"])
    
    def predict(self, context, model_input):
        """Route based on region"""
        region = model_input['region'].iloc[0]
        
        if region == 'US':
            model = self.us_model
        elif region == 'EU':
            model = self.eu_model
        elif region == 'APAC':
            model = self.apac_model
        else:
            raise ValueError(f"Unknown region: {region}")
        
        # Drop routing column before prediction
        features = model_input.drop('region', axis=1)
        return {"prediction": model.predict(features)}

# Usage
router = RequestBasedRouter()
```

### Pattern 2: Time-Based Routing
Route based on time of day or date:

```python
from datetime import datetime

class TimeBasedRouter(mlflow.pyfunc.PythonModel):
    """Route models based on time"""
    
    def load_context(self, context):
        self.day_model = mlflow.sklearn.load_model(context.artifacts["day_model"])
        self.night_model = mlflow.sklearn.load_model(context.artifacts["night_model"])
    
    def predict(self, context, model_input):
        """Route based on current time"""
        current_hour = datetime.now().hour
        
        if 6 <= current_hour < 18:  # Daytime
            model = self.day_model
        else:  # Nighttime
            model = self.night_model
        
        return {"prediction": model.predict(model_input)}

# Usage
router = TimeBasedRouter()
```

### Pattern 3: Multi-Domain Router with Schema Validation
Handle completely different input schemas:

```python
class MultiDomainRouter(mlflow.pyfunc.PythonModel):
    """Route between models with different schemas"""
    
    def load_context(self, context):
        self.housing_model = mlflow.sklearn.load_model(
            context.artifacts["housing_model"]
        )
        self.cancer_model = mlflow.sklearn.load_model(
            context.artifacts["cancer_model"]
        )
        
        # Define required features for each domain
        self.housing_features = set(context.artifacts['housing_features'].split(','))
        self.cancer_features = set(context.artifacts['cancer_features'].split(','))
    
    def predict(self, context, model_input):
        """Route and validate input schema"""
        domain = model_input['domain'].iloc[0]
        
        if domain == 'housing':
            # Validate housing features
            input_cols = set(model_input.columns) - {'domain'}
            missing_cols = self.housing_features - input_cols
            
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            features = model_input[list(self.housing_features)]
            return {
                "prediction": self.housing_model.predict(features)
            }
        
        elif domain == 'cancer':
            # Validate cancer features
            input_cols = set(model_input.columns) - {'domain'}
            missing_cols = self.cancer_features - input_cols
            
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            features = model_input[list(self.cancer_features)]
            return {
                "prediction": self.cancer_model.predict(features)
            }
        
        else:
            raise ValueError(f"Unknown domain: {domain}")

# Usage
router = MultiDomainRouter()
```

### Pattern 4: Optional Fields in Model Signature
Create flexible signatures for multi-domain routing:

```python
def create_flexible_signature():
    """Create model signature with optional fields"""
    
    import pandas as pd
    
    # Create merged dataset with None values for optional fields
    housing_sample = pd.DataFrame({
        'MedInc': [8.3],
        'HouseAge': [41.0],
        'AveRooms': [6.9],
        'AveBedrms': [1.0],
        'Population': [322.0],
        'AveOccup': [2.6],
        'Latitude': [37.88],
        'Longitude': [-122.23],
        # Cancer features as None
        'mean_radius': [None],
        'mean_texture': [None],
        'domain': ['housing']
    })
    
    cancer_sample = pd.DataFrame({
        # Housing features as None
        'MedInc': [None],
        'HouseAge': [None],
        'AveRooms': [None],
        'AveBedrms': [None],
        'Population': [None],
        'AveOccup': [None],
        'Latitude': [None],
        'Longitude': [None],
        # Cancer features
        'mean_radius': [17.99],
        'mean_texture': [10.38],
        'domain': ['cancer']
    })
    
    # Merge samples
    merged_df = pd.concat([housing_sample, cancer_sample])
    
    # Infer signature - fields with None become optional
    signature = mlflow.models.infer_signature(
        merged_df,
        {"prediction": [2.0, 0.0]}
    )
    
    return signature

# Usage
flexible_signature = create_flexible_signature()
```

## Reference Files

- [MLflow PyFunc Models](https://mlflow.org/docs/latest/python_api/mlflow.pyfunc.html) - PyFunc documentation
- [Model Serving](https://docs.databricks.com/en/machine-learning/model-serving/index.html) - Databricks Model Serving
- [Traffic Splitting](https://docs.databricks.com/en/machine-learning/model-serving/traffic-splitting.html) - Standard traffic splitting

## Common Issues

| Issue | Solution |
|-------|----------|
| **Combined metrics** | Individual model metrics are combined - use custom logging |
| **Resource inefficiency** | Models load together - consider lazy loading for large models |
| **Routing logic complexity** | Document routing rules clearly, add validation |
| **Model versioning** | Less transparent - use artifact versioning |
| **Schema mismatches** | Use optional fields in signature, validate in predict() |

## Key Takeaways

1. **Use Case**: When traffic splitting isn't sufficient (request-based, time-based routing)
2. **PyFunc Wrapper**: Load models in `load_context()`, route in `predict()`
3. **Artifacts**: Reference registered models via `models:/` URIs
4. **Signature Flexibility**: Use optional fields for multi-domain scenarios
5. **Validation**: Validate inputs in `predict()` method
6. **Limitations**: Combined metrics, resource usage, less transparent versioning

## Complete Example: Production Router

```python
import mlflow
import pandas as pd
from mlflow.pyfunc import PythonModel
from datetime import datetime
import logging

class ProductionModelRouter(PythonModel):
    """Production-ready router with logging and error handling"""
    
    def load_context(self, context):
        """Load models and configuration"""
        self.models = {}
        self.feature_schemas = {}
        
        # Load models
        for model_name in ['housing', 'cancer', 'fraud']:
            self.models[model_name] = mlflow.sklearn.load_model(
                context.artifacts[f"{model_name}_model"]
            )
            self.feature_schemas[model_name] = set(
                context.artifacts[f"{model_name}_features"].split(',')
            )
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
    
    def predict(self, context, model_input):
        """Route with validation and logging"""
        try:
            # Extract routing information
            domain = model_input['domain'].iloc[0]
            user_id = model_input.get('user_id', [None])[0]
            
            # Validate domain
            if domain not in self.models:
                raise ValueError(f"Unknown domain: {domain}")
            
            # Validate features
            model = self.models[domain]
            required_features = self.feature_schemas[domain]
            input_features = set(model_input.columns) - {'domain', 'user_id'}
            missing = required_features - input_features
            
            if missing:
                raise ValueError(f"Missing features for {domain}: {missing}")
            
            # Prepare features
            features = model_input[list(required_features)]
            
            # Predict
            predictions = model.predict(features)
            
            # Log (for monitoring)
            self.logger.info(f"Prediction for domain={domain}, user={user_id}")
            
            return {"prediction": predictions}
        
        except Exception as e:
            self.logger.error(f"Prediction error: {str(e)}")
            raise

# Register
def register_production_router():
    """Register production router"""
    
    router = ProductionModelRouter()
    
    # Create flexible signature
    sample_input = pd.DataFrame({
        'feature1': [1.0, None],
        'feature2': [2.0, None],
        'feature3': [None, 3.0],
        'domain': ['housing', 'cancer'],
        'user_id': ['user1', 'user2']
    })
    
    signature = mlflow.models.infer_signature(
        sample_input,
        {"prediction": [1.5, 0.0]}
    )
    
    with mlflow.start_run() as run:
        mlflow.pyfunc.log_model(
            "production_router",
            python_model=router,
            signature=signature,
            artifacts={
                "housing_model": "models:/catalog.schema.housing_model/1",
                "cancer_model": "models:/catalog.schema.cancer_model/1",
                "fraud_model": "models:/catalog.schema.fraud_model/1",
                "housing_features": "MedInc,HouseAge,AveRooms",
                "cancer_features": "mean_radius,mean_texture",
                "fraud_features": "amount,merchant_category"
            }
        )
        
        mlflow.register_model(
            f"runs:/{run.info.run_id}/production_router",
            "catalog.schema.production_router"
        )

# Usage
register_production_router()
```

## When to Use This Pattern

### Use PyFunc Router When:
- Need request-based routing (user attributes, region, etc.)
- Need time-based routing
- Managing dozens of micro-models
- Want to consolidate infrastructure
- Standard traffic splitting insufficient

### Use Standard Traffic Splitting When:
- Simple A/B testing
- Canary deployments
- Percentage-based routing is sufficient
- Want transparent model metrics

## Best Practices

### 1. Document Routing Logic
```python
class DocumentedRouter(PythonModel):
    """
    Router with clear documentation.
    
    Routing Rules:
    - domain='housing': Uses California Housing model
    - domain='cancer': Uses Breast Cancer model
    - region='US': Uses US-specific model
    - time 6-18: Uses daytime model
    """
    pass
```

### 2. Add Input Validation
```python
def validate_input(self, model_input, required_features):
    """Validate input before prediction"""
    missing = required_features - set(model_input.columns)
    if missing:
        raise ValueError(f"Missing features: {missing}")
```

### 3. Handle Errors Gracefully
```python
def predict(self, context, model_input):
    try:
        # Routing logic
        pass
    except ValueError as e:
        # Return error response
        return {"error": str(e), "prediction": None}
```

## Related Skills

- mlflow-pyfunc-models
- model-serving-patterns
- dynamic-model-routing
- multi-model-consolidation