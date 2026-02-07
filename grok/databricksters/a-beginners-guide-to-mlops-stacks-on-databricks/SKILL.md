---
name: "databricks-mlops-stacks"
description: "Complete guide to implementing MLOps workflows on Databricks using Asset Bundles (DABs) for end-to-end ML lifecycle management."
---

# Databricks MLOps Stacks: Production ML Workflows

## Overview

This skill provides a comprehensive guide to implementing production-ready MLOps workflows on Databricks using MLOps Stacks and Asset Bundles (DABs). Learn how to structure ML projects, manage environments (dev/staging/prod), implement CI/CD pipelines, and automate the entire ML lifecycle from development to production deployment. Includes practical patterns for feature engineering, model training, validation, deployment, and monitoring.

## Quick Start

### Initialize MLOps Stack
Create your first production ML project using the MLOps Stacks template:

```bash
# Clone the template
git clone https://github.com/databricks/mlops-stacks.git
cd mlops-stacks

# Create your project
databricks bundle init mlops-stack \
  --project-name my-ml-project \
  --cloud aws \
  --profile DEFAULT

cd my-ml-project
```

### Configure Environments
Set up your development, staging, and production environments:

```yaml
# databricks.yml
bundle:
  name: my-ml-project

targets:
  dev:
    workspace:
      host: https://your-dev-workspace.cloud.databricks.com
      profile: dev-profile
    variables:
      catalog: dev_catalog

  staging:
    workspace:
      host: https://your-staging-workspace.cloud.databricks.com
      profile: staging-profile
    variables:
      catalog: staging_catalog

  prod:
    workspace:
      host: https://your-prod-workspace.cloud.databricks.com
      profile: prod-profile
    variables:
      catalog: prod_catalog
```

### Basic Workflow Execution
Run your first end-to-end ML pipeline:

```bash
# Validate configuration
databricks bundle validate -t dev

# Deploy to development
databricks bundle deploy -t dev

# Run feature engineering
databricks bundle run write_feature_table_job -t dev

# Run model training
databricks bundle run model_training_job -t dev

# Run model validation
databricks bundle run model_validation_job -t dev
```

## Common Patterns

### Pattern 1: Feature Engineering with Feature Store
Implement robust feature engineering pipelines:

```python
# features/feature_engineering.py
import mlflow
from databricks.feature_store import FeatureStoreClient

def create_feature_table():
    fs = FeatureStoreClient()

    # Read raw data
    raw_df = spark.read.table("raw.customer_data")

    # Feature engineering
    features_df = raw_df.withColumn("account_age_days",
        datediff(current_date(), col("signup_date"))) \
        .withColumn("avg_transaction_value",
            col("total_spend") / col("transaction_count")) \
        .select("customer_id", "account_age_days", "avg_transaction_value",
                "support_tickets", "churn_label")

    # Create feature table
    feature_table_name = "mlops_features.customer_features"

    fs.create_table(
        name=feature_table_name,
        primary_keys=["customer_id"],
        df=features_df,
        description="Customer features for churn prediction"
    )

    # Enable online store for real-time serving
    fs.publish_table(
        name=feature_table_name,
        online_store="databricks"
    )
```

### Pattern 2: Experiment Tracking with MLflow
Track model experiments and select best performing models:

```python
# training/train_model.py
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score
from databricks.feature_store import FeatureStoreClient

def train_churn_model():
    # Start MLflow run
    with mlflow.start_run(run_name="churn_model_v2") as run:

        # Get features from Feature Store
        fs = FeatureStoreClient()
        features_df = fs.read_table("mlops_features.customer_features")

        # Prepare training data
        train_df = features_df.filter(col("split") == "train")
        X = train_df.drop("churn_label").toPandas()
        y = train_df.select("churn_label").toPandas()

        # Model training with hyperparameter tuning
        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42
        )

        model.fit(X, y.values.ravel())

        # Evaluate model
        val_df = features_df.filter(col("split") == "validation")
        X_val = val_df.drop("churn_label").toPandas()
        y_val = val_df.select("churn_label").toPandas()

        predictions = model.predict(X_val)
        accuracy = accuracy_score(y_val, predictions)
        precision = precision_score(y_val, predictions)

        # Log metrics and parameters
        mlflow.log_param("n_estimators", 100)
        mlflow.log_param("max_depth", 10)
        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("precision", precision)

        # Log model
        mlflow.sklearn.log_model(model, "model")

        return accuracy, precision
```

### Pattern 3: Model Validation and Quality Gates
Implement automated model validation before deployment:

```python
# validation/model_validation.py
import mlflow
from databricks.feature_store import FeatureStoreClient
from sklearn.metrics import classification_report

def validate_model(model_uri, test_data_table):
    """Validate model performance against test data"""

    # Load model
    model = mlflow.sklearn.load_model(model_uri)

    # Get test data
    test_df = spark.read.table(test_data_table)
    X_test = test_df.drop("churn_label").toPandas()
    y_test = test_df.select("churn_label").toPandas()

    # Generate predictions
    predictions = model.predict(X_test)

    # Calculate metrics
    report = classification_report(y_test, predictions, output_dict=True)

    # Quality gates
    accuracy_threshold = 0.85
    precision_threshold = 0.80

    if report['accuracy'] < accuracy_threshold:
        raise ValueError(f"Model accuracy {report['accuracy']:.3f} below threshold {accuracy_threshold}")

    if report['weighted avg']['precision'] < precision_threshold:
        raise ValueError(f"Model precision {report['weighted avg']['precision']:.3f} below threshold {precision_threshold}")

    print("Model validation passed!")
    return report
```

### Pattern 4: Automated Deployment and Serving
Deploy validated models to production endpoints:

```python
# deployment/deploy_model.py
from databricks.sdk import WorkspaceClient
import mlflow

def deploy_model_to_serving(model_name, model_version, endpoint_name):
    """Deploy model to Databricks Model Serving endpoint"""

    w = WorkspaceClient()

    # Get model details
    model_details = mlflow.get_model_info(f"models:/{model_name}/{model_version}")

    # Create or update serving endpoint
    try:
        # Update existing endpoint
        w.serving_endpoints.update_config(
            name=endpoint_name,
            served_models=[{
                "model_name": model_name,
                "model_version": model_version,
                "scale_to_zero_enabled": True,
                "workload_size": "Small"
            }]
        )
    except:
        # Create new endpoint
        w.serving_endpoints.create(
            name=endpoint_name,
            config={
                "served_models": [{
                    "model_name": model_name,
                    "model_version": model_version,
                    "scale_to_zero_enabled": True,
                    "workload_size": "Small"
                }]
            }
        )

    print(f"Model deployed to endpoint: {endpoint_name}")
```

## Reference Files

- [MLOps Stacks Template](https://github.com/databricks/mlops-stacks) - Official template repository
- [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html) - DABs documentation
- [Databricks Feature Store](https://docs.databricks.com/en/machine-learning/feature-store/index.html) - Feature management
- [MLflow Model Registry](https://mlflow.org/docs/latest/model-registry.html) - Model versioning

## Common Issues

| Issue | Solution |
|-------|----------|
| **Bundle validation fails** | Check `databricks.yml` syntax and workspace permissions |
| **Feature Store table creation fails** | Verify Unity Catalog permissions and schema existence |
| **MLflow tracking issues** | Ensure MLflow experiment is initialized and accessible |
| **Model deployment timeout** | Increase timeout settings or use smaller model versions |
| **Environment variable conflicts** | Use target-specific variables in `databricks.yml` |
| **CI/CD pipeline failures** | Check Github Actions secrets and workspace connectivity |

## Key Takeaways

1. **Asset Bundles Foundation** - DABs provide infrastructure-as-code for ML workflows
2. **Three-Environment Pattern** - Dev → Staging → Prod with automated testing
3. **Feature Store Integration** - Centralized feature management for consistency
4. **MLflow Experiment Tracking** - Systematic model versioning and comparison
5. **Automated Validation** - Quality gates prevent poor models from reaching production
6. **CI/CD Integration** - Github Actions/Azure DevOps for automated deployments

## Environment Configuration Patterns

### Development Environment
```yaml
# Focus on experimentation and iteration
dev:
  variables:
    catalog: dev_catalog
    cluster_size: small
    enable_monitoring: false
  resources:
    jobs:
      - name: model_training_job
        max_concurrent_runs: 3
```

### Staging Environment
```yaml
# Focus on testing and validation
staging:
  variables:
    catalog: staging_catalog
    cluster_size: medium
    enable_monitoring: true
  resources:
    jobs:
      - name: integration_test_job
        timeout_seconds: 3600
```

### Production Environment
```yaml
# Focus on reliability and monitoring
prod:
  variables:
    catalog: prod_catalog
    cluster_size: large
    enable_monitoring: true
  resources:
    jobs:
      - name: model_training_job
        schedule: "0 2 * * 1"  # Weekly retraining
        max_concurrent_runs: 1
```

## Testing Strategies

### Unit Tests
```python
# tests/test_features.py
def test_feature_engineering():
    # Test feature transformations
    raw_data = spark.createDataFrame([
        (1, "2023-01-01", 1000.0, 10, 0),
        (2, "2023-06-01", 500.0, 2, 1)
    ], ["customer_id", "signup_date", "total_spend", "transaction_count", "churn_label"])

    result = create_feature_table(raw_data)

    # Assertions
    assert "account_age_days" in result.columns
    assert "avg_transaction_value" in result.columns
    assert result.filter(col("customer_id") == 1).select("account_age_days").first()[0] > 0
```

### Integration Tests
```bash
# Run full pipeline validation
databricks bundle validate -t staging
databricks bundle deploy -t staging
databricks bundle run end_to_end_test_job -t staging
```

## When to Use This Skill

- Setting up production ML workflows on Databricks
- Implementing CI/CD for machine learning projects
- Managing multiple environments (dev/staging/prod)
- Building reproducible ML pipelines
- Scaling from single models to ML factories
- Implementing governance for ML assets

## Performance Optimization

### Parallel Processing
```python
# training/parallel_training.py
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

def parallel_hyperparameter_tuning():
    """Distributed hyperparameter optimization"""

    # Define parameter grid
    paramGrid = ParamGridBuilder() \
        .addGrid(rf.numTrees, [10, 50, 100]) \
        .addGrid(rf.maxDepth, [5, 10, 20]) \
        .build()

    # Cross-validation
    cv = CrossValidator(
        estimator=rf,
        estimatorParamMaps=paramGrid,
        evaluator=BinaryClassificationEvaluator(),
        numFolds=3,
        parallelism=4  # Parallel folds
    )

    # Fit on distributed data
    cvModel = cv.fit(trainingData)

    return cvModel.bestModel
```

### Cost Optimization
```yaml
# Cost-effective production configuration
prod:
  resources:
    jobs:
      - name: daily_scoring_job
        # Use serverless for variable workloads
        compute:
          serverless: true
        # Scale to zero when idle
        parameters:
          environment:
            SPARK_CONF: "spark.databricks.sql.warehouse.enabled=true"
```

## Related Skills

- databricks-asset-bundles
- mlflow-experiment-tracking
- databricks-feature-store
- model-deployment-strategies
- mlops-ci-cd-pipelines