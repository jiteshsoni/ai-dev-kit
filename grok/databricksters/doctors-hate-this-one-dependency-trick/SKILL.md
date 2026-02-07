---
name: "mlflow-dependency-locking-uv"
description: "Lock MLflow model dependencies using uv: capture direct and transitive dependencies during logging, ensure consistent training and serving environments, and integrate with Databricks Asset Bundles."
---

# MLflow Dependency Locking with uv: The Dependency Trick

## Overview

This skill covers locking MLflow model dependencies using `uv` to capture both direct and transitive dependencies during model logging. Learn how to ensure consistent environments between training and serving, avoid dependency resolution issues at deployment time, and integrate dependency locking with Databricks Asset Bundles for end-to-end consistency.

## Quick Start

### Enable Dependency Locking
Lock dependencies during model logging:

```python
import os

# Enable dependency locking (MLflow 3+)
os.environ["MLFLOW_LOCK_MODEL_DEPENDENCIES"] = "true"

# Now MLflow will capture direct AND transitive dependencies
import mlflow
import mlflow.sklearn

# Log model - dependencies automatically locked
mlflow.sklearn.log_model(
    model, 
    "my_model",
    # Can still use extra_pip_requirements or pip_requirements
    # uv will resolve all dependencies and lock versions
)

print("Model logged with locked dependencies!")
```

### Verify Locked Dependencies
Check the requirements.txt file:

```python
import mlflow

# Load model to check dependencies
model_uri = "runs:/<run_id>/my_model"
model = mlflow.pyfunc.load_model(model_uri)

# Check requirements.txt
import os
requirements_path = os.path.join(model_uri.replace("runs:/", ""), "requirements.txt")

# Read requirements
with open(requirements_path, "r") as f:
    requirements = f.read()

print("Locked dependencies:")
print(requirements)
```

## Common Patterns

### Pattern 1: Resolve Environment Mismatches
Handle warnings about dependency differences:

```python
def handle_dependency_warnings():
    """
    MLflow may warn about dependency mismatches.
    Example: cloudpickle (current: 2.2.1, required: 3.1.1)
    """
    
    # Option 1: Use Databricks Asset Bundles to ensure consistency
    # In databricks.yml:
    """
    resources:
      jobs:
        train_model:
          tasks:
            - task_key: train
              libraries:
                - requirements: ./requirements.txt  # Pre-resolved dependencies
    """
    
    # Option 2: Pin specific versions in extra_pip_requirements
    mlflow.sklearn.log_model(
        model,
        "my_model",
        extra_pip_requirements=[
            "cloudpickle==3.1.1",  # Pin version matching uv resolution
            "numpy==1.24.3"
        ]
    )
    
    print("Dependencies pinned to match uv resolution")

# Usage
handle_dependency_warnings()
```

### Pattern 2: Integrate with Databricks Asset Bundles
Ensure consistency across environments:

```python
# databricks.yml
"""
resources:
  jobs:
    train_model:
      tasks:
        - task_key: train_model
          libraries:
            - requirements: ./requirements.txt  # Pre-resolved from dev workspace
          
    deploy_model:
      tasks:
        - task_key: deploy
          libraries:
            - requirements: ./requirements.txt  # Same dependencies
"""

# Workflow:
# 1. Develop in dev workspace with MLFLOW_LOCK_MODEL_DEPENDENCIES=true
# 2. Log model - generates requirements.txt with locked dependencies
# 3. Copy requirements.txt to Asset Bundle
# 4. Use same requirements.txt in test/prod workspaces
# 5. Training and serving environments match exactly
```

### Pattern 3: Custom Dependency Resolution
Control dependency resolution:

```python
import os

# Enable dependency locking
os.environ["MLFLOW_LOCK_MODEL_DEPENDENCIES"] = "true"

# Log model with custom requirements
mlflow.sklearn.log_model(
    model,
    "my_model",
    # uv will resolve these AND their transitive dependencies
    extra_pip_requirements=[
        "scikit-learn==1.3.0",
        "pandas==2.0.3",
        "numpy>=1.24.0"
    ]
)

# uv automatically:
# - Resolves transitive dependencies
# - Pins all versions
# - Captures in requirements.txt
```

### Pattern 4: Validate Dependencies Before Deployment
Check dependencies match environment:

```python
def validate_dependencies(model_uri, current_environment):
    """Validate model dependencies match current environment"""
    
    import pkg_resources
    
    # Load model requirements
    model = mlflow.pyfunc.load_model(model_uri)
    
    # Parse requirements.txt
    requirements_path = os.path.join(model_uri, "requirements.txt")
    with open(requirements_path, "r") as f:
        required_packages = {}
        for line in f:
            if "==" in line:
                package, version = line.strip().split("==")
                required_packages[package] = version
    
    # Check installed versions
    mismatches = []
    for package, required_version in required_packages.items():
        try:
            installed = pkg_resources.get_distribution(package).version
            if installed != required_version:
                mismatches.append({
                    "package": package,
                    "required": required_version,
                    "installed": installed
                })
        except pkg_resources.DistributionNotFound:
            mismatches.append({
                "package": package,
                "required": required_version,
                "installed": "NOT INSTALLED"
            })
    
    if mismatches:
        print("Dependency mismatches found:")
        for mismatch in mismatches:
            print(f"  {mismatch['package']}: required {mismatch['required']}, installed {mismatch['installed']}")
        return False
    else:
        print("All dependencies match!")
        return True

# Usage
is_valid = validate_dependencies("runs:/run_id/my_model", "serving")
```

## Reference Files

- [MLflow Dependency Management](https://mlflow.org/docs/latest/python_api/mlflow.models.html) - Dependency inference
- [uv Package Manager](https://github.com/astral-sh/uv) - Fast Python package resolver
- [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html) - Deployment orchestration

## Common Issues

| Issue | Solution |
|-------|----------|
| **Dependency errors at serving** | Enable MLFLOW_LOCK_MODEL_DEPENDENCIES to catch during logging |
| **Transitive dependency conflicts** | uv resolves all dependencies automatically |
| **Environment mismatches** | Use Databricks Asset Bundles with requirements.txt |
| **cloudpickle version warnings** | Pin version in extra_pip_requirements or use Asset Bundles |
| **Slow dependency resolution** | uv is fast, but can take time for complex dependency trees |

## Key Takeaways

1. **Dependency Locking**: MLflow 3+ with `MLFLOW_LOCK_MODEL_DEPENDENCIES=true` uses uv
2. **Transitive Dependencies**: uv captures ALL dependencies, not just direct ones
3. **Early Detection**: Dependencies resolved at logging time, not serving time
4. **Environment Consistency**: Use Databricks Asset Bundles with locked requirements.txt
5. **Version Pinning**: All dependencies pinned to specific versions in requirements.txt
6. **Warning Handling**: Address dependency mismatch warnings with Asset Bundles

## Complete Workflow

```python
def complete_dependency_management_workflow():
    """Complete workflow from development to production"""
    
    import os
    
    # Step 1: Enable dependency locking in dev workspace
    os.environ["MLFLOW_LOCK_MODEL_DEPENDENCIES"] = "true"
    
    # Step 2: Log model (dependencies automatically locked)
    with mlflow.start_run():
        mlflow.sklearn.log_model(
            model,
            "my_model",
            extra_pip_requirements=["scikit-learn==1.3.0"]
        )
        
        # Get run ID
        run_id = mlflow.active_run().info.run_id
    
    # Step 3: Extract requirements.txt
    model_uri = f"runs:/{run_id}/my_model"
    model = mlflow.pyfunc.load_model(model_uri)
    
    # Copy requirements.txt to Asset Bundle
    import shutil
    shutil.copy(
        os.path.join(model_uri, "requirements.txt"),
        "./requirements.txt"
    )
    
    print("requirements.txt saved to Asset Bundle")
    
    # Step 4: Use in Asset Bundle (databricks.yml)
    """
    resources:
      jobs:
        train_model:
          tasks:
            - task_key: train
              libraries:
                - requirements: ./requirements.txt
    """
    
    # Step 5: Deploy with same dependencies
    # Model serving will use requirements.txt from model artifacts
    # Training and serving environments match exactly
    
    return model_uri

# Usage
model_uri = complete_dependency_management_workflow()
```

## Best Practices

### 1. Always Enable Locking
```python
# Set in environment or at start of notebook
import os
os.environ["MLFLOW_LOCK_MODEL_DEPENDENCIES"] = "true"
```

### 2. Use Asset Bundles for Consistency
```yaml
# databricks.yml
resources:
  jobs:
    train:
      tasks:
        - task_key: train
          libraries:
            - requirements: ./requirements.txt
```

### 3. Validate Before Deployment
```python
# Check dependencies match
validate_dependencies(model_uri, "serving")
```

### 4. Handle Warnings Proactively
```python
# Address dependency mismatch warnings
# Use Asset Bundles or pin versions explicitly
```

## When to Use This Skill

- Avoiding dependency resolution issues at serving time
- Ensuring consistent training and serving environments
- Using Databricks Asset Bundles for deployment
- Managing complex dependency trees
- Debugging model serving failures
- Implementing MLOps best practices

## Related Skills

- mlflow-model-logging
- databricks-asset-bundles
- dependency-management-python
- mlops-best-practices