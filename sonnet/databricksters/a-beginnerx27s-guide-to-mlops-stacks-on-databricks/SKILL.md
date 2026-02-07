---
name: "MLOps Stacks Quick Start"
description: "Bootstrap production ML projects using Databricks MLOps Stacks with CI/CD, testing, and deployment automation."
author: "Databricksters"
url: "https://www.databricksters.com/p/a-beginnerx27s-guide-to-mlops-stacks-on-databricks"
date: "2025"
tags: ["mlops-stacks", "cicd", "automation", "mlflow", "databricks"]
---

# MLOps Stacks Quick Start

## Overview

MLOps Stacks provides project templates with pre-configured CI/CD, testing, and deployment for ML projects. Includes GitHub Actions/Azure DevOps integration, model training pipelines, and production deployment workflows.

## Quick Start

```bash
# Install MLOps Stacks
pip install databricks-mlops-stacks

# Create new project
databricks mlops-stacks init

# Configure:
# - Project name
# - Cloud provider (AWS/Azure/GCP)
# - Git provider (GitHub/Azure DevOps)
# - Unity Catalog locations

# Result: Full MLOps project structure with CI/CD
```

## Common Patterns

### Pattern 1: Project Structure

```
my-ml-project/
├── .github/workflows/     # CI/CD pipelines
├── notebooks/
│   ├── Train.py          # Training logic
│   └── Evaluate.py       # Model evaluation
├── src/                  # Source code
├── tests/                # Unit tests
├── databricks.yml        # Asset bundle config
└── requirements.txt      # Dependencies
```

### Pattern 2: CI/CD Workflow

```yaml
# .github/workflows/deploy.yml
name: Deploy Model
on: [push]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Deploy to Databricks
        run: databricks bundle deploy -t prod
```

## FAQ

**Q: What's included in stacks?**  
A: Project template, CI/CD, model training, testing, deployment automation.

**Q: Can I customize?**  
A: Yes, generated project is fully customizable.
