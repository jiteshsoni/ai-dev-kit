---
name: "Python Dependency Management Patterns"
description: "Manage Python dependencies in Databricks notebooks and jobs using %pip, requirements.txt, and cluster libraries."
author: "Databricksters"
url: "https://www.databricksters.com/p/doctors-hate-this-one-dependency-trick"
date: "2025"
tags: ["dependencies", "python", "pip", "libraries", "databricks"]
---

# Python Dependency Management

## Overview

Install Python packages using %pip magic (notebook-scoped), cluster libraries (cluster-wide), or job requirements (job-scoped). Each approach has trade-offs for isolation, performance, and sharing.

## Quick Start

```python
# Notebook-scoped installation (recommended)
%pip install pandas==2.0.0 numpy==1.24.0
dbutils.library.restartPython()

# Now use packages
import pandas as pd
df = pd.DataFrame({"col": [1, 2, 3]})
```

## Common Patterns

### Pattern 1: Requirements File

```python
# Create requirements.txt
requirements = """
pandas==2.0.0
numpy==1.24.0
scikit-learn==1.3.0
"""

# Install from file
%pip install -r /Workspace/Users/me/requirements.txt
dbutils.library.restartPython()
```

### Pattern 2: Cluster Libraries

```python
# Via Databricks SDK
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

w.libraries.install(
    cluster_id="cluster-id",
    libraries=[
        {"pypi": {"package": "pandas==2.0.0"}},
        {"pypi": {"package": "scikit-learn==1.3.0"}}
    ]
)

# Restart cluster to apply
w.clusters.restart(cluster_id="cluster-id")
```

## FAQ

**Q: %pip vs cluster libraries?**  
A: %pip for notebook isolation and fast iteration. Cluster libraries for shared dependencies.

**Q: How to share environments?**  
A: Export requirements.txt and commit to git. Install in each notebook/job.
