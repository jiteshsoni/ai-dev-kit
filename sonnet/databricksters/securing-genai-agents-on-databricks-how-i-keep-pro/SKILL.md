---
name: "GenAI Agent Security Patterns"
description: "Secure AI agents against prompt injection, data leakage, and unauthorized access using input validation, output filtering, and RBAC."
author: "Databricksters"
url: "https://www.databricksters.com/p/securing-genai-agents-on-databricks-how-i-keep-pro"
date: "2025"
tags: ["security", "agents", "prompt-injection", "databricks"]
---

# GenAI Agent Security

## Overview

Protect agents from attacks: prompt injection (malicious instructions), data leakage (exposing sensitive info), and unauthorized access. Defense layers: input sanitization, output filtering, Unity Catalog permissions, and audit logging.

## Quick Start

```python
class SecureAgent:
    def __init__(self, llm):
        self.llm = llm
        self.allowed_tables = ["catalog.public.*"]
    
    def validate_input(self, user_query):
        # Block injection attempts
        forbidden = ["IGNORE PREVIOUS", "SYSTEM:", "DROP TABLE"]
        if any(term in user_query.upper() for term in forbidden):
            raise SecurityError("Suspicious input detected")
    
    def filter_output(self, response):
        # Redact sensitive patterns
        import re
        response = re.sub(r'\b\d{3}-\d{2}-\d{4}\b', '[SSN_REDACTED]', response)
        return response
    
    def execute(self, query):
        self.validate_input(query)
        response = self.llm.invoke(query)
        return self.filter_output(response)
```

## Common Patterns

### Pattern 1: Input Validation

```python
def validate_sql_tool_input(sql: str) -> bool:
    # Only allow SELECT
    if not sql.strip().upper().startswith("SELECT"):
        return False
    # Block sensitive tables
    if "passwords" in sql.lower() or "secrets" in sql.lower():
        return False
    return True

@tool
def safe_query_database(sql: str) -> list:
    if not validate_sql_tool_input(sql):
        return "Unauthorized query"
    return spark.sql(sql).collect()
```

### Pattern 2: Unity Catalog RBAC

```sql
-- Restrict agent service principal
GRANT USE CATALOG ON catalog.public TO agent_sp;
GRANT SELECT ON catalog.public.* TO agent_sp;
-- No access to sensitive catalog
DENY ALL ON catalog.sensitive TO agent_sp;
```

## FAQ

**Q: How to prevent prompt injection?**  
A: Input validation, system message separation, output parsing validation.
