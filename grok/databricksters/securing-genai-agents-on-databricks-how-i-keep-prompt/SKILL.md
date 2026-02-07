---
name: "securing-genai-agents-databricks"
description: "Complete security framework for GenAI agents on Databricks: prompt injection prevention, data exfiltration protection, Mosaic AI Gateway, serverless egress control, and Unity Catalog integration."
---

# Securing GenAI Agents on Databricks: Complete Security Framework

## Overview

This skill covers comprehensive security practices for GenAI agents deployed on Databricks. Learn how to prevent prompt injection attacks, protect against data exfiltration, implement Mosaic AI Gateway guardrails, configure serverless egress control, enforce least-privilege data access with Unity Catalog, and integrate security testing with NVIDIA Garak. Includes OWASP LLM Top 10 compliance and Databricks AI Security Framework (DASF) alignment.

## Quick Start

### Enable Mosaic AI Gateway Guardrails
Set up first-line defense against prompt injection:

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedModelInput,
    AiProvidersInput
)

def configure_ai_gateway_guardrails():
    """Configure Mosaic AI Gateway with safety guardrails"""
    
    w = WorkspaceClient()
    
    # Create gateway endpoint configuration
    gateway_config = {
        "name": "secure-genai-gateway",
        "config": {
            "served_models": [
                {
                    "name": "llm-model",
                    "model_name": "databricks-meta-llama-3-3-70b-instruct",
                    "model_version": "1",
                    "workload_size": "Small",
                    "scale_to_zero_enabled": True
                }
            ],
            "ai_providers": [
                {
                    "name": "databricks-model-serving",
                    "provider": "databricks",
                    "ai_provider_type": "DATABRICKS_MODEL_SERVING"
                }
            ],
            "route_config": {
                "routes": [
                    {
                        "name": "default",
                        "route_type": "LLM_V1_COMPLETIONS",
                        "model": {
                            "name": "llm-model",
                            "provider": "databricks-model-serving"
                        }
                    }
                ]
            },
            # Enable AI Guardrails
            "ai_guardrails": {
                "enabled": True,
                "safety_model": "meta-llama/LlamaGuard-2-8b",  # Meta Llama Guard 2
                "rate_limiting": {
                    "enabled": True,
                    "requests_per_minute": 100,
                    "tokens_per_minute": 10000
                }
            }
        }
    }
    
    # Create gateway endpoint
    endpoint = w.serving_endpoints.create(**gateway_config)
    
    print("Mosaic AI Gateway configured with:")
    print("  - AI Guardrails enabled (Llama Guard 2)")
    print("  - Rate limiting: 100 req/min, 10k tokens/min")
    print("  - PII detection enabled")
    print("  - Violence/hate speech filtering")
    
    return endpoint

# Usage
gateway = configure_ai_gateway_guardrails()
```

### Configure Serverless Egress Control
Prevent unauthorized data exfiltration:

```python
from databricks.sdk import WorkspaceClient

def configure_serverless_egress_control():
    """Configure serverless egress control for model serving"""
    
    w = WorkspaceClient()
    
    # Create network policy (default-deny)
    network_policy = {
        "name": "model-serving-egress-policy",
        "policy_type": "EGRESS",
        "rules": [
            {
                "action": "ALLOW",
                "destination": "databricks.com",  # Allow Databricks services
                "description": "Allow Databricks services"
            },
            {
                "action": "ALLOW",
                "destination": "*.s3.amazonaws.com",  # Allow S3 access
                "description": "Allow S3 bucket access"
            },
            {
                "action": "DENY",
                "destination": "*",  # Deny all other outbound
                "description": "Default deny all"
            }
        ],
        "dry_run": False  # Set to True for testing
    }
    
    # Apply policy
    policy = w.network_policies.create(**network_policy)
    
    print("Serverless Egress Control configured:")
    print("  - Default-deny policy")
    print("  - FQDN allowlisting enabled")
    print("  - Audit logging enabled")
    
    return policy

# Usage
egress_policy = configure_serverless_egress_control()
```

## Common Patterns

### Pattern 1: Unity Catalog Least-Privilege Access
Enforce fine-grained data access:

```python
def configure_unity_catalog_access():
    """Configure Unity Catalog for least-privilege access"""
    
    # Grant minimal permissions to service principal
    spark.sql("""
        -- Grant SELECT on specific columns only
        GRANT SELECT (user_id, email, purchase_amount) 
        ON TABLE catalog.schema.user_data 
        TO `service-principal@databricks.com`;
        
        -- Apply row-level filter
        ALTER TABLE catalog.schema.user_data
        SET ROW FILTER catalog.schema.user_filter ON (user_id);
        
        -- Grant time-bound access
        GRANT SELECT ON TABLE catalog.schema.sensitive_data
        TO `service-principal@databricks.com`
        WITH GRANT OPTION;
    """)
    
    print("Unity Catalog configured:")
    print("  - Column-level permissions")
    print("  - Row-level filters")
    print("  - Time-bound access")
    
    # Create row filter function
    spark.sql("""
        CREATE FUNCTION catalog.schema.user_filter(user_id STRING)
        RETURNS BOOLEAN
        RETURN user_id = current_user();
    """)
```

### Pattern 2: NVIDIA Garak Security Testing
Automated security testing for LLM endpoints:

```python
import json
import requests
from datetime import datetime

def configure_garak_security_testing(endpoint_url: str, api_token: str):
    """Configure NVIDIA Garak for automated security testing"""
    
    # Garak configuration
    garak_config = {
        "model_type": "openai",
        "model_name": endpoint_url,
        "api_key": api_token,
        "probes": [
            "probe.promptinject",
            "probe.jailbreak",
            "probe.toxicity",
            "probe.pii"
        ],
        "generators": ["openai"],
        "reporting": ["json"]
    }
    
    # Save config
    with open("garak_config.json", "w") as f:
        json.dump(garak_config, f)
    
    print("Garak configuration created:")
    print("  - 150+ attack templates")
    print("  - Jailbreak detection")
    print("  - Prompt injection testing")
    print("  - Toxicity detection")
    print("  - PII leakage testing")
    
    # Example: Run Garak (requires garak package)
    # import garak
    # garak.run(garak_config)
    
    return garak_config

# Usage
garak_config = configure_garak_security_testing(
    endpoint_url="https://workspace.cloud.databricks.com/serving-endpoints/llm/invocations",
    api_token="your-token"
)
```

### Pattern 3: Network Connectivity Configuration (NCC)
Private connectivity for model serving:

```python
def configure_private_connectivity():
    """Configure Network Connectivity Configuration for private endpoints"""
    
    w = WorkspaceClient()
    
    # Create NCC for Azure
    ncc_config = {
        "name": "model-serving-ncc",
        "cloud": "azure",
        "region": "eastus",
        "private_endpoints": [
            {
                "name": "storage-endpoint",
                "resource_id": "/subscriptions/.../privateEndpoints/storage-pe",
                "service": "storage"
            },
            {
                "name": "keyvault-endpoint",
                "resource_id": "/subscriptions/.../privateEndpoints/kv-pe",
                "service": "keyvault"
            }
        ]
    }
    
    # Create NCC
    ncc = w.network_connectivity_configs.create(**ncc_config)
    
    print("Network Connectivity Configuration:")
    print("  - Private endpoints configured")
    print("  - No VPC complexity")
    print("  - Up to 100 endpoints per region (Azure)")
    
    return ncc

# Usage
ncc = configure_private_connectivity()
```

## Reference Files

- [Mosaic AI Gateway](https://docs.databricks.com/en/generative-ai/ai-gateway/index.html) - Gateway documentation
- [AI Security Framework](https://www.databricks.com/blog/announcing-databricks-ai-security-framework-20) - DASF 2.0
- [OWASP LLM Top 10](https://owasp.org/www-project-top-10-for-large-language-model-applications/) - Security risks
- [Serverless Egress Control](https://docs.databricks.com/en/security/network/serverless-egress-control.html) - Egress documentation
- [NVIDIA Garak](https://github.com/leondz/garak) - LLM vulnerability scanner

## Common Issues

| Issue | Solution |
|-------|----------|
| **Prompt injection attacks** | Enable Mosaic AI Gateway guardrails, use Llama Guard 2 |
| **Data exfiltration** | Configure serverless egress control, use FQDN allowlisting |
| **Unauthorized data access** | Implement Unity Catalog permissions, row-level filters |
| **Missing security testing** | Integrate NVIDIA Garak for automated testing |
| **No audit trail** | Enable comprehensive logging in Gateway and Unity Catalog |

## Key Takeaways

1. **Mosaic AI Gateway**: First-line defense with AI guardrails and rate limiting
2. **Serverless Egress Control**: Default-deny network policies prevent data exfiltration
3. **Unity Catalog**: Fine-grained access control (column-level, row-level, time-bound)
4. **NVIDIA Garak**: Automated security testing with 150+ attack templates
5. **Network Connectivity**: Private endpoints without VPC complexity
6. **OWASP LLM Top 10**: Prompt injection (LLM01) is #1 risk - address first

## Security Checklist

```python
def genai_security_checklist():
    """Pre-launch security checklist for GenAI agents"""
    
    checklist = {
        "gateway_guardrails": {
            "enabled": True,
            "safety_model": "meta-llama/LlamaGuard-2-8b",
            "rate_limiting": True,
            "pii_detection": True
        },
        "egress_control": {
            "enabled": True,
            "default_deny": True,
            "fqdn_allowlisting": True,
            "audit_logging": True
        },
        "data_access": {
            "unity_catalog_permissions": True,
            "row_level_filters": True,
            "column_level_permissions": True,
            "time_bound_access": True
        },
        "security_testing": {
            "garak_integration": True,
            "nightly_testing": True,
            "alert_on_failures": True
        },
        "monitoring": {
            "gateway_logging": True,
            "unity_catalog_audit": True,
            "egress_audit_logs": True,
            "anomaly_detection": True
        },
        "compliance": {
            "dasf_aligned": True,
            "owasp_llm_top10": True,
            "data_retention_policy": True,
            "incident_response_plan": True
        }
    }
    
    print("GenAI Security Checklist:")
    for category, items in checklist.items():
        print(f"\n{category.replace('_', ' ').title()}:")
        for item, status in items.items():
            marker = "✓" if status else "✗"
            print(f"  {marker} {item.replace('_', ' ').title()}")
    
    return checklist

# Usage
checklist = genai_security_checklist()
```

## Advanced Security Patterns

### Pattern 4: Custom Guardrails (Private Preview)
```python
def configure_custom_guardrails():
    """Configure custom guardrails for domain-specific rules"""
    
    # Note: Custom Guardrails is Private Preview
    # Check current Databricks documentation for availability
    
    custom_rules = {
        "prohibited_topics": [
            "internal pricing",
            "customer data",
            "trade secrets"
        ],
        "allowed_domains": [
            "general knowledge",
            "public information"
        ],
        "response_filtering": {
            "max_response_length": 1000,
            "block_sensitive_patterns": True
        }
    }
    
    print("Custom Guardrails Configuration:")
    print("  - Domain-specific rules")
    print("  - Topic moderation")
    print("  - Response filtering")
    
    return custom_rules

# Usage
custom_rules = configure_custom_guardrails()
```

### Pattern 5: Incident Response Automation
```python
def setup_incident_response():
    """Automate incident response for security events"""
    
    # Monitor Gateway logs for suspicious activity
    monitoring_query = """
    SELECT 
        timestamp,
        user_id,
        prompt_text,
        response_text,
        guardrail_action,
        CASE 
            WHEN guardrail_action = 'BLOCKED' THEN 'ALERT'
            WHEN rate_limit_exceeded THEN 'ALERT'
            ELSE 'OK'
        END as alert_status
    FROM system.access.gateway_logs
    WHERE timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
      AND (guardrail_action = 'BLOCKED' OR rate_limit_exceeded)
    """
    
    # Send alerts to Slack/Teams
    def send_alert(incident):
        # Integration with Slack/Teams webhook
        pass
    
    print("Incident Response configured:")
    print("  - Real-time monitoring")
    print("  - Automated alerts")
    print("  - Forensic logging")
    
    return monitoring_query

# Usage
monitoring = setup_incident_response()
```

## Databricks AI Security Framework (DASF) Alignment

```python
def dasf_compliance_check():
    """Verify alignment with DASF 2.0 (62 security risks)"""
    
    dasf_components = {
        "raw_data": ["encryption", "access_control", "data_lineage"],
        "data_preparation": ["pii_detection", "data_quality", "versioning"],
        "model_training": ["secure_training", "model_poisoning_prevention"],
        "model_serving": ["prompt_injection", "jailbreak_prevention", "egress_control"],
        "inference_responses": ["response_filtering", "pii_leakage_prevention"],
        "monitoring": ["anomaly_detection", "audit_logging", "alerting"]
    }
    
    print("DASF 2.0 Compliance Check:")
    for component, controls in dasf_components.items():
        print(f"\n{component.replace('_', ' ').title()}:")
        for control in controls:
            print(f"  - {control.replace('_', ' ').title()}")
    
    return dasf_components

# Usage
dasf_check = dasf_compliance_check()
```

## When to Use This Skill

- Deploying GenAI agents to production
- Implementing OWASP LLM Top 10 compliance
- Preventing prompt injection attacks
- Protecting against data exfiltration
- Configuring least-privilege data access
- Setting up automated security testing
- Aligning with Databricks AI Security Framework

## Related Skills

- ai-gateway-configuration
- data-governance-unity-catalog
- network-security-policies
- llm-security-testing
- prompt-injection-prevention