---
name: "token-rate-limiting-lakebase"
description: "Implement token-based rate limiting for LLM APIs using Databricks Lakebase to control AI costs and prevent budget overruns."
---

# Token-Based Rate Limiting with Lakebase

## Overview

This skill covers implementing token-based rate limiting for Large Language Model APIs using Databricks Lakebase (PostgreSQL) to track usage, control costs, and prevent budget overruns. Learn how to create custom MLflow PyFunc models that validate token limits before API calls, track usage in real-time, and provide cost control for AI workloads while maintaining user experience.

## Quick Start

### Set Up Lakebase Tables
Create the necessary tables for token tracking:

```sql
-- Create token_usage table for tracking all API calls
CREATE TABLE IF NOT EXISTS token_usage (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_name VARCHAR(255) NOT NULL,
    model_name VARCHAR(100) NOT NULL,
    prompt_tokens INTEGER NOT NULL,
    completion_tokens INTEGER NOT NULL,
    total_tokens INTEGER NOT NULL,
    request_timestamp TIMESTAMP NOT NULL,
    request_id VARCHAR(255),
    response_content STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create user_token_limits table for managing quotas
CREATE TABLE IF NOT EXISTS user_token_limits (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_name VARCHAR(255) NOT NULL,
    model_name VARCHAR(100) NOT NULL,
    token_limit INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample user limits
INSERT INTO user_token_limits (user_name, model_name, token_limit) 
VALUES 
    ('analyst@databricks.com', 'gpt-4', 50000),
    ('developer@databricks.com', 'claude-3', 25000),
    ('manager@databricks.com', 'gpt-4', 100000);
```

### Create Token-Limiting Gateway Model
Implement the rate limiting logic as an MLflow PyFunc model:

```python
import mlflow.pyfunc
import psycopg2
import requests
import pandas as pd
import json
import os
from datetime import datetime

class TokenLimitedGatewayModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        """Initialize database connection and endpoint URL"""
        self.conn = psycopg2.connect(
            host=os.environ['POSTGRES_HOST'],
            dbname=os.environ['POSTGRES_DBNAME'],
            user=os.environ['POSTGRES_USER'],
            password=os.environ['POSTGRES_PASSWORD'],
            port=int(os.environ.get('POSTGRES_PORT', 5432)),
            sslmode=os.environ.get('POSTGRES_SSLMODE', 'require')
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        
        # FM endpoint URL
        self.fm_endpoint = os.environ['FM_ENDPOINT_URL']
        self.api_token = os.environ.get('DATABRICKS_TOKEN', '')
        
        print("Token-limited gateway model loaded successfully")

    def predict(self, context, model_input):
        """Process request with token limit checking"""
        
        # Parse input
        if isinstance(model_input, pd.DataFrame):
            data = model_input.iloc[0].to_dict()
        elif isinstance(model_input, dict):
            data = model_input
        else:
            return {"error": f"Unsupported input type: {type(model_input)}"}
        
        # Extract parameters
        user_name = str(data.get('user_name', 'default@user.com'))
        model_name = str(data.get('model', 'gpt-4'))
        messages = data.get('messages', [])
        max_tokens = int(data.get('max_tokens', 128))
        temperature = float(data.get('temperature', 0.7))
        
        # Check current token usage
        self.cursor.execute("""
            SELECT COALESCE(SUM(total_tokens), 0) as total_used
            FROM token_usage 
            WHERE user_name = %s AND model_name = %s
        """, (user_name, model_name))
        
        result = self.cursor.fetchone()
        tokens_used = int(result[0]) if result and result[0] else 0
        
        # Check user's token limit
        self.cursor.execute("""
            SELECT token_limit 
            FROM user_token_limits 
            WHERE user_name = %s AND model_name = %s
        """, (user_name, model_name))
        
        limit_result = self.cursor.fetchone()
        if not limit_result:
            return {
                "error": f"No token limit found for user {user_name} and model {model_name}",
                "tokens_used": tokens_used
            }
        
        token_limit = int(limit_result[0])
        
        # Check if limit would be exceeded
        if tokens_used >= token_limit:
            return {
                "error": f"Token limit exceeded. Used: {tokens_used}, Limit: {token_limit}",
                "tokens_used": tokens_used,
                "token_limit": token_limit,
                "tokens_remaining": 0
            }
        
        # Prepare FM API request
        fm_request = {
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature
        }
        
        headers = {"Content-Type": "application/json"}
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"
        
        try:
            # Call FM endpoint
            response = requests.post(
                self.fm_endpoint,
                json=fm_request,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            fm_response = response.json()
            
            # Extract token usage
            usage = fm_response.get('usage', {})
            prompt_tokens = int(usage.get('prompt_tokens', 0))
            completion_tokens = int(usage.get('completion_tokens', 0))
            total_tokens = int(usage.get('total_tokens', prompt_tokens + completion_tokens))
            
            # Log usage to database
            self.cursor.execute("""
                INSERT INTO token_usage (
                    user_name, model_name, prompt_tokens, completion_tokens, 
                    total_tokens, request_timestamp, request_id, response_content
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                user_name, model_name, prompt_tokens, completion_tokens, total_tokens,
                datetime.utcnow(), fm_response.get('id', ''), json.dumps(fm_response)
            ))
            
            # Add usage info to response
            total_used_now = tokens_used + total_tokens
            fm_response['usage_info'] = {
                'tokens_used_total': total_used_now,
                'token_limit': token_limit,
                'tokens_remaining': max(0, token_limit - total_used_now),
                'request_cost': self._calculate_cost(model_name, total_tokens)
            }
            
            return fm_response
            
        except requests.exceptions.RequestException as e:
            return {
                "error": f"Failed to call FM endpoint: {str(e)}",
                "tokens_used": tokens_used,
                "token_limit": token_limit
            }
        except Exception as e:
            return {
                "error": f"Unexpected error: {str(e)}",
                "tokens_used": tokens_used,
                "token_limit": token_limit
            }
    
    def _calculate_cost(self, model_name, total_tokens):
        """Calculate approximate cost for the request"""
        # Cost per 1000 tokens (approximate rates)
        rates = {
            'gpt-4': 0.03,      # $0.03 per 1K tokens
            'gpt-3.5-turbo': 0.002,
            'claude-3': 0.015,
            'claude-2': 0.008
        }
        
        rate = rates.get(model_name, 0.01)  # Default rate
        return round((total_tokens / 1000) * rate, 4)

# Test the model
if __name__ == "__main__":
    # Test data
    test_input = pd.DataFrame([{
        "messages": json.dumps([
            {"role": "user", "content": "Hello, how are you?"}
        ]),
        "user_name": "test@databricks.com",
        "model": "gpt-4",
        "max_tokens": 50,
        "temperature": 0.7
    }])
    
    model = TokenLimitedGatewayModel()
    model.load_context(None)
    result = model.predict(None, test_input)
    print("Test result:", result)
```

### Deploy as MLflow Model
Log and register the token-limiting model:

```python
from mlflow.types import DataType, Schema, ColSpec
import mlflow.models

# Define model signature
input_schema = Schema([
    ColSpec(DataType.string, "messages"),
    ColSpec(DataType.string, "user_name"), 
    ColSpec(DataType.string, "model"),
    ColSpec(DataType.long, "max_tokens"),
    ColSpec(DataType.double, "temperature")
])

output_schema = Schema([
    ColSpec(DataType.string, "response")
])

signature = mlflow.models.ModelSignature(
    inputs=input_schema,
    outputs=output_schema
)

# Log the model
with mlflow.start_run() as run:
    mlflow.pyfunc.log_model(
        artifact_path="token_gateway",
        python_model=TokenLimitedGatewayModel(),
        pip_requirements=[
            "mlflow",
            "requests", 
            "psycopg2-binary",
            "pandas"
        ],
        signature=signature
    )

model_uri = f"runs:/{run.info.run_id}/token_gateway"
print(f"Model logged with URI: {model_uri}")

# Register to Unity Catalog
catalog = "your_catalog"
schema = "your_schema" 
model_name = "token_limited_gateway"

registered_model = mlflow.register_model(
    model_uri=model_uri,
    name=f"{catalog}.{schema}.{model_name}",
    tags={
        "use_case": "rate_limiting",
        "model_type": "gateway",
        "backend": "openai_gpt4",
        "database": "lakebase_postgres"
    }
)
```

## Common Patterns

### Pattern 1: Multi-Level Token Limits
Implement hierarchical token limits (user, department, organization):

```python
def create_multi_level_limits():
    """Create hierarchical token limits"""
    
    # Organization-level limits
    org_limits = """
    CREATE TABLE organization_limits (
        org_id VARCHAR(50) PRIMARY KEY,
        monthly_token_limit INTEGER NOT NULL,
        current_month_tokens INTEGER DEFAULT 0,
        reset_date DATE DEFAULT CURRENT_DATE + INTERVAL '1 month'
    );
    
    INSERT INTO organization_limits (org_id, monthly_token_limit) 
    VALUES ('engineering', 5000000), ('sales', 2000000);
    """
    
    # Department-level limits
    dept_limits = """
    CREATE TABLE department_limits (
        dept_id VARCHAR(50) PRIMARY KEY,
        org_id VARCHAR(50) REFERENCES organization_limits(org_id),
        monthly_token_limit INTEGER NOT NULL,
        current_month_tokens INTEGER DEFAULT 0
    );
    
    INSERT INTO department_limits (dept_id, org_id, monthly_token_limit)
    VALUES 
        ('data_science', 'engineering', 1000000),
        ('ml_engineering', 'engineering', 2000000);
    """
    
    # Enhanced user limits with department reference
    user_limits_enhanced = """
    CREATE TABLE user_token_limits_enhanced (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        user_name VARCHAR(255) NOT NULL,
        dept_id VARCHAR(50) REFERENCES department_limits(dept_id),
        model_name VARCHAR(100) NOT NULL,
        daily_token_limit INTEGER,
        monthly_token_limit INTEGER,
        current_day_tokens INTEGER DEFAULT 0,
        current_month_tokens INTEGER DEFAULT 0,
        last_reset_date DATE DEFAULT CURRENT_DATE
    );
    """
    
    return {
        'org_limits': org_limits,
        'dept_limits': dept_limits, 
        'user_limits_enhanced': user_limits_enhanced
    }

class HierarchicalTokenLimiter(TokenLimitedGatewayModel):
    """Token limiter with hierarchical limits"""
    
    def _check_hierarchical_limits(self, user_name, model_name, requested_tokens):
        """Check limits at user, department, and organization levels"""
        
        # Get user's department
        self.cursor.execute("""
            SELECT dept_id FROM user_token_limits_enhanced 
            WHERE user_name = %s
        """, (user_name,))
        
        dept_result = self.cursor.fetchone()
        if not dept_result:
            return {"error": "User not found in limits table"}
        
        dept_id = dept_result[0]
        
        # Check user daily/monthly limits
        self.cursor.execute("""
            SELECT daily_token_limit, monthly_token_limit,
                   current_day_tokens, current_month_tokens
            FROM user_token_limits_enhanced
            WHERE user_name = %s AND model_name = %s
        """, (user_name, model_name))
        
        user_limits = self.cursor.fetchone()
        if user_limits:
            daily_limit, monthly_limit, day_tokens, month_tokens = user_limits
            
            if daily_limit and day_tokens + requested_tokens > daily_limit:
                return {"error": f"Daily limit exceeded: {day_tokens}/{daily_limit}"}
            
            if monthly_limit and month_tokens + requested_tokens > monthly_limit:
                return {"error": f"Monthly limit exceeded: {month_tokens}/{monthly_limit}"}
        
        # Check department limits
        self.cursor.execute("""
            SELECT org_id, monthly_token_limit, current_month_tokens
            FROM department_limits d
            JOIN organization_limits o ON d.org_id = o.org_id
            WHERE d.dept_id = %s
        """, (dept_id,))
        
        dept_org_limits = self.cursor.fetchone()
        if dept_org_limits:
            org_id, dept_limit, dept_tokens = dept_org_limits
            
            if dept_limit and dept_tokens + requested_tokens > dept_limit:
                return {"error": f"Department limit exceeded: {dept_tokens}/{dept_limit}"}
        
        return {"approved": True}
```

### Pattern 2: Dynamic Token Allocation
Implement dynamic token allocation based on usage patterns:

```python
class DynamicTokenAllocator:
    """Dynamic token allocation based on usage patterns and priorities"""
    
    def __init__(self, base_connection):
        self.conn = base_connection
        self.cursor = self.conn.cursor()
    
    def allocate_tokens_based_on_priority(self, user_name, model_name, requested_tokens):
        """Allocate tokens based on user priority and current usage"""
        
        # Get user priority and current usage
        self.cursor.execute("""
            SELECT priority_level, current_month_tokens, monthly_token_limit
            FROM user_priorities 
            WHERE user_name = %s
        """, (user_name,))
        
        priority_result = self.cursor.fetchone()
        if not priority_result:
            priority_level = 'medium'  # Default priority
            current_usage = 0
            monthly_limit = 10000
        else:
            priority_level, current_usage, monthly_limit = priority_result
        
        # Priority multipliers
        priority_multipliers = {
            'low': 0.5,
            'medium': 1.0,
            'high': 1.5,
            'critical': 2.0
        }
        
        # Calculate dynamic limit
        base_limit = monthly_limit
        priority_multiplier = priority_multipliers.get(priority_level, 1.0)
        
        # Burst allowance for high-priority users
        usage_ratio = current_usage / base_limit if base_limit > 0 else 0
        if priority_level in ['high', 'critical'] and usage_ratio < 0.8:
            burst_multiplier = 1.2
        else:
            burst_multiplier = 1.0
        
        dynamic_limit = int(base_limit * priority_multiplier * burst_multiplier)
        
        # Check if allocation is possible
        if current_usage + requested_tokens <= dynamic_limit:
            return {
                "approved": True,
                "dynamic_limit": dynamic_limit,
                "priority_level": priority_level,
                "allocated_tokens": requested_tokens
            }
        else:
            return {
                "approved": False,
                "reason": f"Dynamic limit exceeded: {current_usage + requested_tokens}/{dynamic_limit}",
                "priority_level": priority_level
            }
    
    def update_usage_with_decay(self):
        """Update usage with time-based decay for fairness"""
        
        # Daily decay (reduce usage by 5% daily for fairness)
        self.cursor.execute("""
            UPDATE user_token_limits_enhanced
            SET current_day_tokens = GREATEST(0, current_day_tokens * 0.95)
            WHERE last_reset_date < CURRENT_DATE
        """)
        
        # Monthly decay
        self.cursor.execute("""
            UPDATE user_token_limits_enhanced
            SET current_month_tokens = GREATEST(0, current_month_tokens * 0.98)
            WHERE DATE_TRUNC('month', last_reset_date) < DATE_TRUNC('month', CURRENT_DATE)
        """)
        
        self.conn.commit()
```

### Pattern 3: Cost-Aware Token Limiting
Implement cost-based limits alongside token limits:

```python
class CostAwareTokenLimiter(TokenLimitedGatewayModel):
    """Token limiter that considers both token counts and monetary costs"""
    
    def __init__(self):
        super().__init__()
        # Cost per 1000 tokens for different models (USD)
        self.cost_rates = {
            'gpt-4': 0.03,
            'gpt-4-turbo': 0.01,
            'gpt-3.5-turbo': 0.002,
            'claude-3-opus': 0.015,
            'claude-3-sonnet': 0.008,
            'claude-3-haiku': 0.0008,
            'gemini-pro': 0.0005
        }
        
        # Daily cost limits per user
        self.daily_cost_limits = {
            'analyst': 5.00,
            'developer': 10.00, 
            'manager': 25.00,
            'executive': 50.00
        }
    
    def _check_cost_limits(self, user_name, model_name, estimated_tokens):
        """Check if request would exceed cost limits"""
        
        # Calculate estimated cost
        rate_per_1k = self.cost_rates.get(model_name, 0.01)
        estimated_cost = (estimated_tokens / 1000) * rate_per_1k
        
        # Get user's role and current daily spending
        self.cursor.execute("""
            SELECT role, COALESCE(daily_cost_spent, 0) as daily_spent
            FROM user_profiles 
            WHERE user_name = %s
        """, (user_name,))
        
        profile_result = self.cursor.fetchone()
        if not profile_result:
            user_role = 'analyst'  # Default role
            daily_spent = 0
        else:
            user_role, daily_spent = profile_result
        
        # Check daily cost limit
        daily_limit = self.daily_cost_limits.get(user_role, 5.00)
        
        if daily_spent + estimated_cost > daily_limit:
            return {
                "approved": False,
                "reason": f"Daily cost limit exceeded: ${daily_spent + estimated_cost:.2f}/${daily_limit:.2f}",
                "current_spending": daily_spent,
                "estimated_request_cost": estimated_cost
            }
        
        return {
            "approved": True,
            "estimated_cost": estimated_cost,
            "remaining_daily_budget": daily_limit - (daily_spent + estimated_cost)
        }
    
    def _log_cost_usage(self, user_name, model_name, tokens_used, request_cost):
        """Log cost usage for monitoring"""
        
        self.cursor.execute("""
            INSERT INTO cost_usage (
                user_name, model_name, tokens_used, cost_usd, 
                request_timestamp, daily_accumulated_cost
            ) VALUES (%s, %s, %s, %s, %s, 
                COALESCE((
                    SELECT daily_cost_spent + %s
                    FROM user_profiles 
                    WHERE user_name = %s
                ), %s)
            )
        """, (
            user_name, model_name, tokens_used, request_cost,
            datetime.utcnow(), request_cost, user_name, request_cost
        ))
        
        # Update user's accumulated daily cost
        self.cursor.execute("""
            UPDATE user_profiles 
            SET daily_cost_spent = COALESCE(daily_cost_spent, 0) + %s,
                last_cost_update = CURRENT_TIMESTAMP
            WHERE user_name = %s
        """, (request_cost, user_name))
        
        self.conn.commit()
```

## Reference Files

- [Databricks Lakebase](https://docs.databricks.com/en/query-federation/lakebase.html) - Lakebase documentation
- [MLflow PyFunc Models](https://mlflow.org/docs/latest/models.html#pyfunc-models) - Custom model implementation
- [PostgreSQL Connection](https://www.psycopg.org/docs/) - Database connectivity

## Common Issues

| Issue | Solution |
|-------|----------|
| **Database connection failures** | Verify Lakebase connection parameters and SSL settings |
| **Token estimation errors** | Implement proper token counting before API calls |
| **Rate limiting conflicts** | Combine token limits with QPM limits for comprehensive control |
| **Cost calculation drift** | Regularly update cost rates and validate against actual usage |
| **User experience degradation** | Provide clear error messages and remaining balance information |

## Key Takeaways

1. **Token-Based Control** - Shift from QPM to token-based limiting for true cost control
2. **Lakebase Integration** - Use PostgreSQL for fast, reliable usage tracking
3. **Hierarchical Limits** - Implement user, department, and organization-level controls
4. **Cost Awareness** - Track both token usage and monetary costs
5. **Dynamic Allocation** - Adjust limits based on usage patterns and priorities
6. **Monitoring & Alerts** - Track usage trends and set up budget alerts

## Advanced Monitoring Dashboard

### Create Token Usage Analytics
```python
def create_token_usage_dashboard():
    """Create comprehensive token usage monitoring dashboard"""
    
    # Daily usage trends
    daily_usage_query = """
    SELECT 
        DATE(request_timestamp) as date,
        user_name,
        model_name,
        SUM(total_tokens) as daily_tokens,
        SUM(cost_usd) as daily_cost,
        COUNT(*) as request_count
    FROM token_usage
    WHERE request_timestamp >= CURRENT_DATE() - INTERVAL 30 DAYS
    GROUP BY DATE(request_timestamp), user_name, model_name
    ORDER BY date DESC, daily_cost DESC
    """
    
    # User spending analysis
    user_spending_query = """
    SELECT 
        user_name,
        SUM(total_tokens) as total_tokens_used,
        SUM(cost_usd) as total_cost,
        AVG(total_tokens) as avg_tokens_per_request,
        COUNT(*) as total_requests,
        MAX(request_timestamp) as last_request
    FROM token_usage
    WHERE request_timestamp >= CURRENT_DATE() - INTERVAL 30 DAYS
    GROUP BY user_name
    ORDER BY total_cost DESC
    """
    
    # Model utilization analysis
    model_utilization_query = """
    SELECT 
        model_name,
        SUM(total_tokens) as total_tokens,
        SUM(cost_usd) as total_cost,
        COUNT(*) as request_count,
        AVG(total_tokens) as avg_tokens_per_request,
        SUM(cost_usd) / SUM(total_tokens) * 1000 as cost_per_1k_tokens
    FROM token_usage
    WHERE request_timestamp >= CURRENT_DATE() - INTERVAL 30 DAYS
    GROUP BY model_name
    ORDER BY total_cost DESC
    """
    
    # Budget vs actual spending
    budget_analysis_query = """
    SELECT 
        u.user_name,
        l.token_limit,
        COALESCE(SUM(u.total_tokens), 0) as tokens_used,
        (COALESCE(SUM(u.total_tokens), 0) / NULLIF(l.token_limit, 0)) * 100 as usage_percentage,
        CASE 
            WHEN COALESCE(SUM(u.total_tokens), 0) >= l.token_limit * 0.9 THEN 'CRITICAL'
            WHEN COALESCE(SUM(u.total_tokens), 0) >= l.token_limit * 0.75 THEN 'WARNING'
            ELSE 'NORMAL'
        END as status
    FROM user_token_limits l
    LEFT JOIN token_usage u ON l.user_name = u.user_name 
        AND l.model_name = u.model_name
        AND u.request_timestamp >= CURRENT_DATE() - INTERVAL 30 DAYS
    GROUP BY u.user_name, l.user_name, l.token_limit
    """
    
    return {
        "daily_usage": daily_usage_query,
        "user_spending": user_spending_query,
        "model_utilization": model_utilization_query,
        "budget_analysis": budget_analysis_query
    }

# Usage
dashboard_queries = create_token_usage_dashboard()
# These queries can be used in Databricks SQL dashboards or exported to BI tools
```

## Cost Optimization Strategies

### Intelligent Model Selection
```python
def select_cost_optimal_model(task_complexity, token_budget):
    """Select the most cost-effective model for a given task"""
    
    model_options = {
        'simple_qa': {
            'models': ['gpt-3.5-turbo', 'claude-3-haiku', 'gemini-pro'],
            'expected_tokens': 100
        },
        'complex_analysis': {
            'models': ['gpt-4', 'claude-3-sonnet', 'gpt-4-turbo'],
            'expected_tokens': 500
        },
        'creative_writing': {
            'models': ['claude-3-opus', 'gpt-4', 'claude-3-sonnet'],
            'expected_tokens': 300
        }
    }
    
    if task_complexity not in model_options:
        return None
    
    options = model_options[task_complexity]
    expected_tokens = options['expected_tokens']
    
    # Calculate cost for each model
    model_costs = {}
    for model in options['models']:
        rate_per_1k = cost_rates.get(model, 0.01)
        estimated_cost = (expected_tokens / 1000) * rate_per_1k
        model_costs[model] = estimated_cost
    
    # Select cheapest model within budget
    viable_models = {k: v for k, v in model_costs.items() if v <= token_budget}
    
    if viable_models:
        return min(viable_models.items(), key=lambda x: x[1])
    else:
        return min(model_costs.items(), key=lambda x: x[1])  # Return cheapest regardless

# Usage
optimal_model, cost = select_cost_optimal_model('complex_analysis', 0.50)
print(f"Recommended: {optimal_model} at ${cost:.4f}")
```

## When to Use This Skill

- Controlling AI API costs for organizations
- Implementing user-based token quotas
- Preventing budget overruns from AI usage
- Providing fair access to AI resources
- Monitoring and optimizing AI spending
- Building cost-aware AI applications

## Integration with Existing Systems

### API Gateway Integration
```python
class APIGatewayTokenLimiter:
    """Integrate token limiting with existing API gateways"""
    
    def __init__(self, gateway_url, lakebase_connection):
        self.gateway_url = gateway_url
        self.limiter = TokenLimitedGatewayModel()
        self.limiter.conn = lakebase_connection
    
    def proxy_request(self, request_data):
        """Proxy requests through token limiter"""
        
        # Check tokens first
        token_check = self.limiter._check_token_limits(
            request_data.get('user_name'),
            request_data.get('model'),
            100  # Estimated tokens
        )
        
        if not token_check.get('approved'):
            return {
                "error": "Token limit exceeded",
                "details": token_check
            }
        
        # Forward to actual API
        response = requests.post(self.gateway_url, json=request_data)
        
        # Log usage
        if response.status_code == 200:
            self.limiter._log_token_usage(
                request_data.get('user_name'),
                request_data.get('model'),
                response.json().get('usage', {}).get('total_tokens', 0)
            )
        
        return response.json()

# Usage with FastAPI
from fastapi import FastAPI, HTTPException

app = FastAPI()
limiter = APIGatewayTokenLimiter("https://api.openai.com/v1/chat/completions", lakebase_conn)

@app.post("/chat/completions")
async def chat_completions(request: dict):
    result = limiter.proxy_request(request)
    if "error" in result:
        raise HTTPException(status_code=429, detail=result["error"])
    return result
```

## Related Skills

- ai-cost-optimization
- rate-limiting-patterns
- usage-tracking-analytics
- budget-monitoring-alerts
- multi-tenant-ai-systems