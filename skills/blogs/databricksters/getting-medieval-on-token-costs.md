---
name: getting-medieval-on-token-costs
description: 
---

# Getting Medieval on Token Costs

## Overview


## Source
- **Author:** Databricks
- **URL:** https://www.databricksters.com/p/getting-medieval-on-token-costs

## Tags
mlops, ai, databricks

## Full Content

# Getting Medieval on Token Costs

**Source:** https://www.databricksters.com/p/getting-medieval-on-token-costs

**Blog:** Databricks

---



Getting Medieval on Token Costs - by Austin

Databricksters

Subscribe

Sign in

AI &amp; ML

Getting Medieval on Token Costs

A Lakebase Powered Solution to Token-Based Rate Limiting 

Austin

Oct 24, 2025

2

Share

Don’t you hate it when your employees run up a thousand dollar tab on Claude API calls inside of a week and then hit you with this look when you tell them that was the budget for the quarter?

I might have something for that. 

Subscribe

One of the cornerstones of the Databricks value-add in AI is that we are a model provider neutral platform. We offer native pay-per-token hosting for open source model families like Llama, Gemma, and GPT OSS and we have first party connections with Claude, OpenAI, and Gemini. However, if you want to control costs, our current AI Gateway offering only allows you to do so via QPM rate limiting. QPM certainly has its use cases, but the majority of companies don’t care how many times per minute their employees or end users hit a model; they care about how much it’s going to cost them.

Luckily with Lakebase, token-based rate limiting is now possible and the implementation is simple: a user submits a request, which is then validated by the endpoint via queries to two Lakebase tables, the first to determine that user’s token limits and the second to determine how far into those limits they already are. If the user is out of tokens, a cutoff message is returned and the request does not hit the FM. Otherwise, the request is passed to the FM and the payload is written back to Lakebase so that the user’s total token count is updated. Finally, the response is returned to the end user with a message noting their remaining token balance.

Great, let’s see some code then, huh?

First we need to install 

psycopg2

:

%pip install psycopg2
dbutils.library.restartPython()

And set a few environment variables from a Lakebase instance:

import mlflow.pyfunc
import os

os.environ[’OPENAI_API_KEY’] = ‘’ # or whatever FM API key
os.environ[’DATABRICKS_TOKEN’] = ‘’
os.environ[’POSTGRES_HOST’] = ‘’
os.environ[’POSTGRES_DBNAME’] = ‘databricks_postgres’ # or ‘’
os.environ[’POSTGRES_USER’] = ‘’
os.environ[’POSTGRES_SSLMODE’] = ‘’
os.environ[’POSTGRES_PORT’] = 5432 # or ‘’
os.environ[’POSTGRES_PASSWORD’] = ‘’

For the demonstration, let’s create a couple quick example tables and populate the 

user_token_limits

 table with a record:

%sql
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

-- Insert sample user limit
INSERT INTO user_token_limits (user_name, model_name, token_limit) 
VALUES (’test.user@databricks.com’, ‘gpt-4.1-2025-04-14’, 1000);

Obviously you could do the above in the PostgreSQL editor, but might as well use the notebook since we’re here.

And now we can define our rate limiter. Note that this is 

extremely

 flexible. Any kind of rate limiting you can think up is doable as long as you can translate it into PostgreSQL. That means per user, per user per model, per user per model per unit time, and so on are all at your fingertips. I’m going to define a simple per user per model rate limit as hinted above and populate that with a token cutoff of just 1000 tokens on GPT 4.1:

import mlflow
from mlflow.types import DataType, Schema, ColSpec
import mlflow.models
import json
import pandas as pd
import psycopg2
import requests
from datetime import datetime
import os

class TokenLimitedGatewayModel(mlflow.pyfunc.PythonModel):
 def load_context(self, context):
 “”“Initialize database connection and endpoint URL”“”
 self.conn = psycopg2.connect(
 host=os.environ[’POSTGRES_HOST’],
 dbname=os.environ[’POSTGRES_DBNAME’],
 user=os.environ[’POSTGRES_USER’],
 password=os.environ[’POSTGRES_PASSWORD’],
 port=int(os.environ.get(’POSTGRES_PORT’, 5432)),
 sslmode=os.environ.get(’POSTGRES_SSLMODE’, ‘require’)
 )
 self.conn.autocommit = True
 self.cursor = self.conn.cursor()

 # FM endpoint
 self.fm_endpoint = “”

 # Get API token from environment if needed
 self.api_token = os.environ.get(’DATABRICKS_TOKEN’, ‘’)
 print(”Model context loaded successfully”)

 def predict(self, context, model_input):
 “”“Process request with token limit checking”“”

 # Handle different input types
 if isinstance(model_input, pd.DataFrame):
 # Convert DataFrame to dict and get first row
 if len(model_input) > 0:
 data = model_input.iloc[0].to_dict()
 else:
 return {”error”: “Empty input DataFrame”}
 elif isinstance(model_input, dict):
 data = model_input
 else:
 # Try to convert to dict
 try:
 data = dict(model_input)
 except:
 return {”error”: f”Unsupported input type: {type(model_input)}”}

 # Extract and parse messages
 messages = data.get(”messages”, [])
 if isinstance(messages, str):
 try:
 messages = json.loads(messages)
 except json.JSONDecodeError:
 return {”error”: “Invalid JSON in messages field”}

 # Extract parameters with defaults
 user_name = str(data.get(”user_name”, “test.user@databricks.com”))
 model_name = str(data.get(”model”, “gpt-4.1-2025-04-14”))

 # Handle max_tokens in case missing, this is on request side, not the rate limiter
 max_tokens_raw = data.get(”max_tokens”, 128)
 if pd.isna(max_tokens_raw) or max_tokens_raw is None:
 max_tokens = 128
 else:
 max_tokens = int(max_tokens_raw)

 # Handle temperature in case missing
 temperature_raw = data.get(”temperature”, 0.7)
 if pd.isna(temperature_raw) or temperature_raw is None:
 temperature = 0.7
 else:
 temperature = float(temperature_raw)

 # Check current token usage
 self.cursor.execute(”“”
 SELECT COALESCE(SUM(total_tokens), 0) as total_used
 FROM token_usage 
 WHERE user_name = %s AND model_name = %s
 “”“, (user_name, model_name))

 result = self.cursor.fetchone()
 tokens_used = int(result[0]) if result and result[0] else 0

 # Check user’s token limit
 self.cursor.execute(”“”
 SELECT token_limit 
 FROM user_token_limits 
 WHERE user_name = %s AND model_name = %s
 “”“, (user_name, model_name))

 limit_result = self.cursor.fetchone()

 if not limit_result:
 return {”error”: f”No token limit found for user {user_name} and model {model_name}”}

 token_limit = int(limit_result[0])

 # Check if limit exceeded
 if tokens_used >= token_limit:
 return {
 “error”: f”Token limit exceeded. Used: {tokens_used}, Limit: {token_limit}”,
 “tokens_used”: tokens_used,
 “token_limit”: token_limit
 }

 # Prepare request for FM endpoint
 fm_request = {
 “messages”: messages,
 “max_tokens”: max_tokens,
 “temperature”: temperature
 }

 headers = {
 “Content-Type”: “application/json”
 }

 if self.api_token:
 headers[”Authorization”] = f”Bearer {self.api_token}”

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

 # Extract token usage from response
 usage = fm_response.get(”usage”, {})
 prompt_tokens = int(usage.get(”prompt_tokens”, 0))
 completion_tokens = int(usage.get(”completion_tokens”, 0))
 total_tokens = int(usage.get(”total_tokens”, 0))

 # Log token usage to database
 self.cursor.execute(”“”
 INSERT INTO token_usage (
 user_name, 
 model_name, 
 prompt_tokens, 
 



---
*Skill auto-generated from blog content*
*Created: 2026-02-07*
