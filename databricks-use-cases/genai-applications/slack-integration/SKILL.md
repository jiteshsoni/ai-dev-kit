---
name: slack-integrated-genai
description: Build Slack-integrated GenAI applications on Databricks for agent feedback collection, conversational data access via Genie, and team collaboration. Covers Databricks Apps, MLflow tracing, and interactive Slack components.
author: Veena Ramesh, Artem Chebotko, Debu Sinha
type: use-case-collection
source_blogs:
  - title: "Trace your steps back to Slack"
    url: https://www.databricksters.com/p/trace-your-steps-back-to-slack
    author: Veena Ramesh
  - title: "Integrate Slack with Genie natively using Databricks Apps in 30mins!"
    url: https://www.databricksters.com/p/integrate-slack-with-genie-natively
    author: Artem Chebotko
  - title: "Integrate Teams with Genie using webhooks"
    url: https://www.databricksters.com/p/integrate-teams-with-genie-using
    author: Debu Sinha
---

# Slack-Integrated GenAI Applications on Databricks

## Overview

This use case demonstrates how to integrate Databricks GenAI capabilities with Slack for enhanced team collaboration. By bringing AI agents and data access directly into Slack workflows, teams can interact with data and provide feedback without switching contexts.

**Business Value:**
- Collect human feedback on AI agents within existing Slack workflows
- Enable conversational data access via Databricks Genie
- Reduce context switching between tools
- Democratize data access for non-technical team members
- Build trust through transparent AI interactions

**Three Integration Patterns:**
1. **Agent Feedback Bot** - Interactive feedback collection on AI responses
2. **Genie Data Assistant** - Natural language data queries in Slack
3. **Custom Review Workflows** - SME labeling sessions via Slack

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│     Slack       │◄───▶│  Databricks App  │◄───▶│   MLflow Agent  │
│  (User Interface)│     │  (Hosting Layer) │     │  (Model/Agent)  │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                               │                           │
                               ▼                           ▼
                        ┌──────────────────┐      ┌─────────────────┐
                        │   Unity Catalog  │      │  MLflow Tracing │
                        │   (Data/Governance)│    │  (Feedback)     │
                        └──────────────────┘      └─────────────────┘
```

## Pattern 1: Agent Feedback Collection Bot

Build a Slack bot that enables real-time agent interaction and feedback collection via MLflow.

### Prerequisites

- Databricks workspace with MLflow and Model Serving enabled
- Slack workspace with admin access
- Databricks Apps enabled

### Step 1: Create Slack App

1. Go to [Slack API Apps](https://api.slack.com/apps)
2. Create New App → From scratch
3. Add the following **Bot Token Scopes**:
   - `chat:write` - Send messages
   - `groups:read` - Read private channel info
   - `im:read` - Read direct message info
   - `mpim:history` - Read group DM history
   - `commands` - Add slash commands

4. Enable **Socket Mode** for real-time event handling
5. Install app to workspace and note:
   - Bot User OAuth Token (starts with `xoxb-`)
   - App-Level Token (starts with `xapp-`)
   - Signing Secret

### Step 2: Create Databricks App

```bash
# Create the app
databricks apps create slack-feedback-bot

# Deploy from local files
databricks sync . "/Users/$DATABRICKS_USERNAME/slack-bot"
databricks apps deploy slack-feedback-bot --source-code-path "/Workspace/Users/$DATABRICKS_USERNAME/slack-bot"
```

### Step 3: Create Labeling Session

Create an MLflow labeling session to store feedback:

```python
import mlflow.genai.labeling as labeling
import mlflow.genai.label_schemas as schemas

# Create labeling session for feedback collection
session = labeling.create_labeling_session(
    name="agent_feedback_session",
    assigned_users=["sme1@company.com", "sme2@company.com"],
    label_schemas=[schemas.EXPECTED_FACTS, schemas.RESPONSE_QUALITY]
)

# Store the run ID for later use
LABELING_RUN_ID = session.mlflow_run_id
```

### Step 4: Implement Slack Bot Code

```python
import os
import ssl
import json
import logging
from slack_sdk import WebClient
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from databricks.sdk import WorkspaceClient
from mlflow.deployments import get_deploy_client

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
ENDPOINT_NAME = "your-agent-endpoint"
LABELING_RUN_ID = "your-labeling-session-run-id"

def get_slack_auth():
    """Retrieve Slack tokens from Databricks Secrets."""
    w = WorkspaceClient()
    token_bot = dbutils.secrets.get(scope="slack-scope", key="bot-token")
    return token_bot

def start_slack_client():
    """Initialize Slack client with Socket Mode."""
    logger.info("Initializing Slack client...")
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    token_bot = get_slack_auth()
    client = WebClient(token=token_bot, ssl=ssl_context)
    return App(client=client, process_before_response=False)

# Initialize app
app = start_slack_client()
mlflow_client = get_deploy_client("databricks")

@app.event("message")
def handle_message(event, say, client):
    """Process incoming messages and generate agent responses."""
    user_id = event.get("user")
    message_text = event.get("text", "")
    channel_id = event.get("channel")
    thread_ts = event.get("ts")
    
    logger.info(f"Message received - User: {user_id}, Text: {message_text[:50]}...")
    
    # Skip bot messages
    if event.get("bot_id"):
        return
    
    # Prepare conversation history for context
    history = []
    if thread_ts:
        # Get thread messages for context
        thread_messages = get_thread_messages(client, channel_id, thread_ts)
        history = [
            {"role": "user" if msg.get("user") != client.auth_test().data["user_id"] else "assistant", 
             "content": msg.get("text", "")}
            for msg in thread_messages[:-1]  # Exclude current message
        ]
    
    # Call agent endpoint with trace enabled
    input_data = {
        "input": history + [{"role": "user", "content": message_text}],
        "databricks_options": {"return_trace": True}
    }
    
    try:
        response = mlflow_client.predict(endpoint=ENDPOINT_NAME, inputs=input_data)
        
        # Extract trace ID for feedback tracking
        trace_id = response["databricks_output"]["trace"]["info"]["trace_id"]
        agent_response = response["choices"][0]["message"]["content"]
        
        # Link trace to labeling session
        link_trace_to_run(LABELING_RUN_ID, trace_id)
        
        # Post response with metadata
        client.chat_postMessage(
            channel=channel_id,
            text=agent_response,
            thread_ts=thread_ts,
            metadata={
                "event_type": "agent_response",
                "event_payload": {
                    "trace_id": trace_id,
                    "thread_id": thread_ts,
                    "resource_type": "AGENT_RESPONSE"
                }
            }
        )
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        say(f"Sorry, I encountered an error: {str(e)}", thread_ts=thread_ts)

def get_thread_messages(client, channel, thread_ts):
    """Retrieve messages from a thread."""
    response = client.conversations_replies(
        channel=channel,
        ts=thread_ts,
        inclusive=True,
        limit=50
    )
    return response["messages"]

def link_trace_to_run(run_id, trace_id):
    """Link trace to labeling session run."""
    from databricks.sdk.core import ApiClient
    
    creds = get_databricks_host_creds()
    url = f"{creds.host}/api/2.0/mlflow/traces/link-to-run"
    
    import requests
    requests.post(
        url,
        headers={"Authorization": f"Bearer {creds.token}"},
        json={"run_id": run_id, "trace_ids": [trace_id]}
    )

@app.message_shortcut("log_feedback")
def handle_feedback_shortcut(ack, shortcut, client):
    """Handle feedback submission via message shortcut."""
    ack()
    
    user = shortcut["user"]["name"]
    message = shortcut["message"]
    trace_id = message.get("metadata", {}).get("event_payload", {}).get("trace_id")
    
    if not trace_id:
        client.views_open(
            trigger_id=shortcut["trigger_id"],
            view={
                "type": "modal",
                "title": {"type": "plain_text", "text": "Error"},
                "blocks": [{
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "No trace ID found. Cannot log feedback."}
                }]
            }
        )
        return
    
    # Open feedback modal
    client.views_open(
        trigger_id=shortcut["trigger_id"],
        view={
            "type": "modal",
            "callback_id": "feedback_modal",
            "title": {"type": "plain_text", "text": "Agent Feedback"},
            "private_metadata": json.dumps({"trace_id": trace_id, "user": user}),
            "blocks": [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"Providing feedback for trace: `{trace_id[:20]}...`"}
                },
                {
                    "type": "input",
                    "block_id": "feedback_type",
                    "label": {"type": "plain_text", "text": "Feedback Type"},
                    "element": {
                        "type": "static_select",
                        "action_id": "type_select",
                        "options": [
                            {"text": {"type": "plain_text", "text": "👍 Accurate & Helpful"}, "value": "positive"},
                            {"text": {"type": "plain_text", "text": "👎 Inaccurate"}, "value": "negative"},
                            {"text": {"type": "plain_text", "text": "⚠️ Needs Improvement"}, "value": "improvement"}
                        ]
                    }
                },
                {
                    "type": "input",
                    "block_id": "feedback_comment",
                    "label": {"type": "plain_text", "text": "Comments (Optional)"},
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "comment_input",
                        "multiline": True,
                        "placeholder": {"type": "plain_text", "text": "What could be improved?"}
                    },
                    "optional": True
                }
            ],
            "submit": {"type": "plain_text", "text": "Submit Feedback"}
        }
    )

@app.view("feedback_modal")
def handle_feedback_submission(ack, body, client):
    """Process feedback form submission."""
    ack()
    
    private_data = json.loads(body["view"]["private_metadata"])
    trace_id = private_data["trace_id"]
    user = private_data["user"]
    
    # Extract form values
    values = body["view"]["state"]["values"]
    feedback_type = values["feedback_type"]["type_select"]["selected_option"]["value"]
    comment = values["feedback_comment"]["comment_input"].get("value", "")
    
    # Log feedback to MLflow
    import mlflow
    
    assessment_value = {
        "positive": 1.0,
        "negative": 0.0,
        "improvement": 0.5
    }.get(feedback_type, 0.5)
    
    mlflow.log_feedback(
        trace_id=trace_id,
        key="human_feedback",
        value=assessment_value,
        comment=comment,
        source={"type": "HUMAN", "user_id": user}
    )
    
    # Notify user
    client.chat_postMessage(
        channel=body["user"]["id"],
        text=f"✅ Feedback recorded for trace `{trace_id[:20]}...`\nType: {feedback_type}\nComment: {comment or 'None'}"
    )

# Start the app
if __name__ == "__main__":
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    handler.start()
```

### Step 5: Configure Secrets

Store sensitive credentials in Databricks Secrets:

```bash
# Create secret scope
databricks secrets create-scope slack-scope

# Store tokens
databricks secrets put --scope slack-scope --key bot-token
databricks secrets put --scope slack-scope --key app-token
```

## Pattern 2: Genie Integration Bot

Enable natural language data queries via Databricks Genie directly in Slack.

### Core Implementation

```python
import os
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.genie import GenieAPI

# Initialize clients
app = App(token=os.environ["SLACK_BOT_TOKEN"])
workspace_client = WorkspaceClient()
genie_client = GenieAPI(workspace_client.api_client)

GENIE_SPACE_ID = "your-genie-space-id"

@app.event("app_mention")
def handle_mention(event, say):
    """Handle when bot is mentioned in a channel."""
    text = event.get("text", "")
    channel = event.get("channel")
    thread_ts = event.get("thread_ts") or event.get("ts")
    
    # Remove bot mention from text
    query = text.split(">", 1)[-1].strip()
    
    # Start Genie conversation
    try:
        response = genie_client.start_conversation(
            space_id=GENIE_SPACE_ID,
            content=query
        )
        
        conversation_id = response.id
        message_id = response.message_id
        
        # Poll for response
        result = poll_genie_response(conversation_id, message_id)
        
        # Format and send response
        formatted_response = format_genie_response(result)
        
        say(
            text=formatted_response,
            thread_ts=thread_ts,
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*Query:* {query}"}
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": formatted_response}
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "👍 Helpful"},
                            "value": f"helpful:{conversation_id}",
                            "action_id": "genie_helpful"
                        },
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "👎 Not Helpful"},
                            "value": f"not_helpful:{conversation_id}",
                            "action_id": "genie_not_helpful"
                        }
                    ]
                }
            ]
        )
        
    except Exception as e:
        say(f"Sorry, I couldn't process that query: {str(e)}", thread_ts=thread_ts)

def poll_genie_response(conversation_id, message_id, max_attempts=30):
    """Poll Genie for query completion."""
    import time
    
    for _ in range(max_attempts):
        response = genie_client.get_message(
            space_id=GENIE_SPACE_ID,
            conversation_id=conversation_id,
            message_id=message_id
        )
        
        if response.status == "COMPLETED":
            return response
        elif response.status == "FAILED":
            raise Exception("Genie query failed")
        
        time.sleep(1)
    
    raise TimeoutError("Genie response timeout")

def format_genie_response(response):
    """Format Genie response for Slack."""
    parts = []
    
    if response.text_content:
        parts.append(response.text_content)
    
    if response.sql_query:
        parts.append(f"```sql\n{response.sql_query}\n```")
    
    return "\n\n".join(parts)

@app.action("genie_helpful")
def handle_helpful_feedback(ack, body, client):
    """Record positive feedback."""
    ack()
    
    value = body["actions"][0]["value"]
    conversation_id = value.split(":")[1]
    
    # Send feedback to Genie space
    genie_client.create_feedback(
        space_id=GENIE_SPACE_ID,
        conversation_id=conversation_id,
        sentiment="POSITIVE"
    )
    
    # Update message to show feedback received
    client.chat_update(
        channel=body["channel"]["id"],
        ts=body["message"]["ts"],
        text=body["message"]["text"] + "\n\n✅ Feedback: Helpful",
        blocks=body["message"]["blocks"][:-1]  # Remove buttons
    )
```

### Required Permissions

Grant the Databricks App Service Principal:
- **Genie Space**: "Can Run" permission
- **SQL Warehouse**: "Can Use" permission
- **Unity Catalog**: Read access to referenced tables

## Pattern 3: Teams Integration (Webhook-based)

For Microsoft Teams integration, use webhook-based architecture:

```python
from flask import Flask, request, jsonify
import requests
import json

app = Flask(__name__)

@app.route("/teams-webhook", methods=["POST"])
def handle_teams_message():
    """Handle incoming Teams messages."""
    data = request.json
    
    # Extract message text
    text = data.get("text", "")
    conversation_id = data.get("conversation", {}).get("id")
    
    # Process with Genie
    response = query_genie(text)
    
    # Send response back to Teams
    send_teams_reply(conversation_id, response)
    
    return jsonify({"status": "ok"})

def query_genie(query):
    """Query Genie and return formatted response."""
    # Implementation similar to Slack pattern
    pass

def send_teams_reply(conversation_id, message):
    """Send reply to Teams conversation."""
    teams_webhook_url = "https://smba.trafficmanager.net/..."
    
    payload = {
        "type": "message",
        "conversation": {"id": conversation_id},
        "text": message
    }
    
    requests.post(teams_webhook_url, json=payload)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
```

## Best Practices

### 1. Error Handling

Always wrap API calls in try-except blocks:

```python
def safe_api_call(func, fallback_message="Service temporarily unavailable"):
    """Wrapper for safe API calls with fallback."""
    try:
        return func()
    except Exception as e:
        logger.error(f"API call failed: {e}")
        return {"error": fallback_message}
```

### 2. Rate Limiting

Implement rate limiting to prevent abuse:

```python
from functools import wraps
import time

user_last_request = {}

def rate_limit(max_requests=10, window=60):
    """Rate limit decorator for Slack handlers."""
    def decorator(func):
        @wraps(func)
        def wrapper(event, *args, **kwargs):
            user_id = event.get("user")
            now = time.time()
            
            # Clean old entries
            for uid in list(user_last_request.keys()):
                if now - user_last_request[uid]["time"] > window:
                    del user_last_request[uid]
            
            # Check rate
            if user_id in user_last_request:
                if user_last_request[user_id]["count"] >= max_requests:
                    return {"error": "Rate limit exceeded. Please try again later."}
                user_last_request[user_id]["count"] += 1
            else:
                user_last_request[user_id] = {"time": now, "count": 1}
            
            return func(event, *args, **kwargs)
        return wrapper
    return decorator
```

### 3. Thread Management

Maintain conversation context using threads:

```python
def get_conversation_context(client, channel, thread_ts, max_messages=10):
    """Retrieve conversation context for better responses."""
    messages = client.conversations_replies(
        channel=channel,
        ts=thread_ts,
        limit=max_messages
    )["messages"]
    
    # Format as conversation history
    context = []
    for msg in messages:
        role = "assistant" if msg.get("bot_id") else "user"
        context.append({"role": role, "content": msg.get("text", "")})
    
    return context
```

## Deployment Checklist

- [ ] Create Slack app with required scopes
- [ ] Store credentials in Databricks Secrets
- [ ] Create Databricks App with proper permissions
- [ ] Create MLflow labeling session for feedback
- [ ] Configure Genie space permissions (if using)
- [ ] Test in development workspace first
- [ ] Set up monitoring and alerting
- [ ] Document slash commands and usage

## Attribution

This use case synthesizes content from:

1. **Veena Ramesh** - Agent feedback collection bot with MLflow integration
2. **Artem Chebotko** - Genie integration with conversational APIs
3. **Debu Sinha** - Teams webhook integration patterns

## Related Skills

- [databricks-genie](../databricks-skills/databricks-genie/SKILL.md) - Genie space configuration
- [agent-bricks](../databricks-skills/agent-bricks/SKILL.md) - Low-code agent development
- [databricks-app-python](../databricks-skills/databricks-app-python/SKILL.md) - Databricks Apps development
