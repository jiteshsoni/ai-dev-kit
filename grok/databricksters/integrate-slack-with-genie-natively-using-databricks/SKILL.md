---
name: "slack-genie-integration-databricks-apps"
description: "Integrate Slack with Databricks Genie using Databricks Apps: conversational AI bot, threaded conversations, real-time updates via socket mode, and Unity Catalog governance."
---

# Integrate Slack with Genie using Databricks Apps

## Overview

This skill covers building a Slack bot that integrates with Databricks Genie using Databricks Apps. Learn how to create a conversational AI bot that allows users to query data in natural language directly from Slack, maintain threaded conversations, provide rich responses with query results, and ensure secure access through Unity Catalog governance.

## Quick Start

### Slack App Setup
Configure Slack app with required permissions:

```python
# Slack App Configuration
SLACK_BOT_SCOPES = [
    "app_mentions:read",
    "chat:write",
    "im:history",
    "im:read",
    "im:write"
]

SLACK_EVENTS = [
    "app_mention",
    "message.im"
]

# Required tokens:
# - Bot User OAuth Token (xoxb-...)
# - App-Level Token for Socket Mode (xapp-...)
# - Signing Secret
```

### Databricks App Configuration
Set up Databricks App with Genie integration:

```yaml
# app.yaml configuration
name: databricks-genie-bot-app
config:
  slack:
    bot_token: ${SLACK_BOT_TOKEN}
    app_token: ${SLACK_APP_TOKEN}
    signing_secret: ${SLACK_SIGNING_SECRET}
  databricks:
    workspace_url: ${WORKSPACE_URL}
    genie_space_id: ${GENIE_SPACE_ID}
```

### Basic Bot Implementation
Simple Slack bot handler:

```python
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from databricks.sdk import WorkspaceClient
from databricks.genai import GenieClient

def initialize_bot():
    """Initialize Slack bot with Genie integration"""
    
    # Get credentials from Databricks secrets
    bot_token = dbutils.secrets.get(scope="slack", key="bot_token")
    app_token = dbutils.secrets.get(scope="slack", key="app_token")
    
    # Initialize Slack app
    app = App(token=bot_token)
    
    # Initialize Genie client
    w = WorkspaceClient()
    genie_client = GenieClient(w)
    
    return app, genie_client

# Handle direct messages
@app.event("message")
def handle_message(event, say, client):
    """Handle direct messages to bot"""
    user_message = event['text']
    channel = event['channel']
    thread_ts = event.get('ts')
    
    # Query Genie
    genie_response = query_genie(user_message)
    
    # Respond in thread
    client.chat_postMessage(
        channel=channel,
        text=genie_response,
        thread_ts=thread_ts
    )

# Handle mentions
@app.event("app_mention")
def handle_mention(event, say):
    """Handle bot mentions in channels"""
    user_message = event['text']
    
    # Query Genie
    genie_response = query_genie(user_message)
    
    # Respond
    say(genie_response, thread_ts=event['ts'])

def query_genie(question: str) -> str:
    """Query Databricks Genie"""
    w = WorkspaceClient()
    genie_client = GenieClient(w)
    
    response = genie_client.query(
        space_id=GENIE_SPACE_ID,
        question=question
    )
    
    return response.answer

# Start bot
if __name__ == "__main__":
    app, genie_client = initialize_bot()
    handler = SocketModeHandler(app, app_token)
    handler.start()
```

## Common Patterns

### Pattern 1: Threaded Conversations
Maintain context across messages:

```python
def get_thread_history(client, channel, thread_ts):
    """Retrieve conversation history from thread"""
    response = client.conversations_replies(
        channel=channel,
        ts=thread_ts,
        inclusive=True,
        limit=20
    )
    
    # Build conversation context
    messages = []
    for msg in response['messages']:
        if 'user' in msg:
            messages.append({
                "role": "user",
                "content": msg['text']
            })
        elif 'bot_id' in msg:
            messages.append({
                "role": "assistant",
                "content": msg['text']
            })
    
    return messages

@app.event("message")
def handle_threaded_message(event, say, client):
    """Handle messages with conversation context"""
    thread_ts = event.get('thread_ts') or event['ts']
    
    # Get conversation history
    history = get_thread_history(client, event['channel'], thread_ts)
    
    # Add current message
    history.append({
        "role": "user",
        "content": event['text']
    })
    
    # Query Genie with context
    genie_response = query_genie_with_context(history)
    
    # Respond in thread
    client.chat_postMessage(
        channel=event['channel'],
        text=genie_response,
        thread_ts=thread_ts
    )
```

### Pattern 2: Rich Responses with Blocks
Format responses with Slack Block Kit:

```python
def format_genie_response(response_data):
    """Format Genie response as Slack blocks"""
    
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Query:* {response_data['question']}\n\n*Answer:* {response_data['answer']}"
            }
        }
    ]
    
    # Add query results if available
    if 'results' in response_data:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"```{response_data['results']}```"
            }
        })
    
    # Add feedback button
    blocks.append({
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "👍 Helpful"},
                "value": "helpful",
                "action_id": "feedback_helpful"
            },
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "👎 Not Helpful"},
                "value": "not_helpful",
                "action_id": "feedback_not_helpful"
            }
        ]
    })
    
    return blocks

@app.event("message")
def handle_message_with_blocks(event, say, client):
    """Send rich formatted response"""
    genie_response = query_genie(event['text'])
    
    blocks = format_genie_response(genie_response)
    
    client.chat_postMessage(
        channel=event['channel'],
        blocks=blocks,
        thread_ts=event.get('thread_ts')
    )
```

### Pattern 3: Service Principal Permissions
Grant required permissions:

```python
def grant_genie_permissions(service_principal_id: str, genie_space_id: str):
    """Grant Genie access to service principal"""
    
    w = WorkspaceClient()
    
    # Grant Genie Space access
    w.permissions.set(
        request_object_type="genie_spaces",
        request_object_id=genie_space_id,
        access_control_list=[
            {
                "principal": service_principal_id,
                "permission_level": "CAN_RUN"
            }
        ]
    )
    
    # Grant warehouse access
    warehouse_id = dbutils.secrets.get(scope="config", key="warehouse_id")
    w.permissions.set(
        request_object_type="sql/warehouses",
        request_object_id=warehouse_id,
        access_control_list=[
            {
                "principal": service_principal_id,
                "permission_level": "CAN_USE"
            }
        ]
    )
    
    # Grant Unity Catalog table access
    # Grant SELECT on required tables/views
    spark.sql(f"""
        GRANT SELECT ON TABLE catalog.schema.table_name
        TO `{service_principal_id}`
    """)
    
    print(f"Permissions granted to {service_principal_id}")

# Usage
service_principal = "databricks-genie-bot-app-service-principal"
grant_genie_permissions(service_principal, GENIE_SPACE_ID)
```

## Reference Files

- [Databricks Apps](https://docs.databricks.com/en/dev-tools/apps/index.html) - App hosting platform
- [Slack Bolt Python](https://slack.dev/bolt-python/) - Slack SDK
- [Genie API](https://docs.databricks.com/en/generative-ai/genie/index.html) - Genie documentation

## Common Issues

| Issue | Solution |
|-------|----------|
| **Socket mode not working** | Ensure app-level token has connections:write scope |
| **Bot not responding** | Check event subscriptions, verify bot is installed |
| **Genie access denied** | Grant CAN_RUN permission on Genie Space to service principal |
| **No warehouse access** | Grant CAN_USE permission on SQL warehouse |
| **Table access denied** | Grant SELECT permissions via Unity Catalog |

## Key Takeaways

1. **Socket Mode**: Real-time updates without webhooks (requires app-level token)
2. **Service Principal**: Databricks App creates SP automatically - grant permissions
3. **Threaded Conversations**: Use thread_ts to maintain context
4. **Genie Integration**: Link Genie Space ID in app configuration
5. **Permissions**: Grant Genie Space, Warehouse, and Unity Catalog access
6. **Feedback Collection**: Use Slack buttons to collect user feedback

## Complete Setup Workflow

```python
def complete_slack_genie_setup():
    """Complete setup workflow"""
    
    # Step 1: Create Databricks App
    # Via UI: Compute > Apps > Create custom app
    # Or CLI: databricks apps create slack-genie-bot
    
    # Step 2: Get Service Principal
    w = WorkspaceClient()
    app = w.apps.get("slack-genie-bot")
    service_principal = app.service_principal_name
    
    # Step 3: Grant Permissions
    grant_genie_permissions(service_principal, GENIE_SPACE_ID)
    
    # Step 4: Store Secrets
    dbutils.secrets.put(
        scope="slack",
        key="bot_token",
        value=SLACK_BOT_TOKEN
    )
    dbutils.secrets.put(
        scope="slack",
        key="app_token",
        value=SLACK_APP_TOKEN
    )
    
    # Step 5: Deploy App
    # Upload code to workspace folder
    # Deploy via Apps UI or CLI
    
    # Step 6: Test
    # Send DM to bot or mention in channel
    
    print("Setup complete!")

# Usage
complete_slack_genie_setup()
```

## When to Use This Skill

- Building conversational AI interfaces in Slack
- Enabling natural language data queries from Slack
- Integrating Genie with collaboration tools
- Creating feedback collection mechanisms
- Building production-grade Slack bots

## Related Skills

- databricks-apps-deployment
- slack-bot-development
- genie-api-integration
- unity-catalog-permissions