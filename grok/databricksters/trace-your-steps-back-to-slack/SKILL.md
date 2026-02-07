---
name: "mlflow-tracing-slack-feedback"
description: "Build Slackbot for MLflow agent tracing and feedback collection: real-time agent interaction, trace linking to labeling sessions, and feedback annotation via Slack shortcuts."
---

# Trace Your Steps Back to Slack: MLflow Tracing & Feedback

## Overview

This skill covers building a Slackbot that enables real-time agent interaction and feedback collection for MLflow traces. Learn how to create traces from agent interactions, link traces to MLflow labeling sessions, collect feedback via Slack shortcuts, and annotate traces with human feedback for continuous improvement.

## Quick Start

### Initialize Slack Client
Set up Slack bot with MLflow integration:

```python
from slack_bolt import App
from slack_sdk import WebClient
from databricks.sdk import WorkspaceClient
import mlflow
import ssl

def get_slack_auth():
    """Get Slack bot token from Databricks secrets"""
    w = WorkspaceClient()
    token_bot = dbutils.secrets.get(scope="slack", key="slack-bot-token")
    return token_bot

def start_slack_client():
    """Initialize Slack client"""
    logger.info("Initialized slack client.")
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    token_bot = get_slack_auth()
    client = WebClient(token=token_bot, ssl=ssl_context)
    return App(client=client, process_before_response=False)

app = start_slack_client()
```

### Create Labeling Session
Set up MLflow labeling session:

```python
import mlflow.genai.labeling as labeling
import mlflow.genai.label_schemas as schemas

# Create labeling session
LABELING_SESSION = labeling.create_labeling_session(
    name="customer_service_review_jan_2024",
    assigned_users=["alice@company.com", "bob@company.com"],
    label_schemas=[schemas.EXPECTED_FACTS]  # Required: at least one schema
)

print(f"Labeling session created: {LABELING_SESSION.mlflow_run_id}")
```

### Handle Messages and Create Traces
Process messages and create MLflow traces:

```python
@app.event("message")
def llm_response(event, say, client):
    """Handle messages and create MLflow traces"""
    logger.info(f"Message received - User: {event['user']}, Text: {event['text'][:20]}...")
    
    message_text = event['text']
    channel = event['channel']
    thread_ts = event.get('thread_ts') or event['ts']
    
    # Get conversation history
    history = get_thread_messages(client, channel, thread_ts)
    
    # Call agent with trace enabled
    from mlflow.deployments import get_deploy_client
    
    mlflow_client = get_deploy_client("databricks")
    ENDPOINT_NAME = "my-agent-endpoint"
    
    input_data = {
        "input": history + [{"role": "user", "content": message_text}],
        "databricks_options": {"return_trace": True}  # Enable trace
    }
    
    response = mlflow_client.predict(endpoint=ENDPOINT_NAME, inputs=input_data)
    
    # Extract trace ID
    trace_id = response['databricks_output']['trace']['info']['trace_id']
    
    # Get agent response
    agent_response = response['predictions'][0]
    
    # Link trace to labeling session
    link_traces_to_run(
        run_id=LABELING_SESSION.mlflow_run_id,
        trace_ids=[trace_id]
    )
    
    # Respond in thread with trace ID in metadata
    result = client.chat_postMessage(
        channel=channel,
        blocks=[
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": agent_response}
            }
        ],
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
    
    logger.info(f"Trace linked - Run: {LABELING_SESSION.mlflow_run_id}, Trace: {trace_id}")
```

## Common Patterns

### Pattern 1: Collect Feedback via Shortcuts
Handle feedback collection:

```python
@app.message_shortcut("log_feedback")
def handle_log_feedback_shortcut(ack, shortcut, client):
    """Handle feedback shortcut"""
    ack()
    logger.info(f"Feedback shortcut triggered by user: {shortcut['user']['name']}")
    
    message = shortcut['message']
    message_ts = message['ts']
    
    # Get trace ID from message metadata
    metadata = message.get('metadata', {})
    event_payload = metadata.get('event_payload', {})
    trace_id = event_payload.get('trace_id')
    
    if not trace_id:
        client.chat_postMessage(
            channel=shortcut['channel']['id'],
            text="No trace ID found in message metadata"
        )
        return
    
    # Open feedback form
    client.views_open(
        trigger_id=shortcut['trigger_id'],
        view={
            "type": "modal",
            "title": {"type": "plain_text", "text": "Provide Feedback"},
            "submit": {"type": "plain_text", "text": "Submit"},
            "callback_id": "feedback_form",
            "private_metadata": trace_id,  # Store trace_id
            "blocks": [
                {
                    "type": "input",
                    "block_id": "feedback_rating",
                    "element": {
                        "type": "radio_buttons",
                        "options": [
                            {"text": {"type": "plain_text", "text": "👍 Helpful"}, "value": "helpful"},
                            {"text": {"type": "plain_text", "text": "👎 Not Helpful"}, "value": "not_helpful"}
                        ]
                    },
                    "label": {"type": "plain_text", "text": "Rating"}
                },
                {
                    "type": "input",
                    "block_id": "feedback_comment",
                    "element": {
                        "type": "plain_text_input",
                        "multiline": True
                    },
                    "label": {"type": "plain_text", "text": "Comments (optional)"},
                    "optional": True
                }
            ]
        }
    )

@app.view("feedback_form")
def handle_feedback_submission(ack, view, client):
    """Handle feedback form submission"""
    ack()
    
    # Get trace ID from private metadata
    trace_id = view['private_metadata']
    
    # Get feedback values
    rating = view['state']['values']['feedback_rating']['feedback_rating']['selected_option']['value']
    comment = view['state']['values'].get('feedback_comment', {}).get('feedback_comment', {}).get('value', '')
    
    # Log feedback to MLflow
    import mlflow
    
    mlflow.log_feedback(
        trace_id=trace_id,
        feedback={
            "rating": rating,
            "comment": comment,
            "user": view['user']['id']
        }
    )
    
    logger.info(f"Feedback logged - Trace: {trace_id}, Rating: {rating}")
    
    # Confirm to user
    client.chat_postMessage(
        channel=view['user']['id'],
        text=f"Thank you for your feedback! Rating: {rating}"
    )
```

### Pattern 2: Link Traces to Labeling Session
Connect traces to MLflow runs:

```python
def link_traces_to_run(run_id: str, trace_ids: list) -> dict:
    """Link traces to MLflow labeling session run"""
    from databricks.sdk import WorkspaceClient
    
    w = WorkspaceClient()
    creds = w.config
    
    import requests
    
    url = f"{creds.host}/api/2.0/mlflow/traces/link-to-run"
    headers = {
        "Authorization": f"Bearer {creds.token}",
        "Content-Type": "application/json"
    }
    
    data = {
        "run_id": run_id,
        "trace_ids": trace_ids
    }
    
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()
    
    return response.json()

# Usage in message handler
link_traces_to_run(
    run_id=LABELING_SESSION.mlflow_run_id,
    trace_ids=[trace_id]
)
```

### Pattern 3: Retrieve Thread History
Get conversation context:

```python
def get_thread_messages(client, channel, thread_ts):
    """Retrieve conversation history from Slack thread"""
    response = client.conversations_replies(
        channel=channel,
        ts=thread_ts,
        inclusive=True,  # Include parent message
        limit=20  # Max messages to retrieve
    )
    
    logger.info(f"Retrieved {len(response['messages'])} messages from thread {thread_ts}")
    
    # Convert to agent format
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
```

## Reference Files

- [MLflow Tracing](https://docs.databricks.com/en/mlflow3/genai/tracing/index.html) - Trace collection
- [Labeling Sessions](https://docs.databricks.com/en/mlflow3/genai/human-feedback/concepts/labeling-sessions.html) - Feedback collection
- [Slack Bolt Python](https://slack.dev/bolt-python/) - Slack SDK

## Common Issues

| Issue | Solution |
|-------|----------|
| **Trace ID not returned** | Ensure `return_trace=True` in databricks_options |
| **Feedback not logging** | Verify trace_id in message metadata, check MLflow permissions |
| **Traces not linking** | Verify run_id matches labeling session, check API permissions |
| **Thread history empty** | Check bot permissions (channels:history, groups:history) |
| **Metadata not persisting** | Use message metadata, not custom fields |

## Key Takeaways

1. **Trace Collection**: Enable `return_trace=True` in agent calls to get trace IDs
2. **Labeling Sessions**: Create MLflow labeling session to organize traces
3. **Trace Linking**: Link traces to labeling session run using API
4. **Feedback Collection**: Use Slack shortcuts to collect user feedback
5. **Metadata Storage**: Store trace_id in Slack message metadata for retrieval
6. **Thread Context**: Use Slack threads to maintain conversation history

## Complete Workflow

```python
def complete_slack_tracing_setup():
    """Complete setup workflow"""
    
    # Step 1: Create labeling session
    session = labeling.create_labeling_session(
        name="agent_review_session",
        assigned_users=["reviewer@company.com"],
        label_schemas=[schemas.EXPECTED_FACTS]
    )
    
    # Step 2: Initialize Slack bot
    app = start_slack_client()
    
    # Step 3: Handle messages (creates traces)
    @app.event("message")
    def handle_message(event, say, client):
        # ... trace creation code ...
        pass
    
    # Step 4: Collect feedback
    @app.message_shortcut("log_feedback")
    def handle_feedback(ack, shortcut, client):
        # ... feedback collection code ...
        pass
    
    # Step 5: Start bot
    from slack_bolt.adapter.socket_mode import SocketModeHandler
    handler = SocketModeHandler(app, dbutils.secrets.get(scope="slack", key="app_token"))
    handler.start()
    
    print("Slack tracing bot ready!")

# Usage
complete_slack_tracing_setup()
```

## When to Use This Skill

- Collecting feedback on agent responses
- Building review apps for MLflow traces
- Enabling real-time agent interaction
- Linking traces to labeling sessions
- Gathering human feedback for model improvement

## Related Skills

- mlflow-tracing-patterns
- slack-bot-development
- agent-feedback-collection
- labeling-sessions-mlflow