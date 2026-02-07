---
name: "slack-genie-chat-integration"
description: "Chat with your data in Slack using Databricks Genie: AWS Lambda integration, API Gateway setup, DynamoDB for conversation state, and Genie Conversational API integration."
---

# Chat with Your Data in Slack using Databricks Genie

## Overview

This skill covers integrating Slack with Databricks Genie to enable natural language data queries directly from Slack. Learn how to set up AWS Lambda functions, configure API Gateway, use DynamoDB for conversation state management, and integrate with Genie's Conversational API to provide answers with reasoning and insights.

## Quick Start

### Architecture Overview
```
Slack → API Gateway → Lambda → Genie Conversational API
                ↓
            DynamoDB (conversation state)
```

### Lambda Function Setup
Basic Lambda handler:

```python
import json
import boto3
import requests
from databricks.sdk import WorkspaceClient

dynamodb = boto3.resource('dynamodb')
conversations_table = dynamodb.Table('genie-slack-conversations')

def lambda_handler(event, context):
    """Handle Slack message and query Genie"""
    
    # Parse Slack event
    body = json.loads(event['body'])
    
    # Verify Slack request (check signature)
    if not verify_slack_request(event):
        return {'statusCode': 401}
    
    # Handle Slack challenge (URL verification)
    if 'challenge' in body:
        return {'statusCode': 200, 'body': body['challenge']}
    
    # Extract message
    event_data = body.get('event', {})
    user_message = event_data.get('text', '')
    channel = event_data.get('channel', '')
    user = event_data.get('user', '')
    ts = event_data.get('ts', '')
    
    # Get or create conversation context
    conversation_id = f"{channel}_{user}"
    context = get_conversation_context(conversation_id)
    
    # Query Genie
    genie_response = query_genie(user_message, context)
    
    # Update conversation context
    update_conversation_context(conversation_id, user_message, genie_response)
    
    # Post response to Slack
    post_to_slack(channel, genie_response, ts)
    
    return {'statusCode': 200}

def query_genie(question: str, context: list = None):
    """Query Databricks Genie Conversational API"""
    
    w = WorkspaceClient(
        host=os.environ['DATABRICKS_WORKSPACE_URL'],
        token=os.environ['DATABRICKS_TOKEN']
    )
    
    # Build conversation history
    messages = context or []
    messages.append({"role": "user", "content": question})
    
    # Call Genie Conversational API
    response = requests.post(
        f"{os.environ['DATABRICKS_WORKSPACE_URL']}/api/2.0/genie/conversations",
        headers={
            "Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}",
            "Content-Type": "application/json"
        },
        json={
            "space_id": os.environ['GENIE_SPACE_ID'],
            "messages": messages
        }
    )
    
    genie_data = response.json()
    
    # Extract answer and reasoning
    answer = genie_data.get('answer', '')
    reasoning = genie_data.get('reasoning', '')
    insights = genie_data.get('insights', [])
    
    # Format response for Slack
    formatted_response = format_genie_response(answer, reasoning, insights)
    
    return formatted_response

def format_genie_response(answer: str, reasoning: str, insights: list) -> str:
    """Format Genie response for Slack"""
    
    response_text = f"*Answer:*\n{answer}\n\n"
    
    if reasoning:
        response_text += f"*Reasoning:*\n{reasoning}\n\n"
    
    if insights:
        response_text += "*Additional Insights:*\n"
        for insight in insights:
            response_text += f"• {insight}\n"
    
    return response_text
```

## Common Patterns

### Pattern 1: Conversation State Management
Track conversations in DynamoDB:

```python
def get_conversation_context(conversation_id: str) -> list:
    """Retrieve conversation history from DynamoDB"""
    
    try:
        response = conversations_table.get_item(
            Key={'conversation_id': conversation_id}
        )
        
        if 'Item' in response:
            return response['Item'].get('messages', [])
    except Exception as e:
        print(f"Error retrieving context: {e}")
    
    return []

def update_conversation_context(conversation_id: str, user_message: str, genie_response: str):
    """Update conversation history in DynamoDB"""
    
    # Get existing context
    context = get_conversation_context(conversation_id)
    
    # Add new messages
    context.append({"role": "user", "content": user_message})
    context.append({"role": "assistant", "content": genie_response})
    
    # Keep last 20 messages (limit context size)
    context = context[-20:]
    
    # Update DynamoDB
    conversations_table.put_item(
        Item={
            'conversation_id': conversation_id,
            'messages': context,
            'last_updated': datetime.utcnow().isoformat()
        }
    )
```

### Pattern 2: Slack Message Formatting
Format responses with Slack Block Kit:

```python
def format_slack_blocks(answer: str, reasoning: str, sql_query: str = None):
    """Format response as Slack blocks"""
    
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Answer:*\n{answer}"
            }
        }
    ]
    
    if reasoning:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Reasoning:*\n{reasoning}"
            }
        })
    
    if sql_query:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*SQL Query:*\n```{sql_query}```"
            }
        })
    
    # Add feedback buttons
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

def post_to_slack(channel: str, response: str, thread_ts: str = None):
    """Post response to Slack"""
    
    slack_token = os.environ['SLACK_BOT_TOKEN']
    
    payload = {
        "channel": channel,
        "text": response,
        "thread_ts": thread_ts  # Reply in thread
    }
    
    requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {slack_token}",
            "Content-Type": "application/json"
        },
        json=payload
    )
```

### Pattern 3: Handle Duplicate Messages
Prevent processing same message twice:

```python
def is_duplicate_message(channel: str, ts: str) -> bool:
    """Check if message already processed"""
    
    message_id = f"{channel}_{ts}"
    
    try:
        response = conversations_table.get_item(
            Key={'conversation_id': message_id}
        )
        return 'Item' in response
    except:
        return False

def mark_message_processed(channel: str, ts: str):
    """Mark message as processed"""
    
    message_id = f"{channel}_{ts}"
    
    conversations_table.put_item(
        Item={
            'conversation_id': message_id,
            'processed': True,
            'timestamp': datetime.utcnow().isoformat()
        }
    )
```

## Reference Files

- [Genie Conversational API](https://docs.databricks.com/en/generative-ai/genie/conversation-api.html) - API documentation
- [Slack API](https://api.slack.com/) - Slack integration guide
- [AWS Lambda](https://docs.aws.amazon.com/lambda/) - Lambda documentation

## Common Issues

| Issue | Solution |
|-------|----------|
| **Slack signature verification fails** | Verify signing secret, check timestamp |
| **Genie API errors** | Check workspace URL, token, Genie Space ID |
| **Conversation context lost** | Ensure DynamoDB table exists, check permissions |
| **Duplicate messages** | Track message IDs in DynamoDB |
| **Thread context not maintained** | Use thread_ts for replies |

## Key Takeaways

1. **API Gateway**: Interface between Slack and Lambda
2. **Lambda Function**: Handles messages, queries Genie, posts responses
3. **DynamoDB**: Stores conversation state and message tracking
4. **Genie Conversational API**: Natural language to SQL conversion
5. **Thread Replies**: Use thread_ts to maintain conversation context
6. **Message Deduplication**: Track processed messages to avoid duplicates

## Complete Setup Workflow

```python
def complete_slack_genie_setup():
    """Complete setup workflow"""
    
    # Step 1: Set up AIBI Genie Space
    # - Create Genie Space in Databricks
    # - Configure with datasets
    # - Note Genie Space ID
    
    # Step 2: Create Slack App
    # - Create app at api.slack.com/apps
    # - Configure OAuth scopes
    # - Get bot token and signing secret
    
    # Step 3: Set up AWS Resources
    # - Create DynamoDB table: genie-slack-conversations
    # - Create Lambda function
    # - Create API Gateway REST API
    # - Configure Lambda integration
    
    # Step 4: Configure Lambda Environment
    # - DATABRICKS_WORKSPACE_URL
    # - DATABRICKS_TOKEN
    # - GENIE_SPACE_ID
    # - SLACK_BOT_TOKEN
    # - SLACK_SIGNING_SECRET
    
    # Step 5: Deploy and Test
    # - Deploy Lambda function
    # - Configure Slack event subscriptions
    # - Test with sample message
    
    print("Slack-Genie integration ready!")

# Usage
complete_slack_genie_setup()
```

## When to Use This Skill

- Building Slack bots for data access
- Enabling natural language queries from Slack
- Integrating Genie with collaboration tools
- Providing data insights in Slack channels
- Creating conversational data interfaces

## Related Skills

- slack-bot-development
- aws-lambda-integration
- genie-conversational-api
- dynamodb-state-management