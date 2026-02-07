---
name: "teams-genie-integration-azure-functions"
description: "Integrate Microsoft Teams with Databricks Genie using Azure Functions: serverless Teams bot, conversational AI, OAuth 2.0 authentication, and automated resource provisioning."
---

# Integrate Teams with Genie using Azure Serverless Framework

## Overview

This skill covers building a Microsoft Teams bot that integrates with Databricks Genie using Azure Functions and Azure Bot Service. Learn how to create a serverless Teams bot, configure Azure AD authentication with OAuth 2.0, automate resource provisioning with installation scripts, and enable conversational data queries directly from Teams channels.

## Quick Start

### Automated Setup Script
Use installation script for complete setup:

```bash
#!/bin/bash
# install.sh - Automated Azure resources setup

# Configuration
RESOURCE_GROUP="databricks-genie-teams-rg"
LOCATION="eastus"
FUNCTION_APP_NAME="genie-teams-function"
STORAGE_ACCOUNT="genieteamsstorage"
BOT_NAME="databricks-genie-bot"

# Step 1: Create Resource Group
az group create --name $RESOURCE_GROUP --location $LOCATION

# Step 2: Create Storage Account
az storage account create \
    --name $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --sku Standard_LRS

# Step 3: Create Function App
az functionapp create \
    --name $FUNCTION_APP_NAME \
    --storage-account $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --consumption-plan-location $LOCATION \
    --runtime python \
    --runtime-version 3.11

# Step 4: Create App Registration & Service Principal
APP_ID=$(az ad app create \
    --display-name "Databricks Genie Teams Bot" \
    --query appId -o tsv)

# Generate client secret
CLIENT_SECRET=$(az ad app credential reset \
    --id $APP_ID \
    --query password -o tsv)

# Step 5: Add Azure Databricks API permission
az ad app permission add \
    --id $APP_ID \
    --api 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d \
    --api-permissions 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d=Scope

# Step 6: Configure Function App settings
az functionapp config appsettings set \
    --name $FUNCTION_APP_NAME \
    --resource-group $RESOURCE_GROUP \
    --settings \
        DATABRICKS_WORKSPACE_URL=$WORKSPACE_URL \
        GENIE_SPACE_ID=$GENIE_SPACE_ID \
        AZURE_CLIENT_ID=$APP_ID \
        AZURE_CLIENT_SECRET=$CLIENT_SECRET \
        AZURE_TENANT_ID=$TENANT_ID

# Step 7: Deploy Function App code
func azure functionapp publish $FUNCTION_APP_NAME

# Step 8: Create Bot Service
az bot create \
    --name $BOT_NAME \
    --resource-group $RESOURCE_GROUP \
    --appid $APP_ID \
    --password $CLIENT_SECRET \
    --kind function

# Step 9: Enable Teams channel
az bot msteams create \
    --name $BOT_NAME \
    --resource-group $RESOURCE_GROUP

echo "Setup complete! App ID: $APP_ID"
```

### Function App Implementation
Azure Function for Teams bot:

```python
import azure.functions as func
import logging
from botbuilder.core import TurnContext, ActivityHandler, MessageFactory
from botbuilder.schema import ChannelAccount
from databricks.sdk import WorkspaceClient
from databricks.genai import GenieClient
import os

class GenieTeamsBot(ActivityHandler):
    """Teams bot handler for Genie integration"""
    
    def __init__(self):
        self.workspace_url = os.environ["DATABRICKS_WORKSPACE_URL"]
        self.genie_space_id = os.environ["GENIE_SPACE_ID"]
        
        # Initialize Databricks client with service principal
        self.w = WorkspaceClient(
            host=self.workspace_url,
            azure_client_id=os.environ["AZURE_CLIENT_ID"],
            azure_client_secret=os.environ["AZURE_CLIENT_SECRET"],
            azure_tenant_id=os.environ["AZURE_TENANT_ID"]
        )
        self.genie_client = GenieClient(self.w)
    
    async def on_message_activity(self, turn_context: TurnContext):
        """Handle incoming messages"""
        user_message = turn_context.activity.text
        
        # Query Genie
        genie_response = await self.query_genie(user_message)
        
        # Send response
        await turn_context.send_activity(
            MessageFactory.text(genie_response)
        )
    
    async def query_genie(self, question: str) -> str:
        """Query Databricks Genie"""
        response = self.genie_client.query(
            space_id=self.genie_space_id,
            question=question
        )
        return response.answer

# Azure Function entry point
def main(req: func.HttpRequest) -> func.HttpResponse:
    """Azure Function HTTP trigger"""
    from botbuilder.core import BotFrameworkAdapter
    from botbuilder.schema import Activity
    
    adapter = BotFrameworkAdapter(
        settings={
            "app_id": os.environ["AZURE_CLIENT_ID"],
            "app_password": os.environ["AZURE_CLIENT_SECRET"]
        }
    )
    
    bot = GenieTeamsBot()
    
    async def process_request():
        activity = Activity().deserialize(req.get_json())
        await adapter.process_activity(activity, "", bot.on_turn)
    
    # Process request
    import asyncio
    asyncio.run(process_request())
    
    return func.HttpResponse("OK", status_code=200)
```

## Common Patterns

### Pattern 1: Service Principal Setup
Configure service principal for Databricks access:

```python
def setup_service_principal():
    """Setup service principal for Databricks access"""
    
    # Step 1: Add service principal at account level
    # Via Databricks Account Console:
    # Account Settings > Identity and Access > Service Principals
    # Add service principal using App ID from Azure AD
    
    # Step 2: Add service principal at workspace level
    # Via Workspace UI:
    # Settings > Identity and Access > Service Principals
    # Add same service principal
    
    # Step 3: Grant Genie Space access
    service_principal_id = "databricks-genie-teams-bot@tenant.onmicrosoft.com"
    genie_space_id = os.environ["GENIE_SPACE_ID"]
    
    w = WorkspaceClient(
        host=os.environ["DATABRICKS_WORKSPACE_URL"],
        azure_client_id=os.environ["AZURE_CLIENT_ID"],
        azure_client_secret=os.environ["AZURE_CLIENT_SECRET"],
        azure_tenant_id=os.environ["AZURE_TENANT_ID"]
    )
    
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
    
    # Step 4: Grant Unity Catalog access
    spark.sql(f"""
        GRANT SELECT ON TABLE catalog.schema.table_name
        TO `{service_principal_id}`
    """)
    
    print("Service principal configured!")

# Usage
setup_service_principal()
```

### Pattern 2: Teams App Package Creation
Create Teams app manifest:

```json
{
  "$schema": "https://developer.microsoft.com/json-schemas/teams/v1.16/MicrosoftTeams.schema.json",
  "manifestVersion": "1.16",
  "version": "1.0.0",
  "id": "YOUR_APP_ID",
  "packageName": "com.databricks.genie",
  "developer": {
    "name": "Databricks",
    "websiteUrl": "https://databricks.com",
    "privacyUrl": "https://databricks.com/privacy",
    "termsOfUseUrl": "https://databricks.com/terms"
  },
  "name": {
    "short": "DB Genie",
    "full": "Databricks Genie Bot"
  },
  "description": {
    "short": "Query your data with natural language",
    "full": "Databricks Genie Bot allows you to query and interact with your data using natural language directly from Microsoft Teams."
  },
  "icons": {
    "outline": "outline.png",
    "color": "color.png"
  },
  "accentColor": "#0078D4",
  "bots": [
    {
      "botId": "YOUR_BOT_ID",
      "scopes": ["personal", "team", "groupchat"],
      "commandLists": [
        {
          "scopes": ["personal", "team", "groupchat"],
          "commands": [
            {
              "title": "Query Data",
              "description": "Ask questions about your data"
            }
          ]
        }
      ]
    }
  ],
  "permissions": ["identity", "messageTeamMembers"],
  "validDomains": ["*.databricks.com"]
}
```

### Pattern 3: Threaded Conversations
Maintain context in Teams threads:

```python
class GenieTeamsBot(ActivityHandler):
    """Bot with threaded conversation support"""
    
    async def on_message_activity(self, turn_context: TurnContext):
        """Handle messages with thread context"""
        
        # Get conversation reference
        conversation_id = turn_context.activity.conversation.id
        thread_id = turn_context.activity.conversation.thread_id
        
        # Retrieve thread history
        thread_history = await self.get_thread_history(conversation_id, thread_id)
        
        # Add current message
        thread_history.append({
            "role": "user",
            "content": turn_context.activity.text
        })
        
        # Query Genie with context
        genie_response = await self.query_genie_with_context(thread_history)
        
        # Send response
        await turn_context.send_activity(
            MessageFactory.text(genie_response)
        )
    
    async def get_thread_history(self, conversation_id: str, thread_id: str):
        """Retrieve conversation history from Teams thread"""
        # Implementation to fetch thread messages
        # Use Bot Framework SDK or Teams API
        pass
    
    async def query_genie_with_context(self, history: list) -> str:
        """Query Genie with conversation context"""
        # Build context-aware query
        context = "\n".join([f"{msg['role']}: {msg['content']}" for msg in history])
        
        response = self.genie_client.query(
            space_id=self.genie_space_id,
            question=context
        )
        return response.answer
```

## Reference Files

- [Azure Functions](https://docs.microsoft.com/en-us/azure/azure-functions/) - Serverless compute
- [Bot Framework](https://docs.microsoft.com/en-us/azure/bot-service/) - Bot development
- [Teams App Manifest](https://docs.microsoft.com/en-us/microsoftteams/platform/resources/schema/manifest-schema) - App configuration

## Common Issues

| Issue | Solution |
|-------|----------|
| **Authentication fails** | Verify service principal added at account and workspace level |
| **Genie access denied** | Grant CAN_RUN permission on Genie Space |
| **Function app not responding** | Check app settings, verify bot service configuration |
| **Teams app not installing** | Verify manifest.json, check bot ID matches |
| **OAuth errors** | Verify Azure AD app registration and permissions |

## Key Takeaways

1. **Automated Setup**: Use install.sh script for complete resource provisioning
2. **Azure Functions**: Serverless compute for bot logic
3. **Service Principal**: Add at both account and workspace levels
4. **OAuth 2.0**: Azure AD authentication for Databricks access
5. **Teams App Package**: Create manifest.json and zip for Teams installation
6. **Threaded Conversations**: Maintain context using conversation/thread IDs

## Complete Deployment Workflow

```python
def complete_teams_genie_deployment():
    """Complete deployment workflow"""
    
    # Step 1: Run installation script
    # bash install.sh
    
    # Step 2: Get App ID from script output
    app_id = "xxxxxx-xxx-xxx-xxxxx-xxxxxxx"
    
    # Step 3: Add service principal to Databricks
    # Account level: Account Console > Service Principals
    # Workspace level: Workspace Settings > Service Principals
    
    # Step 4: Grant permissions
    setup_service_principal()
    
    # Step 5: Upload Teams app package
    # Teams Admin Center > Manage apps > Upload
    # Use databricks-genie-teams-app-xxxxxx.zip
    
    # Step 6: Test in Teams
    # Add bot to team/channel
    # Send message: @DB Genie show me sales data
    
    print("Deployment complete!")

# Usage
complete_teams_genie_deployment()
```

## When to Use This Skill

- Building Teams bots for data access
- Integrating Genie with Microsoft Teams
- Using Azure serverless infrastructure
- Enabling conversational data queries
- Automating resource provisioning

## Related Skills

- azure-functions-development
- teams-bot-development
- azure-ad-authentication
- genie-api-integration