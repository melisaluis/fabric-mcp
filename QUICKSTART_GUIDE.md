# Quick Start Guide for fabric-lakehouse-mcp

Get started with fabric-lakehouse-mcp in 5 minutes!

## Prerequisites

- Python 3.10 or higher
- Azure CLI installed ([Download here](https://learn.microsoft.com/cli/azure/install-azure-cli))
- Access to Microsoft Fabric with a Lakehouse
- VS Code with the MCP extension (optional but recommended)

## Step 1: Install Azure CLI and Login

```bash
# Install Azure CLI (if not already installed)
# Visit: https://learn.microsoft.com/cli/azure/install-azure-cli

# Login to Azure
az login
```

## Step 2: Install the Package

```bash
pip install fabric-lakehouse-mcp
```

**Tip**: After installation, you can print the MCP configuration template:
```bash
fabric-lakehouse --print-config
```
This will display the exact configuration you need to copy into your MCP settings.

## Step 3: Get Your Fabric Lakehouse Information

You need two pieces of information:

1. **SQL Endpoint**: Find this in your Fabric Lakehouse
   - Go to your Lakehouse in Fabric
   - Click the "SQL Endpoint" view
   - Copy the connection string (format: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx.datawarehouse.fabric.microsoft.com`)

2. **Lakehouse Name**: The name of your lakehouse (e.g., `MyLakehouse`)

## Step 4: Configure MCP in VS Code

### Option A: Using VS Code MCP Extension

1. Open VS Code Command Palette (`Ctrl+Shift+P` or `Cmd+Shift+P`)
2. Run: `MCP: Add Server`
3. Select "fabric-lakehouse-mcp" from the list
4. Enter your configuration when prompted:
   - **SQL Endpoint**: `your-workspace-id.datawarehouse.fabric.microsoft.com`
   - **Lakehouse Name**: `MyLakehouse`

### Option B: Manual Configuration

Edit your MCP settings file:

**Windows**: `%APPDATA%\Code\User\globalStorage\saoudrizwan.claude-dev\settings\cline_mcp_settings.json`

**macOS/Linux**: `~/.vscode/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json`

Add this configuration:

```json
{
  "mcpServers": {
    "fabric-lakehouse": {
      "command": "fabric-lakehouse",
      "args": [],
      "env": {
        "FABRIC_SQL_ENDPOINT": "your-workspace-id.datawarehouse.fabric.microsoft.com",
        "FABRIC_LAKEHOUSE_NAME": "MyLakehouse",
        "UV_LINK_MODE": "copy"
      }
    }
  }
}
```

**Note**: The `UV_LINK_MODE=copy` setting prevents installation issues on OneDrive-synced folders.

## Step 5: Verify Installation

Restart VS Code and verify the MCP server is running:

1. Open VS Code
2. Check the MCP status indicator (usually in the status bar)
3. The server should show as "Connected" or "Running"

## Step 6: Start Using!

You can now use natural language to query your Fabric Lakehouse:

- "List all tables in my lakehouse"
- "Show me the schema for the sales table"
- "Query the last 10 rows from customer_data"
- "What tables contain customer information?"

## Troubleshooting

### UV Cache Issues (Windows OneDrive)

If you see errors about "hardlinks" or "cloud operation" (error 396):
```bash
# Clear the UV cache
uv cache clean
```

Then add `"UV_LINK_MODE": "copy"` to your environment variables (already included in the config above).

### Authentication Issues

If you see authentication errors:
```bash
# Re-login to Azure CLI
az login
az account show  # Verify you're logged in
```

### Connection Issues

Verify your SQL endpoint format:
- Should be: `workspace-id.datawarehouse.fabric.microsoft.com`
- Should NOT include: `https://`, port numbers, or database names

### Package Not Found

Make sure you're using the correct command name:
- **Correct**: `fabric-lakehouse`
- **Old (incorrect)**: `fabric-lakehouse-mcp`

## Need More Help?

- [Full Documentation](README.md)
- [Detailed Setup Guide](SETUP.md)
- [GitHub Issues](https://github.com/melisa-l/fabric-rti-mcp/issues)

## Configuration Reference

Only **2 required** environment variables:

| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `FABRIC_SQL_ENDPOINT` | ✅ Yes | Your Fabric SQL endpoint | `abc123.datawarehouse.fabric.microsoft.com` |
| `FABRIC_LAKEHOUSE_NAME` | ✅ Yes | Name of your lakehouse | `MyLakehouse` |

Optional variables:

| Variable | Required | Description | Default |
|----------|----------|-------------|---------|
| `FABRIC_API_BASE` | ❌ No | Fabric API base URL | `https://api.fabric.microsoft.com` |
| `FABRIC_BASE_URL` | ❌ No | Fabric base URL | `https://fabric.microsoft.com` |

---

**That's it!** You're ready to query your Fabric Lakehouse with natural language. 🎉
