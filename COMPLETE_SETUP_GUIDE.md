# Complete Setup Guide - Fabric Lakehouse MCP Server

**Version 0.3.3** - Updated November 14, 2025

Get your Fabric Lakehouse connected to GitHub Copilot in **15 minutes**!

---

## What You'll Get

Once set up, you can ask Copilot questions like:
- "What tables are in my lakehouse?"
- "Show me the schema of the Customer table"
- "Query the top 10 rows from Sales"
- "What's the relationship between Orders and Customers?"

All powered by your Microsoft Fabric Lakehouse data!

---

# Prerequisites Checklist

Before you begin, make sure you have:

- **VS Code** installed ([Download here](https://code.visualstudio.com/))
- **GitHub Copilot** subscription (required for MCP functionality)
- **Microsoft Fabric** workspace access
- **Lakehouse** created in your Fabric workspace
- Administrator access to install software on your computer

---

# Part 1: Install Required Software

## Step 1: Install Python

### Important: Python Version

**Supported versions:** Python 3.10, 3.11, 3.12, or **3.13**

### Windows Installation

1. **Download Python:**
   - Go to [python.org/downloads](https://www.python.org/downloads/)
   - Download the latest **Python 3.12** or **3.13** installer
   - **Recommended:** Python 3.12.x or Python 3.13.x

2. **Run the installer:**
   - ✅ **CRITICAL:** Check **"Add python.exe to PATH"** at the bottom
   - Click **"Install Now"**
   - Wait for installation to complete

3. **Verify installation:**
   - Open **PowerShell** (press `Win+X` → "PowerShell")
   - Type:
     ```powershell
     python --version
     ```
   - Should show: `Python 3.12.x` or `Python 3.13.x`

### Mac Installation

1. **Download Python:**
   - Go to [python.org/downloads](https://www.python.org/downloads/)
   - Download the macOS installer

2. **Or use Homebrew:**
   ```bash
   brew install python@3.12
   ```

3. **Verify:**
   ```bash
   python3 --version
   ```

### Linux Installation

```bash
sudo apt update
sudo apt install python3.12 python3-pip
```

---

## Step 2: Install Azure CLI (Recommended)

### ⭐ Why Install Azure CLI?

**Without Azure CLI:** You'll see a browser popup asking you to authenticate **every time** Copilot queries your data.

**With Azure CLI:** You authenticate **once**, and your credentials are cached. All future queries work seamlessly without interruptions!

### Windows Installation

**Option 1: Download Installer (Easiest)**
1. Download from: [aka.ms/installazurecliwindows](https://aka.ms/installazurecliwindows)
2. Run the MSI installer
3. Click through the installation wizard

**Option 2: Using winget**
```powershell
winget install Microsoft.AzureCLI
```

### Mac Installation

```bash
brew install azure-cli
```

### Linux Installation

```bash
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
```

### Verify Installation

Open a **new** terminal window and run:
```powershell
az --version
```

Should show Azure CLI version (2.x.x or higher)

---

## Step 3: Authenticate with Azure (One-Time Setup)

This is the magic step that eliminates repeated authentication prompts!

### Run Azure Login

Open PowerShell (or Terminal on Mac/Linux) and run:
```powershell
az login
```

**What happens:**
1. Your browser opens automatically
2. Sign in with your **Microsoft/organizational account**
3. You'll see "You have signed in to the Azure CLI" message
4. Close the browser tab
5. Back in the terminal, you'll see your subscriptions listed

**✅ Done!** Your credentials are now cached locally and will be used automatically by the MCP server.

### Verify Authentication

```powershell
az account show
```

Should display your account information.

---

## Step 4: Install VS Code Extensions

1. **Open VS Code**

2. **Install GitHub Copilot Extensions:**
   - Press `Ctrl+Shift+X` (Windows/Linux) or `Cmd+Shift+X` (Mac)
   - Search for: **"GitHub Copilot"**
   - Click **Install** on both:
     - ✅ **GitHub Copilot**
     - ✅ **GitHub Copilot Chat**

3. **Sign in to GitHub Copilot:**
   - Click the GitHub icon in the left sidebar
   - Follow prompts to authenticate
   - Ensure you have an active Copilot subscription

---

# Part 2: Get Your Fabric Connection Details

You need **2 pieces of information** from Microsoft Fabric.

## Step 5: Find Your SQL Endpoint

1. **Open Fabric Portal:**
   - Go to [app.fabric.microsoft.com](https://app.fabric.microsoft.com/)
   - Sign in with your organizational account

2. **Navigate to your Lakehouse:**
   - Click on your **Workspace** (left sidebar)
   - Find and click your **Lakehouse**

3. **Get SQL Endpoint:**
   - In the top ribbon, click **"SQL endpoint"** button
   - You'll see connection details
   - Find the **"Server"** field
   - Copy the entire server address

**Example format:**
```
abc123-workspace.datawarehouse.fabric.microsoft.com
```

**Important:** 
- ❌ Do NOT include `https://`
- ❌ Do NOT include port numbers
- ✅ Just the hostname

📝 **Save this** - you'll need it in Step 7!

---

## Step 6: Get Your Lakehouse Name

1. **Still in Fabric Portal**, note the name of your Lakehouse
2. Look at the **title bar** or **breadcrumb navigation**
3. The name is exactly as shown (case-sensitive!)

**Example:** `SalesLakehouse` or `CustomerDataLakehouse`

📝 **Save this** - you'll need it in Step 7!

---

# Part 3: Configure the MCP Server

## Step 7: Add MCP Server in VS Code

Now we'll install and configure the Fabric Lakehouse MCP Server.

### 7.1 Open Command Palette

1. In VS Code, press:
   - **Windows/Linux:** `Ctrl+Shift+P`
   - **Mac:** `Cmd+Shift+P`

2. Type: **`MCP: Add Server`**

3. Press **Enter**

### 7.2 Select Installation Method

You'll see several options:
- npm (Node.js)
- **pip (Python)** ← **Choose this one**
- uvx (Python via uv)
- docker
- Other

**Click: "pip (Python)"**

### 7.3 Enter Package Name

**Prompt:** "Enter the package name:"

Type exactly:
```
fabric-lakehouse-mcp
```

Press **Enter**

**Wait for installation** - this may take 30-60 seconds while it downloads and installs the package and dependencies.

### 7.4 Configure Environment Variables

Now you'll add your Fabric connection details.

#### Add Variable 1: SQL Endpoint

**Prompt:** "Enter environment variable name (or press Enter to skip):"

1. Type: `FABRIC_SQL_ENDPOINT`
2. Press **Enter**

**Prompt:** "Enter value for FABRIC_SQL_ENDPOINT:"

1. Paste your SQL endpoint from Step 5
2. Example: `abc123-workspace.datawarehouse.fabric.microsoft.com`
3. Press **Enter**

#### Add Variable 2: Lakehouse Name

**Prompt:** "Enter environment variable name (or press Enter to skip):"

1. Type: `FABRIC_LAKEHOUSE_NAME`
2. Press **Enter**

**Prompt:** "Enter value for FABRIC_LAKEHOUSE_NAME:"

1. Type your lakehouse name from Step 6
2. Example: `SalesLakehouse`
3. Must match exactly (case-sensitive)
4. Press **Enter**

#### Finish Configuration

**Prompt:** "Enter environment variable name (or press Enter to skip):"

Just press **Enter** (no more variables to add)

### 7.5 Restart VS Code

**Important:** Close and reopen VS Code to load the new MCP server.

---

# Part 4: Test Your Setup

## Step 8: Verify MCP Server is Running

1. **Open Copilot Chat:**
   - Click the chat icon in the left sidebar (or press `Ctrl+Alt+I`)

2. **Check available tools:**
   - Type: `@workspace /tools`
   - Press **Enter**

**Expected output:**
```
Available tools:
- fabric_lakehouse_list_tables
- fabric_lakehouse_describe_table
- fabric_lakehouse_execute_query
- fabric_lakehouse_get_relationships
- fabric_lakehouse_search_columns
```

If you see these tools, **congratulations! 🎉** Your setup is complete!

---

## Step 9: Try Your First Query

Ask Copilot something about your data:

**Example questions:**

1. **"What tables are available in my lakehouse?"**
   - Copilot will use `fabric_lakehouse_list_tables` tool

2. **"Describe the schema of the Customer table"**
   - Copilot will use `fabric_lakehouse_describe_table` tool

3. **"Show me the first 5 rows from the Sales table"**
   - Copilot will use `fabric_lakehouse_execute_query` tool

4. **"What columns contain 'email' in their name?"**
   - Copilot will use `fabric_lakehouse_search_columns` tool

### Expected Experience

**With Azure CLI (Recommended):**
- ✅ Queries run smoothly without any popups
- ✅ Fast response times
- ✅ No interruptions

**Without Azure CLI:**
- ⚠️ First query: Browser popup asking for authentication
- ⚠️ Subsequent queries: May prompt again periodically
- 💡 **Solution:** Install Azure CLI (go back to Step 2)

---

# Troubleshooting

## Problem: "Python not found"

**Solution:**
1. Reinstall Python from [python.org](https://www.python.org/downloads/)
2. **Make sure to check** "Add Python to PATH"
3. Close and reopen your terminal

## Problem: "pip not found"

**Solution:**
```powershell
python -m ensurepip --upgrade
```

## Problem: Installation fails with compilation errors

**If you see errors about "pyodbc" or "C++ compiler":**

1. **Check Python version:**
   ```powershell
   python --version
   ```

2. **If Python 3.9 or older:**
   - Upgrade to Python 3.10+ (see Step 1)

3. **If Python 3.10+:**
   - Try upgrading pip:
     ```powershell
     python -m pip install --upgrade pip
     ```
   - Retry installation

## Problem: "Getting prompted to authenticate repeatedly"

**Solution:**
1. Install Azure CLI (see Step 2)
2. Run `az login` (see Step 3)
3. Restart VS Code
4. Credentials will now be cached automatically

## Problem: "No tables found" or "Connection failed"

**Check these:**

1. **Verify SQL endpoint is correct:**
   - Should NOT have `https://`
   - Should NOT have port numbers
   - Format: `xxx.datawarehouse.fabric.microsoft.com`

2. **Verify lakehouse name is exact:**
   - Case-sensitive
   - No extra spaces
   - Matches exactly what's in Fabric Portal

3. **Check permissions:**
   - Do you have access to the workspace?
   - Do you have read permissions on the lakehouse?

4. **View mcp.json configuration:**
   - Open Command Palette (`Ctrl+Shift+P`)
   - Type: "Preferences: Open User Settings (JSON)"
   - Look for `"mcp"` section
   - Verify your environment variables are set correctly

## Problem: MCP tools not showing in @workspace /tools

**Solution:**
1. Ensure GitHub Copilot extensions are installed and active
2. Restart VS Code completely (close all windows)
3. Check Output panel (View → Output → select "MCP" from dropdown)
4. Look for any error messages

## Problem: "Token expired" or authentication errors

**Solution:**
```powershell
az login
```
Run this to refresh your Azure credentials.

---

# Advanced Configuration

## Viewing Your Configuration

Your MCP configuration is stored in VS Code's settings.

**To view/edit:**
1. Press `Ctrl+Shift+P` (or `Cmd+Shift+P` on Mac)
2. Type: "Preferences: Open User Settings (JSON)"
3. Look for the `"mcp"` section

**Example configuration:**
```json
{
  "mcp": {
    "servers": {
      "fabric-lakehouse-mcp": {
        "command": "python",
        "args": ["-m", "fabric_rti_mcp.server"],
        "env": {
          "FABRIC_SQL_ENDPOINT": "abc123.datawarehouse.fabric.microsoft.com",
          "FABRIC_LAKEHOUSE_NAME": "SalesLakehouse"
        }
      }
    }
  }
}
```

## Manual Installation (Alternative)

If the VS Code command doesn't work, you can install manually:

1. **Install the package:**
   ```powershell
   pip install fabric-lakehouse-mcp
   ```

2. **Create mcp.json** in your VS Code settings folder:
   - Windows: `%APPDATA%\Code\User\mcp.json`
   - Mac: `~/Library/Application Support/Code/User/mcp.json`
   - Linux: `~/.config/Code/User/mcp.json`

3. **Add this content:**
   ```json
   {
     "mcpServers": {
       "fabric-lakehouse-mcp": {
         "command": "python",
         "args": ["-m", "fabric_rti_mcp.server"],
         "env": {
           "FABRIC_SQL_ENDPOINT": "your-endpoint.datawarehouse.fabric.microsoft.com",
           "FABRIC_LAKEHOUSE_NAME": "YourLakehouseName"
         }
       }
     }
   }
   ```

4. **Restart VS Code**

---

# What's Next?

## Explore Available Tools

Your MCP server provides these tools to Copilot:

1. **`fabric_lakehouse_list_tables`**
   - Lists all tables in your lakehouse
   - Shows schema and table names
   - Great for discovering what data you have

2. **`fabric_lakehouse_describe_table`**
   - Shows detailed schema for a specific table
   - Column names, data types, nullability
   - Useful for understanding table structure

3. **`fabric_lakehouse_execute_query`**
   - Run SQL queries against your lakehouse
   - Returns results in a readable format
   - Use for data analysis and exploration

4. **`fabric_lakehouse_get_relationships`**
   - Discovers potential relationships between tables
   - Based on column names and foreign key patterns
   - Helps understand data model

5. **`fabric_lakehouse_search_columns`**
   - Find columns by name across all tables
   - Search for specific data fields
   - Useful for data discovery

## Example Workflows

### Data Discovery
```
You: "What tables are in my lakehouse?"
Copilot: [Lists all tables with schemas]

You: "Describe the Customer table"
Copilot: [Shows detailed schema]

You: "What columns contain 'date'?"
Copilot: [Searches and lists matching columns]
```

### Data Analysis
```
You: "Show me the top 10 customers by revenue"
Copilot: [Writes and executes SQL query]

You: "What's the average order value?"
Copilot: [Calculates from your data]

You: "Group sales by region"
Copilot: [Aggregates and displays results]
```

### Data Modeling
```
You: "What's the relationship between Orders and Customers?"
Copilot: [Uses get_relationships tool to show connections]

You: "Show me the data model structure"
Copilot: [Maps out table relationships]
```

---

# Getting Help

## Documentation
- **GitHub Repository:** [github.com/melisa-l/fabric-rti-mcp](https://github.com/melisa-l/fabric-rti-mcp)
- **PyPI Package:** [pypi.org/project/fabric-lakehouse-mcp](https://pypi.org/project/fabric-lakehouse-mcp/)

## Common Issues
- Check the Troubleshooting section above
- Review the GitHub Issues page
- Ensure all prerequisites are met

## Updates
Check for updates regularly:
```powershell
pip install --upgrade fabric-lakehouse-mcp
```

Current version: **0.3.3** (November 14, 2025)

---

# Summary

You've successfully:
- ✅ Installed Python and Azure CLI
- ✅ Authenticated with Azure (one-time)
- ✅ Installed the Fabric Lakehouse MCP Server
- ✅ Configured your connection details
- ✅ Tested your setup

**You can now use GitHub Copilot to query and analyze your Fabric Lakehouse data directly in VS Code!**

Happy querying! 🚀
