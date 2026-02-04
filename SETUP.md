# Fabric RTI MCP Setup Guide

This guide will help you set up the environment for working with Fabric RTI MCP tools.

## Prerequisites

1. Python 3.10 or later
2. ODBC Driver 18 for SQL Server
3. Azure account with access to Fabric resources
4. Visual Studio Code (recommended)

## Installation Steps

1. **Clone the Repository**
   ```bash
   git clone https://github.com/melisa-l/fabric-rti-mcp.git
   cd fabric-rti-mcp
   ```

2. **Set Up Python Virtual Environment**
   ```bash
   # Windows PowerShell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1

   # Linux/macOS
   python -m venv .venv
   source .venv/bin/activate
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Install ODBC Driver**
   - Windows: Download and install [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
   - Linux:
     ```bash
     curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
     sudo add-apt-repository "$(curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list)"
     sudo apt-get update
     sudo apt-get install -y msodbcsql18
     ```

5. **Configure Environment Variables**
   ```powershell
   # Windows PowerShell
   $env:FABRIC_SQL_ENDPOINT = "your-endpoint.fabric.microsoft.com"
   $env:FABRIC_LAKEHOUSE_NAME = "YourLakehouseName"
   $env:FABRIC_RTI_DEFAULT_MODEL = "gpt-5"  # Default model for LLM operations

   # Linux/macOS
   export FABRIC_SQL_ENDPOINT="your-endpoint.fabric.microsoft.com"
   export FABRIC_LAKEHOUSE_NAME="YourLakehouseName"
   export FABRIC_RTI_DEFAULT_MODEL="gpt-5"  # Default model for LLM operations
   ```

   **Note**: The default model is set to `gpt-5`. You can override this by setting the `FABRIC_RTI_DEFAULT_MODEL` environment variable to a different model (e.g., `gpt-4`, `gpt-4o`, `gpt-3.5-turbo`).

## Verifying Setup

1. **Test Connection**
   Run the list tables script to verify your connection:
   ```bash
   python list_lakehouse_tables.py
   ```
   This will open a browser window for authentication and then list all tables in your lakehouse.

2. **View Table Schemas**
   To see detailed schema information for all tables:
   ```bash
   python describe_lakehouse_tables.py
   ```

## Common Issues and Solutions

1. **ODBC Driver Not Found**
   - Ensure you've installed the correct version (x64/x86) matching your Python installation
   - Verify the driver is listed in ODBC Data Source Administrator

2. **Authentication Failed**
   - Check you have the correct permissions in Azure AD
   - Ensure you're logged into the correct tenant
   - Try running `az login` first if using Azure CLI authentication

3. **Environment Variables Not Set**
   - Double-check the environment variables are set correctly
   - Verify endpoint format matches your Fabric SQL endpoint

## Development Tools

1. **Code Formatting**
   ```bash
   # Format code using black
   black .
   ```

2. **Running Tests**
   ```bash
   pytest tests/
   ```

## Project Structure

- `fabric_rti_mcp/`: Core package directory
  - `tools/`: Contains MCP tools for Fabric integration
  - `kusto/`: Kusto query and connection handling
  - `eventstream/`: Event stream processing
- `tests/`: Test files
  - `unit/`: Unit tests
  - `live/`: Integration tests
- `examples/`: Example scripts and notebooks
- `scripts/`: Development and deployment scripts

## Adding New Tables

When adding new tables to the lakehouse:

1. Create your table DDL scripts
2. Update any relevant tooling for schema validation
3. Add appropriate documentation
4. Update tests as needed

## Next Steps

1. Review the `README.md` for detailed project information
2. Check out example notebooks in `examples/notebooks/`
3. Set up your development environment with recommended VS Code extensions
4. Familiarize yourself with the API documentation

## Getting Help

- Check the troubleshooting guide in `TROUBLESHOOTING.md`
- Review existing issues on GitHub
- Join the development team's communication channels