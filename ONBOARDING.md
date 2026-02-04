# Onboarding data sources with the Fabric RTI MCP Server

This document explains how to use the MCP server and example utilities in `examples/` to automate onboarding of Fabric SQL lakehouses and to generate dataset documentation for new users.

Goal
----
- Provide a repeatable, secure flow for discovering tables and schemas in a Fabric SQL lakehouse
- Produce README-style dataset docs (schema, sample rows, example queries) to accelerate user onboarding
- Show how an orchestrator or notebook can call the MCP server (HTTP) to run discovery tools

Quick prerequisites
-------------------
- A running instance of the Fabric RTI MCP Server (local or hosted). For orchestrator integration, enable HTTP mode by setting the transport env vars described in the main `README.md`.
- A service identity (managed identity or service principal) with read-only access to the target SQL lakehouse.
- Python 3.10 installed for example scripts
- (Optional) An API token for the MCP HTTP endpoint, or credentials so the orchestrator can call the MCP server.

Environment variables used by examples
-------------------------------------
- `FABRIC_RTI_TRANSPORT` - set to `http` to use HTTP mode (recommended for orchestrators)
- `FABRIC_RTI_HTTP_HOST` - host for MCP HTTP server (default `127.0.0.1`)
- `FABRIC_RTI_HTTP_PORT` - port for MCP HTTP server (default `3000`)
- `FABRIC_RTI_MCP_TOKEN` - Bearer token used by examples to authenticate to MCP HTTP API (if required)
- `FABRIC_SQL_ENDPOINT` - SQL endpoint for lakehouse
- `FABRIC_LAKEHOUSE_NAME` - Lakehouse database name

Files added in `examples/`
-------------------------
- `mcp_client.py` - small HTTP wrapper that invokes MCP tools via the MCP HTTP API (includes simple retry/backoff)
- `generate_dataset_docs.py` - command-line script that uses the MCP client to:
  1. Call `sql_list_lakehouse_tables` to get a list of tables
  2. Call `sql_get_table_schema` for each table (or top-N)
  3. Write a Markdown file per table under `examples/datasets/<database>/<table>.md`

How it works (high level)
-------------------------
1. The orchestrator (or notebook) authenticates to the MCP server using a short-lived Bearer token or by relying on the host environment identity.
2. The orchestrator calls the MCP HTTP endpoint to run the `sql_list_lakehouse_tables` tool.
3. For each discovered table, the orchestrator calls `sql_get_table_schema` to fetch columns and a small sample of rows.
4. A documentation agent (this can be an LLM or a templating step) composes a README from schema + samples and persists it.

Example quick run
-----------------
Set the environment variables (PowerShell example):

```powershell
$env:FABRIC_RTI_TRANSPORT = 'http'
$env:FABRIC_RTI_HTTP_HOST = '127.0.0.1'
$env:FABRIC_RTI_HTTP_PORT = '3000'
$env:FABRIC_RTI_MCP_TOKEN = '<your-token-if-needed>'
$env:SQL_LAKEHOUSE_DATABASE = 'YourDatabase'
```

Run the generator (uses defaults if you omit args):

```powershell
python .\examples\generate_dataset_docs.py --host 127.0.0.1 --port 3000 --database $env:SQL_LAKEHOUSE_DATABASE --top-n 10
```

Notes & best practices
----------------------
- Use a read-only service principal or managed identity for automated orchestration.
- Keep sample sizes small (5–10 rows) to reduce exposure and speed up scans.
- Tag or maintain an allowlist/denylist of tables to avoid scanning sensitive tables.
- Add periodic re-scan jobs to detect schema changes; include a `last_scanned` timestamp in the generated docs.

Next steps and optional enhancements
-----------------------------------
- Add a notebook `examples/notebooks/onboard_lakehouse.ipynb` for interactive exploration and authoring.
- Implement a richer doc-synthesis agent that uses column names + sample rows to draft natural-language summaries using Azure OpenAI.
- Integrate the generator into your CI or orchestrator to automatically update docs when schemas change.
