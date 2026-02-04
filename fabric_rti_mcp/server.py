import sys
print('DEBUG: sys.path =', sys.path)
import json
import os
import signal
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Tuple

from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from fabric_rti_mcp import __version__
from fabric_rti_mcp.authentication.auth_middleware import add_auth_middleware
from fabric_rti_mcp.common import global_config as config
from fabric_rti_mcp.common import logger
import fabric_rti_mcp.tools.query_history_mcp_tools as query_history_mcp_tools
import fabric_rti_mcp.tools.lakehouse_sql_mcp_tools as lakehouse_sql_mcp_tools
import fabric_rti_mcp.tools.query_suggestions_mcp_tools as query_suggestions_mcp_tools
import fabric_rti_mcp.tools.semantic_model_mcp_tools as semantic_model_mcp_tools

# Global variable to store server start time
server_start_time = datetime.now(timezone.utc)


def setup_shutdown_handler(sig: int, frame: Optional[types.FrameType]) -> None:
    """Handle process termination signals."""
    signal_name = signal.Signals(sig).name
    logger.info(f"Received signal {sig} ({signal_name}), shutting down...")
    sys.exit(0)


# Health check endpoint
async def health_check(request: Request) -> JSONResponse:
    current_time = datetime.now(timezone.utc)
    logger.info(f"Server health check at {current_time}")
    return JSONResponse(
        {
            "status": "healthy",
            "current_time_utc": current_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "server": "fabric-lakehouse-mcp",
            "start_time_utc": server_start_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
        }
    )


def add_health_endpoint(mcp: FastMCP) -> None:
    """Add health endpoint for Kubernetes liveness probes."""
    mcp.custom_route("/health", methods=["GET"])(health_check)


def register_tools(mcp: FastMCP) -> None:
    """Register all tools with the MCP server."""
    logger.info("Registering SQL Lakehouse and Query History tools...")

    # Register SQL Lakehouse tools
    query_history_mcp_tools.register_tools(mcp)
    # Register lakehouse_list_tables_safe (schema and table name only, safe)
    from fabric_rti_mcp.tools.lakehouse_sql_tool import lakehouse_list_tables_safe
    mcp.add_tool(
        lakehouse_list_tables_safe,
    )
    query_suggestions_mcp_tools.register_tools(mcp)
    semantic_model_mcp_tools.register_tools(mcp)

    logger.info("All tools registered successfully.")


def print_mcp_config() -> None:
    """Print the MCP configuration for easy copying to Copilot settings."""
    # Try to find the mcp-manifest.json file
    package_dir = Path(__file__).parent
    manifest_path = package_dir / "mcp-manifest.json"
    
    if manifest_path.exists():
        with open(manifest_path, 'r') as f:
            config_data = json.load(f)
    else:
        # Fallback to hardcoded config
        config_data = {
            "servers": {
                "fabric-lakehouse": {
                    "command": "fabric-lakehouse",
                    "args": [],
                    "env": {
                        "FABRIC_SQL_ENDPOINT": "${input:FABRIC_SQL_ENDPOINT}",
                        "FABRIC_LAKEHOUSE_NAME": "${input:FABRIC_LAKEHOUSE_NAME}"
                    },
                    "type": "stdio"
                }
            },
            "inputs": [
                {
                    "id": "FABRIC_SQL_ENDPOINT",
                    "type": "promptString",
                    "description": "Enter the SQL endpoint for your Fabric lakehouse (e.g., workspace.datawarehouse.fabric.microsoft.com)"
                },
                {
                    "id": "FABRIC_LAKEHOUSE_NAME",
                    "type": "promptString",
                    "description": "Enter the name of your Fabric lakehouse (case-sensitive)"
                }
            ]
        }
    
    config_json = json.dumps(config_data, indent=2)
    
    # Try to copy to clipboard
    try:
        import pyperclip
        pyperclip.copy(config_json)
        clipboard_msg = "✅ Configuration copied to clipboard!"
    except ImportError:
        clipboard_msg = "💡 Install 'pyperclip' to auto-copy: pip install pyperclip"
    except Exception:
        clipboard_msg = "⚠️  Could not copy to clipboard automatically"
    
    print("\n" + "="*80)
    print("MCP Configuration for GitHub Copilot")
    print("="*80 + "\n")
    print(config_json)
    print("\n" + "="*80)
    print(clipboard_msg)
    print("\nConfiguration file location (varies by OS):")
    print("  Windows: %APPDATA%\\Code\\User\\globalStorage\\github.copilot-chat\\mcp.json")
    print("  macOS: ~/Library/Application Support/Code/User/globalStorage/github.copilot-chat/mcp.json")
    print("  Linux: ~/.config/Code/User/globalStorage/github.copilot-chat/mcp.json")
    print("\n💡 Tip: You may need to create the 'github.copilot-chat' directory if it doesn't exist")
    print("="*80 + "\n")


def main() -> None:
    """Main entry point for the server."""
    # Check for --print-config flag
    if len(sys.argv) > 1 and sys.argv[1] == "--print-config":
        print_mcp_config()
        return
    
    try:
        # Signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, setup_shutdown_handler)
        signal.signal(signal.SIGTERM, setup_shutdown_handler)

        logger.info("Starting Fabric Lakehouse MCP server")
        logger.info(f"Version: {__version__}")
        logger.info(f"Python version: {sys.version}")
        logger.info(f"Platform: {sys.platform}")
        logger.info(f"PID: {os.getpid()}")
        logger.info(f"Transport: {config.transport}")
        logger.info(f"Default Model: {config.default_model}")

        if config.transport == "http":
            logger.info(f"Host: {config.http_host}")
            logger.info(f"Port: {config.http_port}")
            logger.info(f"Path: {config.http_path}")
            logger.info(f"Stateless HTTP: {config.stateless_http}")
            logger.info(f"Use OBO flow: {config.use_obo_flow}")

        name = "fabric-lakehouse-mcp-server"
        if config.transport == "http":
            fastmcp_server = FastMCP(
                name,
                host=config.http_host,
                port=config.http_port,
                streamable_http_path=config.http_path,
                stateless_http=config.stateless_http,
            )
        else:
            fastmcp_server = FastMCP(name)

        # Register tools
        register_tools(fastmcp_server)

        # Add HTTP-specific features if in HTTP mode
        if config.transport == "http":
            add_health_endpoint(fastmcp_server)
            logger.info("Adding authorization middleware")
            add_auth_middleware(fastmcp_server)

        # Run the server
        if config.transport == "http":
            logger.info(f"Starting {name} (HTTP) on {config.http_host}:{config.http_port} with /health endpoint")
            fastmcp_server.run(transport="streamable-http")
        else:
            logger.info(f"Starting {name} (stdio)")
            fastmcp_server.run(transport="stdio")

    except KeyboardInterrupt:
        logger.info("Server interrupted by user")
    except Exception as error:
        logger.error(f"Server error: {error}")
        raise


if __name__ == "__main__":
    main()
