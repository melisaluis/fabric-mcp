import json
from pathlib import Path

try:
    from importlib.metadata import version

    __version__ = version("microsoft-fabric-rti-mcp")
except Exception:
    __version__ = "0.0.0.dev0"


def get_mcp_config():
    """Return the MCP configuration for this server.
    
    This is used by MCP hosts to auto-discover and configure the server.
    """
    manifest_path = Path(__file__).parent / "mcp-manifest.json"
    
    if manifest_path.exists():
        with open(manifest_path, 'r') as f:
            return json.load(f)
    
    # Fallback configuration
    return {
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

