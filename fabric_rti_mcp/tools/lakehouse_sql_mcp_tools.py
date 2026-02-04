"""
MCP tools for Lakehouse SQL operations.
"""

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from fabric_rti_mcp.tools.lakehouse_sql_tool import (
    lakehouse_sql_query,
    lakehouse_list_tables_safe,
    lakehouse_describe_table,
    lakehouse_sample_table,
    lakehouse_find_relationships,
    lakehouse_find_primary_keys,
    lakehouse_get_schema_stats,
)


def register_tools(mcp: FastMCP) -> None:
    """Register lakehouse SQL tools with the MCP server."""

    mcp.add_tool(
        lakehouse_sql_query,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    
    # Register only the new safe tool for listing tables
    mcp.add_tool(
        lakehouse_list_tables_safe,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    
    mcp.add_tool(
        lakehouse_describe_table,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    
    mcp.add_tool(
        lakehouse_sample_table,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    
    mcp.add_tool(
        lakehouse_find_relationships,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    
    # lakehouse_find_potential_relationships removed
    
    mcp.add_tool(
        lakehouse_find_primary_keys,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
    
    mcp.add_tool(
        lakehouse_get_schema_stats,
        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
    )
