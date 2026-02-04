"""
MCP tools for semantic model metadata access.
"""
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from fabric_rti_mcp.tools.semantic_model_service import (
    get_lakehouse_semantic_relationships,
    get_semantic_model_info,
)


def register_tools(mcp: FastMCP) -> None:
    """Register semantic model tools with the MCP server."""

    @mcp.tool()
    def get_semantic_relationships() -> str:
        """
        Get relationship definitions from the Fabric semantic model.
        
        Queries the Power BI/Fabric semantic layer to find actual relationship definitions
        that aren't visible through the SQL Analytics endpoint. This provides formal
        relationship metadata including cross-filtering behavior and active status.
        
        Requires FABRIC_WORKSPACE_ID environment variable to be set.
        
        Returns:
            Formatted list of semantic model relationships
            
        Example:
            Shows relationships like: ICM.Incidents[TenantId] → Common.Tenants[TenantId]
        """
        try:
            relationships = get_lakehouse_semantic_relationships()
            
            if not relationships:
                return """No semantic model relationships found.

This could mean:
  1. No semantic model exists for this lakehouse
  2. The semantic model has no defined relationships
  3. FABRIC_WORKSPACE_ID environment variable not set

Try using lakehouse_find_potential_relationships() to discover relationships by naming patterns."""
            
            output = [f"Semantic Model Relationships ({len(relationships)} found)"]
            output.append("=" * 80)
            output.append("")
            
            for rel in relationships:
                from_table, from_col, to_table, to_col, rel_name, cross_filter = rel
                output.append(f"📊 {rel_name}")
                output.append(f"   From: {from_table}[{from_col}]")
                output.append(f"   To:   {to_table}[{to_col}]")
                output.append(f"   Cross-filtering: {cross_filter}")
                output.append("")
            
            return "\n".join(output)
            
        except Exception as e:
            return f"Error retrieving semantic relationships: {str(e)}\n\nEnsure FABRIC_WORKSPACE_ID is set in environment variables."

    @mcp.tool()
    def get_semantic_model_details() -> str:
        """
        Get comprehensive information about the lakehouse's semantic model.
        
        Provides details about the Power BI/Fabric semantic model including
        dataset ID, relationship count, and model structure. Useful for understanding
        the semantic layer built on top of the lakehouse tables.
        
        Requires FABRIC_WORKSPACE_ID environment variable to be set.
        
        Returns:
            Semantic model information including relationships
        """
        try:
            info = get_semantic_model_info()
            
            if not info.get("found"):
                return info.get("message", "Semantic model not found")
            
            output = ["Semantic Model Information"]
            output.append("=" * 80)
            output.append(f"\nLakehouse: {info['lakehouse_name']}")
            output.append(f"Workspace ID: {info['workspace_id']}")
            output.append(f"Dataset ID: {info['dataset_id']}")
            output.append(f"Relationships: {info['relationship_count']}")
            
            if info['relationships']:
                output.append("\n📊 Relationship Details:\n")
                for rel in info['relationships']:
                    output.append(f"  • {rel['name']}")
                    output.append(f"    {rel['from_table']}.{rel['from_column']} → {rel['to_table']}.{rel['to_column']}")
                    output.append(f"    Active: {rel['is_active']}, Cross-filtering: {rel['cross_filtering']}")
                    output.append("")
            
            return "\n".join(output)
            
        except Exception as e:
            return f"Error retrieving semantic model info: {str(e)}\n\nEnsure FABRIC_WORKSPACE_ID is set in environment variables."
