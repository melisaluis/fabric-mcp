"""
MCP tools for query suggestions and guided exploration.
"""
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from fabric_rti_mcp.tools.query_suggestions_service import (
    suggest_queries_for_table,
    get_exploration_path,
)


def register_tools(mcp: FastMCP) -> None:
    """Register query suggestion tools with the MCP server."""

    @mcp.tool()
    def get_starter_queries(schema_name: str, table_name: str) -> str:
        """
        Get helpful starter queries for exploring a table.
        
        Provides context-aware SQL queries that help new users understand a table's contents.
        Includes queries for:
        - Previewing sample data
        - Counting records
        - Checking data freshness
        - Finding value distributions
        - Identifying duplicates
        - Checking data quality
        
        Args:
            schema_name: The schema containing the table
            table_name: The table to generate queries for
        
        Returns:
            Formatted list of suggested queries with descriptions
            
        Example:
            get_starter_queries("ICM", "Incidents")
            Returns queries like "Preview first 10 rows", "Check data freshness", etc.
        """
        try:
            suggestions = suggest_queries_for_table(schema_name, table_name)
            
            if not suggestions:
                return f"No suggestions available for {schema_name}.{table_name}"
            
            output = [f"Starter Queries for [{schema_name}].[{table_name}]"]
            output.append("=" * 80)
            output.append(f"\nFound {len(suggestions)} helpful queries to get you started:\n")
            
            for i, suggestion in enumerate(suggestions, 1):
                output.append(f"\n{i}. {suggestion['description']}")
                output.append(f"   Purpose: {suggestion['purpose']}")
                output.append(f"   Query:")
                # Indent the query for readability
                query_lines = suggestion['query'].split('\n')
                for line in query_lines:
                    output.append(f"   {line}")
                output.append("")
            
            output.append("\n💡 Tip: Copy any query above and run it with lakehouse_sql_query tool!")
            
            return "\n".join(output)
            
        except Exception as e:
            return f"Error generating queries: {str(e)}"

    @mcp.tool()
    def get_schema_exploration_guide(schema_name: str) -> str:
        """
        Get a guided learning path for exploring a schema's tables.
        
        Recommends optimal table discovery order: start with small reference tables (lookup values),
        then medium fact tables (core business data), finally large analytical tables (requires careful querying).
        This sequence builds foundational knowledge before tackling complex data, preventing performance
        issues and accelerating understanding.
        
        Args:
            schema_name: The schema to generate an exploration guide for
        
        Returns:
            Step-by-step exploration path with rationale for each phase
            
        Example:
            get_schema_exploration_guide("ICM")
            Returns 3-phase path: reference tables → fact tables → analytical tables
        """
        try:
            guide = get_exploration_path(schema_name)
            
            output = [f"Exploration Guide for Schema: {guide['schema']}"]
            output.append("=" * 80)
            
            if 'recommendation' in guide:
                return guide['recommendation']
            
            output.append(f"\nTotal tables: {guide['total_tables']}")
            output.append("\n📚 Recommended Exploration Path:\n")
            
            for step in guide.get('exploration_steps', []):
                output.append(f"Step {step['step']}: {step['action']}")
                output.append(f"  Why: {step['reason']}")
                if step.get('tables'):
                    output.append(f"  Tables to check: {', '.join(step['tables'])}")
                output.append("")
            
            if guide.get('quick_start_query'):
                output.append("\n🚀 Quick Start Query:")
                output.append(guide['quick_start_query'])
            
            return "\n".join(output)
            
        except Exception as e:
            return f"Error generating exploration guide: {str(e)}"
