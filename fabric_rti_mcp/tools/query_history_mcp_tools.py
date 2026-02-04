"""
MCP tools for query history and monitoring.
"""


from mcp.server.fastmcp import FastMCP
from fabric_rti_mcp.tools.query_history_tool import (
    get_query_history,
    get_active_sessions,
    get_table_usage_stats,
)

__all__ = [
    "view_query_history",
    "view_active_sessions",
    "view_table_usage_stats",
    "register_tools",
]



def view_query_history(top_n: int = 20, min_duration_ms: int | None = None) -> str:
    """
    View the history of SQL queries executed against the Fabric lakehouse.
    Args:
        top_n: Number of recent queries to retrieve (default: 20)
        min_duration_ms: Only show queries longer than this duration in milliseconds (optional)
    Returns:
        Formatted query history with execution statistics
    """
    try:
        results = get_query_history(top_n=top_n, min_duration_ms=min_duration_ms)
        if not results:
            return "No query history found."
        output = [f"Query History (Top {len(results)} queries):"]
        output.append("=" * 100)
        for i, row in enumerate(results, 1):
            execution_count, creation_time, last_exec, cpu_ms, elapsed_ms, reads, writes, query_text = row
            output.append(f"\n[{i}] Query:")
            output.append(f"  Executions: {execution_count}")
            output.append(f"  Created: {creation_time}")
            output.append(f"  Last Executed: {last_exec}")
            output.append(f"  CPU Time: {cpu_ms}ms")
            output.append(f"  Elapsed Time: {elapsed_ms}ms")
            output.append(f"  Logical Reads: {reads}")
            output.append(f"  Logical Writes: {writes}")
            output.append(f"  Query: {query_text}")
        return "\n".join(output)
    except Exception as e:
        return f"Error retrieving query history: {str(e)}"

def view_active_sessions() -> str:
    """
    View currently active database sessions connected to the Fabric lakehouse.
    Returns:
        List of active sessions with connection details
    """
    try:
        results = get_active_sessions()
        if not results:
            return "No active sessions found."
        output = [f"Active Sessions ({len(results)} sessions):"]
        output.append("=" * 100)
        for row in results:
            session_id, login_name, host_name, program_name, login_time, status, last_req_start, last_req_end = row
            output.append(f"\nSession ID: {session_id}")
            output.append(f"  User: {login_name}")
            output.append(f"  Host: {host_name}")
            output.append(f"  Program: {program_name}")
            output.append(f"  Login Time: {login_time}")
            output.append(f"  Status: {status}")
            output.append(f"  Last Request: {last_req_start} - {last_req_end}")
        return "\n".join(output)
    except Exception as e:
        return f"Error retrieving active sessions: {str(e)}"

def view_table_usage_stats() -> str:
    """
    View table access statistics showing which tables are being queried most frequently.
    Returns:
        Table usage statistics including seek/scan counts and last access times
    """
    try:
        results = get_table_usage_stats()
        if not results:
            return "No table usage statistics found."
        output = [f"Table Usage Statistics (Top {len(results)} tables):"]
        output.append("=" * 100)
        for row in results:
            schema, table, seeks, scans, lookups, last_seek, last_scan = row
            total_ops = (seeks or 0) + (scans or 0) + (lookups or 0)
            output.append(f"\n{schema}.{table}")
            output.append(f"  Total Operations: {total_ops}")
            output.append(f"  Seeks: {seeks or 0}")
            output.append(f"  Scans: {scans or 0}")
            output.append(f"  Lookups: {lookups or 0}")
            output.append(f"  Last Seek: {last_seek or 'N/A'}")
            output.append(f"  Last Scan: {last_scan or 'N/A'}")
        return "\n".join(output)
    except Exception as e:
        return f"Error retrieving table usage statistics: {str(e)}"

def register_tools(mcp: FastMCP) -> None:
    """Register query history tools with the MCP server."""
    mcp.tool()(view_query_history)
    mcp.tool()(view_active_sessions)
    mcp.tool()(view_table_usage_stats)

