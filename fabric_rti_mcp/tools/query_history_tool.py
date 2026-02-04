from typing import List, Tuple

def get_active_users() -> List[Tuple]:
    """
    List all currently active user sessions on the database.
    Returns:
        List of tuples: (session_id, login_name, host_name, program_name, login_time, status)
    """
    sql_endpoint = os.getenv("FABRIC_SQL_ENDPOINT")
    database = os.getenv("FABRIC_LAKEHOUSE_NAME")
    if not sql_endpoint or not database:
        raise ValueError("FABRIC_SQL_ENDPOINT and FABRIC_LAKEHOUSE_NAME must be set")
    conn_str = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={sql_endpoint};"
        f"Database={database};"
        "Authentication=ActiveDirectoryInteractive;"
        "Encrypt=yes;TrustServerCertificate=no"
    )
    query = """
        SELECT session_id, login_name, host_name, program_name, login_time, status
        FROM sys.dm_exec_sessions
        WHERE is_user_process = 1
        ORDER BY login_time DESC
    """
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        conn.close()
"""
Query history tool for Fabric SQL lakehouse.
Provides access to query execution history and performance metrics.
"""

import os
from typing import List, Tuple, Optional
import pyodbc


def get_query_history(
    top_n: int = 20,
    min_duration_ms: Optional[int] = None,
) -> List[Tuple]:
    """
    Retrieve query execution history from Fabric SQL endpoint.
    
    NOTE: This shows ALL queries executed on the database by ALL users, not just your queries.
    The sys.dm_exec_query_stats DMV tracks query execution at the plan level, not per-user.
    
    Args:
        top_n: Number of recent queries to retrieve (default: 20)
        min_duration_ms: Only show queries longer than this duration in milliseconds
    
    Returns:
        List of tuples containing query history records
    """
    sql_endpoint = os.getenv("FABRIC_SQL_ENDPOINT")
    database = os.getenv("FABRIC_LAKEHOUSE_NAME")
    
    if not sql_endpoint or not database:
        raise ValueError("FABRIC_SQL_ENDPOINT and FABRIC_LAKEHOUSE_NAME must be set")
    
    conn_str = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={sql_endpoint};"
        f"Database={database};"
        "Authentication=ActiveDirectoryInteractive;"
        "Encrypt=yes;TrustServerCertificate=no"
    )
    
    # Build the query with filters
    where_clauses = []
    if min_duration_ms:
        where_clauses.append(f"total_elapsed_time >= {min_duration_ms}")
    
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    
    # Note: sys.dm_exec_query_stats doesn't track which user ran each query.
    # It only tracks query execution statistics at the query plan level.
    # For user-specific query history, we'd need query auditing or Extended Events.
    # Attempt to join with sessions and requests for user attribution
    query = f"""
        SELECT TOP {top_n}
            qs.execution_count,
            qs.creation_time,
            qs.last_execution_time,
            qs.total_worker_time / 1000 as cpu_time_ms,
            qs.total_elapsed_time / 1000 as elapsed_time_ms,
            qs.total_logical_reads,
            qs.total_logical_writes,
            LEFT(CAST(st.text AS NVARCHAR(MAX)), 500) as query_text,
            r.session_id,
            s.login_name,
            s.host_name,
            s.program_name
        FROM sys.dm_exec_query_stats AS qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
        LEFT JOIN sys.dm_exec_requests r ON qs.sql_handle = r.sql_handle
        LEFT JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
        {where_sql}
        ORDER BY qs.last_execution_time DESC
    """
    
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        conn.close()


def get_active_sessions() -> List[Tuple]:
    """
    Get currently active database sessions.
    
    Returns:
        List of tuples containing active session information
    """
    sql_endpoint = os.getenv("FABRIC_SQL_ENDPOINT")
    database = os.getenv("FABRIC_LAKEHOUSE_NAME")
    
    if not sql_endpoint or not database:
        raise ValueError("FABRIC_SQL_ENDPOINT and FABRIC_LAKEHOUSE_NAME must be set")
    
    conn_str = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={sql_endpoint};"
        f"Database={database};"
        "Authentication=ActiveDirectoryInteractive;"
        "Encrypt=yes;TrustServerCertificate=no"
    )
    
    query = """
        SELECT 
            session_id,
            login_name,
            host_name,
            program_name,
            login_time,
            status,
            last_request_start_time,
            last_request_end_time
        FROM sys.dm_exec_sessions
        WHERE is_user_process = 1
        ORDER BY login_time DESC
    """
    
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        conn.close()


def get_table_usage_stats() -> List[Tuple]:
    """
    Get statistics on table access patterns.
    
    Returns:
        List of tuples containing table usage statistics
    """
    sql_endpoint = os.getenv("FABRIC_SQL_ENDPOINT")
    database = os.getenv("FABRIC_LAKEHOUSE_NAME")
    
    if not sql_endpoint or not database:
        raise ValueError("FABRIC_SQL_ENDPOINT and FABRIC_LAKEHOUSE_NAME must be set")
    
    conn_str = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={sql_endpoint};"
        f"Database={database};"
        "Authentication=ActiveDirectoryInteractive;"
        "Encrypt=yes;TrustServerCertificate=no"
    )
    
    query = """
        SELECT 
            SCHEMA_NAME(o.schema_id) as schema_name,
            o.name as table_name,
            SUM(ios.user_seeks) as user_seeks,
            SUM(ios.user_scans) as user_scans,
            SUM(ios.user_lookups) as user_lookups,
            MAX(ios.last_user_seek) as last_seek,
            MAX(ios.last_user_scan) as last_scan
        FROM sys.dm_db_index_usage_stats ios
        INNER JOIN sys.objects o ON ios.object_id = o.object_id
        WHERE o.type = 'U'
        GROUP BY SCHEMA_NAME(o.schema_id), o.name
        ORDER BY SUM(ios.user_seeks + ios.user_scans + ios.user_lookups) DESC
    """
    
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        conn.close()


if __name__ == "__main__":
    # Example usage
    print("Recent Query History:")
    print("=" * 80)
    history = get_query_history(top_n=10)
    for row in history:
        print(f"\nExecutions: {row[0]}")
        print(f"Created: {row[1]}")
        print(f"Last Executed: {row[2]}")
        print(f"CPU Time: {row[3]}ms")
        print(f"Elapsed Time: {row[4]}ms")
        print(f"Query: {row[7][:100]}...")
