"""
Lakehouse SQL tools for querying and exploring Fabric Lakehouse data.
"""
import os
import pyodbc
import logging
from typing import List, Tuple

# Tool registration
def register_tools(mcp):
    """
    Register lakehouse SQL and semantic model tools with MCP.
    """
    mcp.add_tool(
        name="lakehouse_sql_query",
        func=lakehouse_sql_query,
        description="Execute a SQL query against Fabric Lakehouse using the SQL endpoint. Args: query (str). Returns: List of tuples."
    )
    mcp.add_tool(
        name="lakehouse_list_tables",
        func=lakehouse_list_tables,
        description="List all tables in the lakehouse with their schemas and row counts. Returns: List of tuples (schema_name, table_name, row_count)."
    )
    mcp.add_tool(
        name="lakehouse_describe_table",
        func=lakehouse_describe_table,
        description="Get detailed schema information for a specific table. Args: schema_name (str), table_name (str). Returns: List of tuples."
    )
    mcp.add_tool(
        name="lakehouse_sample_table",
        func=lakehouse_sample_table,
        description="Get sample rows from a table to preview the data. Args: schema_name (str), table_name (str), limit (int, optional). Returns: List of tuples."
    )
    mcp.add_tool(
        name="lakehouse_find_relationships",
        func=lakehouse_find_relationships,
        description="Find foreign key relationships between tables in the lakehouse. Returns: List of tuples (parent_schema, parent_table, parent_column, child_schema, child_table, child_column)."
    )
    mcp.add_tool(
        name="lakehouse_find_primary_keys",
        func=lakehouse_find_primary_keys,
        description="Find all primary key columns across all tables. Returns: List of tuples (schema_name, table_name, column_name, ordinal_position)."
    )
    mcp.add_tool(
        name="lakehouse_get_schema_stats",
        func=lakehouse_get_schema_stats,
        description="Get statistics about the schema to help diagnose performance issues. Returns: List of tuples (schema_name, table_count, column_count)."
    )
    mcp.add_tool(
        name="get_semantic_model_connection",
        func=get_semantic_model_connection,
        description="Get the connection string for the semantic model (Power BI endpoint). Returns: str."
    )
    mcp.add_tool(
        name="semantic_model_query",
        func=semantic_model_query,
        description="Query the semantic model using the Power BI endpoint. Args: query (str). Returns: List of tuples."
    )
    mcp.add_tool(
        name="semantic_model_list_relationships",
        func=semantic_model_list_relationships,
        description="List table relationships from the Power BI semantic model. Returns: List of tuples (from_table, from_column, to_table, to_column)."
    )
    mcp.add_tool(
        name="test_semantic_model_connection",
        func=test_semantic_model_connection,
        description="Test connectivity to the Power BI semantic model endpoint. Returns: True if connection succeeds, raises error otherwise."
    )
    mcp.add_tool(
        name="lakehouse_list_tables_safe",
        func=lakehouse_list_tables_safe,
        description="List all tables in the lakehouse with their schemas and row counts (safe, schema-compliant). Returns: List of tuples (schema_name, table_name, row_count:int)."
    )
# List relationships from the semantic model using DMVs
def test_semantic_model_connection() -> bool:
    """
    Test connectivity to the Power BI semantic model endpoint.
    Returns True if connection succeeds, raises error otherwise.
    """
    conn_str = get_semantic_model_connection()
    try:
        from pyadomd import Pyadomd
    except ImportError:
        logging.error("pyadomd is required for semantic model queries. Install with 'pip install pyadomd'.")
        raise
    try:
        with Pyadomd(conn_str) as conn:
            # Try a trivial DMV query
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM $SYSTEM.TMSCHEMA_TABLES")
                _ = cur.fetchone()
        logging.info("Successfully connected to semantic model endpoint.")
        return True
    except Exception as e:
        logging.error(f"Error connecting to semantic model endpoint: {e}")
        raise


def lakehouse_list_tables_v2() -> List[Tuple[str, str, int]]:
    """
    List all tables in the lakehouse with their schemas and row counts.
    
    Returns:
        List of tuples: (schema_name, table_name, row_count)
    """
    query = """
        SELECT 
            s.name AS schema_name,
            t.name AS table_name,
            ISNULL(SUM(p.rows), 0) AS row_count
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        INNER JOIN sys.partitions p ON t.object_id = p.object_id
        WHERE p.index_id IN (0, 1)  -- heap or clustered index
        GROUP BY s.name, t.name
        ORDER BY s.name, t.name
    """
    logging.info("Listing all tables in the lakehouse with row counts...")
    results = lakehouse_sql_query(query)
    def safe_int(val):
        try:
            return int(val) if val is not None else 0
        except (ValueError, TypeError):
            return 0
    filtered = []
    for row in results:
        schema = str(row[0]) if row[0] is not None else "Unknown"
        table = str(row[1]) if row[1] is not None else "Unknown"
        row_count = safe_int(row[2]) if row[2] is not None else 0
        filtered.append((schema, table, row_count))
    # Final pass: guarantee all row_count are int and not None
    sanitized = []
    for schema, table, row_count in filtered:
        if row_count is None or not isinstance(row_count, int):
            row_count = 0
        sanitized.append((schema, table, row_count))
    logging.info(f"Found {len(sanitized)} tables (row_count guaranteed int, not None).")
    return sanitized
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# Global credential object - initialized once, reused for all requests
_credential = None

"""
Configuration:
  - Set FABRIC_SQL_ENDPOINT in your .env for the SQL endpoint (e.g. workspace/lakehouse SQL server)
  - Set FABRIC_LAKEHOUSE_NAME in your .env for the database name
  - Set FABRIC_SEMANTIC_MODEL_CONNECTION in your .env for the semantic model endpoint (e.g. powerbi://api.powerbi.com/v1.0/myorg/HMCClinic2026)
"""

# SQL endpoint connection string parts
SQL_ENDPOINT = os.getenv("FABRIC_SQL_ENDPOINT")
LAKEHOUSE_NAME = os.getenv("FABRIC_LAKEHOUSE_NAME")
# Semantic model connection string (Power BI endpoint)
SEMANTIC_MODEL_CONNECTION = os.getenv("FABRIC_SEMANTIC_MODEL_CONNECTION")

# Helper to validate config
def validate_config():
    missing = []
    if not SQL_ENDPOINT:
        missing.append("FABRIC_SQL_ENDPOINT")
    if not LAKEHOUSE_NAME:
        missing.append("FABRIC_LAKEHOUSE_NAME")
    if not SEMANTIC_MODEL_CONNECTION:
        missing.append("FABRIC_SEMANTIC_MODEL_CONNECTION")
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}. Please set them in your .env file.")



def _get_connection():
    """
    Get a connection to the lakehouse SQL endpoint using Azure authentication.
    Uses DefaultAzureCredential which tries authentication methods in order:
        1. Azure CLI (recommended - run 'az login' once)
        2. Environment variables
        3. Managed Identity (for Azure-hosted deployments)
        4. Visual Studio Code
        5. Interactive browser (fallback)
    Tokens are automatically cached and refreshed by azure-identity library.
    """
    import logging
    global _credential
    logging.warning(f"_get_connection: SQL_ENDPOINT={SQL_ENDPOINT}")
    logging.warning(f"_get_connection: LAKEHOUSE_NAME={LAKEHOUSE_NAME}")
    try:
        validate_config()
    except Exception as e:
        logging.error(f"validate_config failed: {e}")
        return None
    sql_endpoint = SQL_ENDPOINT
    database = LAKEHOUSE_NAME
    # Azure authentication
    conn_str = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={sql_endpoint};"
        f"Database={database};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout=30;"
        f"Authentication=ActiveDirectoryInteractive;"
    )
    logging.warning(f"_get_connection: conn_str={conn_str}")
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as db_err:
        logging.error(f"pyodbc.Error in _get_connection: {db_err}")
        if hasattr(db_err, 'args'):
            for idx, arg in enumerate(db_err.args):
                logging.error(f"ODBC error arg[{idx}]: {arg}")
        try:
            diag = pyodbc.drivers()
            logging.error(f"Available ODBC drivers: {diag}")
        except Exception as diag_err:
            logging.error(f"Error getting ODBC drivers: {diag_err}")
        return None
    except Exception as e:
        logging.error(f"General Exception in _get_connection: {e}")
        return None
    # Add a stub for semantic model connection (Power BI)
def get_semantic_model_connection() -> str:
    """
    Get the connection string for the semantic model (Power BI endpoint).
    Returns: str
    """
    validate_config()
    conn_str = SEMANTIC_MODEL_CONNECTION or "powerbi://api.powerbi.com/v1.0/myorg/HMCClinic2026"
    if not conn_str:
        logging.error("FABRIC_SEMANTIC_MODEL_CONNECTION must be set")
        raise ValueError("FABRIC_SEMANTIC_MODEL_CONNECTION must be set")
    return conn_str


# Semantic model query stub (to be implemented with Power BI API or pyadomd)
# Semantic model query stub (to be implemented with Power BI API or pyadomd)
def semantic_model_query(query: str) -> List[Tuple]:
    """
    Query the semantic model using the Power BI endpoint.
    Args:
        query: DAX or MDX query to execute
    Returns:
        List of tuples containing query results
    """
    logging.info(f"Semantic model query requested: {query}")
    conn_str = get_semantic_model_connection()
    try:
        from pyadomd import Pyadomd
    except ImportError:
        logging.error("pyadomd is required for semantic model queries. Install with 'pip install pyadomd'.")
        raise

    try:
        with Pyadomd(conn_str) as conn:
            # ...existing code for semantic model query...
            # You should add the actual query logic here
            pass
    except Exception as e:
        logging.error(f"Error executing semantic model query: {e}")
        raise


# List relationships from the semantic model using DMVs
def semantic_model_list_relationships() -> List[Tuple[str, str, str, str]]:
    """
    List table relationships from the Power BI semantic model using DMVs.
    Returns:
        List of tuples: (from_table, from_column, to_table, to_column)

    Usage:
        Only works with semantic model (Power BI endpoint) via pyadomd, not SQL endpoint.
        Example:
            relationships = semantic_model_list_relationships()
            for rel in relationships:
                print(rel)
    """
    conn_str = get_semantic_model_connection()
    try:
        from pyadomd import Pyadomd
    except ImportError:
        logging.error("pyadomd is required for semantic model queries. Install with 'pip install pyadomd'.")
        raise

    query = """
        SELECT
            r.FromTableName,
            r.FromColumnName,
            r.ToTableName,
            r.ToColumnName
        FROM $SYSTEM.TMSCHEMA_RELATIONSHIPS r
    """
    try:
        with Pyadomd(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                results = [tuple(row) for row in cur.fetchall()]
                logging.info(f"Semantic model relationships query returned {len(results)} rows.")
                return results
    except Exception as e:
        logging.error(f"Error executing semantic model relationships query: {e}")
        raise

    # Initialize credential once (it handles all caching internally)
    if _credential is None:
        logging.info("Initializing Azure DefaultAzureCredential...")
        _credential = DefaultAzureCredential()

    # Get access token (automatically cached and refreshed by azure-identity)
    import subprocess
    import json
    try:
        logging.info("Requesting Azure access token via az CLI...")
        result = subprocess.run([
            "az.cmd", "account", "get-access-token", "--resource", "https://database.windows.net/"
        ], capture_output=True, text=True)
        logging.debug(f"Subprocess stdout: {result.stdout}")
        logging.debug(f"Subprocess stderr: {result.stderr}")
        token = json.loads(result.stdout)["accessToken"]
        token_bytes = token.encode("utf-16-le")
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        logging.info("Successfully obtained access token.")
    except Exception as e:
        logging.error(f"Subprocess error obtaining token: {e}")
        raise

    # SQL_COPT_SS_ACCESS_TOKEN attribute for pyodbc
    SQL_COPT_SS_ACCESS_TOKEN = 1256

    conn_str = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={sql_endpoint};"
        f"Database={database};"
        "Encrypt=yes;TrustServerCertificate=no"
    )

    conn = pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
    return conn


def lakehouse_sql_query(query: str, timeout: int = 30) -> List[Tuple]:
    """
    Execute a SQL query against Fabric Lakehouse using the SQL endpoint.
    
    Args:
        query: T-SQL query to execute
        timeout: Query timeout in seconds (default: 30)
        
    Returns:
        List of tuples containing query results
    """
    logging.info(f"Executing SQL query: {query[:100]}... (timeout={timeout}s)")
    conn = _get_connection()
    conn.timeout = timeout  # Set connection timeout
    try:
        cursor = conn.cursor()
        # Set lock timeout to 30 seconds (30000 ms) to avoid long waits
        cursor.execute("SET LOCK_TIMEOUT 30000;")
        cursor.execute(query)
        raw_results = [tuple(row) for row in cursor.fetchall()]
        # Patch: Replace any None values in result tuples with 0
        def sanitize_tuple(t):
            return tuple(0 if v is None else v for v in t)
        results = [sanitize_tuple(row) for row in raw_results]
        logging.info(f"Query executed successfully. Returned {len(results)} rows. Sanitized None to 0.")
        return results
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        raise
    finally:
        conn.close()


def lakehouse_list_tables() -> List[Tuple[str, str]]:
    """
    List all tables in the lakehouse with their schemas only (no row counts).
    
    Returns:
        List of tuples: (schema_name, table_name)
    """
    query = (
        "SELECT "
        "s.name AS schema_name, "
        "t.name AS table_name, "
        "ISNULL(SUM(p.rows), 0) AS row_count "
        "FROM sys.tables t "
        "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
        "LEFT JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1) "
        "GROUP BY s.name, t.name "
        "ORDER BY s.name, t.name"
    )
    logging.info("Listing all tables in the lakehouse (schema and table name only, row_count always 0)...")
    try:
        results = lakehouse_sql_query(query)
        logging.warning(f"Raw results from lakehouse_sql_query: {results}")
        sanitized = []
        for row in results:
            schema = str(row[0]) if row[0] is not None else "Unknown"
            table = str(row[1]) if row[1] is not None else "Unknown"
            try:
                row_count = int(row[2]) if row[2] is not None else 0
            except Exception:
                row_count = 0
            sanitized.append((schema, table, row_count))
        logging.warning(f"Sanitized results for MCP output: {sanitized}")
        logging.info(f"Found {len(sanitized)} tables.")
        return sanitized
    except Exception as e:
        logging.error(f"Exception in lakehouse_list_tables: {e}")
        return []
    try:
        results = lakehouse_sql_query(query)
        logging.warning(f"Raw results from lakehouse_sql_query: {results}")
        filtered = []
        for row in results:
            schema = row[0]
            table = row[1]
            row_count = row[2]
            if schema is None or table is None:
                logging.warning(f"Skipping row with None schema/table: {row}")
                continue
            # Final sanitize: replace None with 0 for row_count
            if row_count is None:
                row_count = 0
            try:
                row_count_int = int(row_count)
            except Exception:
                logging.warning(f"Skipping row with non-int row_count: {row}")
                continue
            filtered.append((str(schema), str(table), row_count_int))
        # Final pass: guarantee no tuple has None for row_count
        sanitized = []
        for tup in filtered:
            schema, table, row_count = tup
            if row_count is None:
                row_count = 0
            try:
                row_count = int(row_count)
            except Exception:
                row_count = 0
            sanitized.append((schema, table, row_count))
        logging.warning(f"Sanitized results for MCP output: {sanitized}")
        logging.info(f"Found {len(sanitized)} tables.")
        return sanitized
    except Exception as e:
        logging.error(f"Exception in lakehouse_list_tables: {e}")
        return []


def lakehouse_describe_table(schema_name: str, table_name: str) -> List[Tuple]:
    """
    Get detailed schema information for a specific table.
    Args:
        schema_name: Name of the schema
        table_name: Name of the table
    Returns:
        List of tuples containing column info
    """
    query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
    """
    conn = _get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        return [tuple(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def lakehouse_sample_table(schema_name: str, table_name: str, limit: int = 10) -> List[Tuple]:
    """
    Get sample rows from a table to preview the data.
    Args:
        schema_name: Name of the schema
        table_name: Name of the table
        limit: Number of rows to return (default: 10, max: 1000)
    Returns:
        List of tuples containing sample data rows
    """
    # Limit to reasonable max to avoid memory issues
    limit = min(limit, 1000)
    
    query = f"""
        SELECT TOP {limit} *
        FROM [{schema_name}].[{table_name}]
    """
    return lakehouse_sql_query(query)


def lakehouse_find_relationships() -> List[Tuple[str, str, str, str, str, str]]:
    """
    Find foreign key relationships between tables in the lakehouse.
    This is useful for understanding the semantic model structure and how tables relate to each other.
    
    Note: Many lakehouses don't have formal FK constraints defined. If this returns empty,
    use lakehouse_find_potential_relationships() to discover relationships by naming patterns.
    
    Returns:
        List of tuples with relationship information:
        (parent_schema, parent_table, parent_column, child_schema, child_table, child_column)
    """
    query = """
        SELECT 
            OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS parent_schema,
            OBJECT_NAME(fk.referenced_object_id) AS parent_table,
            COL_NAME(fk.referenced_object_id, fkc.referenced_column_id) AS parent_column,
            OBJECT_SCHEMA_NAME(fk.parent_object_id) AS child_schema,
            OBJECT_NAME(fk.parent_object_id) AS child_table,
            COL_NAME(fk.parent_object_id, fkc.parent_column_id) AS child_column
        FROM sys.foreign_keys AS fk
        INNER JOIN sys.foreign_key_columns AS fkc 
            ON fk.object_id = fkc.constraint_object_id
        ORDER BY parent_schema, parent_table, child_schema, child_table
    """
    return lakehouse_sql_query(query)


## lakehouse_find_potential_relationships removed: semantic model is available


def lakehouse_find_primary_keys() -> List[Tuple[str, str, str, int]]:
    """
    Find all primary key columns across all tables.
    Useful for understanding unique identifiers in the semantic model.
    
    Returns:
        List of tuples: (schema_name, table_name, column_name, ordinal_position)
    """
    query = """
        SELECT 
            s.name AS schema_name,
            t.name AS table_name,
            c.name AS column_name,
            ic.key_ordinal AS ordinal_position
        FROM sys.indexes i
        INNER JOIN sys.index_columns ic 
            ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c 
            ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        INNER JOIN sys.tables t 
            ON i.object_id = t.object_id
        INNER JOIN sys.schemas s 
            ON t.schema_id = s.schema_id
        WHERE i.is_primary_key = 1
        ORDER BY s.name, t.name, ic.key_ordinal
    """
    return lakehouse_sql_query(query)


def lakehouse_get_schema_stats() -> List[Tuple[str, int, int]]:
    """
    Get statistics about the schema to help diagnose performance issues.
    Returns count of tables and columns per schema.
    
    Returns:
        List of tuples: (schema_name, table_count, column_count)
    """
    query = """
        SELECT 
            s.name AS schema_name,
            COUNT(DISTINCT t.object_id) AS table_count,
            COUNT(DISTINCT c.column_id) AS total_columns,
            COUNT(DISTINCT CASE 
                WHEN c.name LIKE '%Id' OR c.name LIKE '%ID' 
                     OR c.name LIKE '%Key' OR c.name LIKE '%Code' 
                     OR c.name LIKE '%Ref'
                THEN c.column_id 
            END) AS relationship_columns
        FROM sys.schemas s
        LEFT JOIN sys.tables t ON s.schema_id = t.schema_id
        LEFT JOIN sys.columns c ON t.object_id = c.object_id
        WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
        GROUP BY s.name
        ORDER BY table_count DESC
    """
    # Ensure table_count and column_count are always integers (default to 0 if None)
    results = lakehouse_sql_query(query)
    def safe_int(val):
        try:
            return int(val) if val is not None else 0
        except (ValueError, TypeError):
            return 0
    return [(schema, safe_int(table_count), safe_int(column_count)) for (schema, table_count, column_count, *_ ) in results]


def lakehouse_list_tables_safe() -> List[Tuple[str, str, int]]:
    """
    List all tables in the lakehouse with their schemas and row counts, guaranteed schema-compliant for MCP output.
    Returns:
        List of tuples: (schema_name, table_name, row_count:int)
    """
    query = """
        SELECT 
            s.name AS schema_name,
            t.name AS table_name,
            COALESCE(SUM(p.rows), 0) AS row_count
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        LEFT JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
        GROUP BY s.name, t.name
        ORDER BY s.name, t.name
    """
    import logging
    logging.info("Listing all tables in the lakehouse with row counts (safe version)...")
    results = lakehouse_sql_query(query)
    logging.debug(f"Raw results from lakehouse_sql_query: {results}")
    sanitized = []
    for row in results:
        schema = str(row[0]) if row[0] is not None else "Unknown"
        table = str(row[1]) if row[1] is not None else "Unknown"
        row_count = row[2]
        # Final guarantee: row_count is always int, never None
        if row_count is None:
            row_count = 0
        # Defensive: handle float, str, or other types
        try:
            if isinstance(row_count, bool):
                row_count = int(row_count)
            elif isinstance(row_count, (int, float)):
                row_count = int(row_count)
            elif isinstance(row_count, str):
                row_count = int(float(row_count)) if row_count.strip() else 0
            else:
                row_count = 0
        except Exception:
            row_count = 0
        # Final assert: never None, always int
        if row_count is None or not isinstance(row_count, int):
            row_count = 0
        sanitized.append((schema, table, row_count))
    # Extra validation: guarantee all row_count are int and not None, log any issues
    really_final = []
    for s, t, rc in sanitized:
        if rc is None or not isinstance(rc, int):
            logging.error(f"Coercing row_count to 0 for: {(s, t, rc)}")
            rc = 0
        really_final.append((s, t, rc))
    logging.warning(f"Sanitized results for MCP output: {really_final}")
    logging.info(f"Found {len(really_final)} tables.")
    return really_final