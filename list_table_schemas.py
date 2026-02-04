"""
Script to list detailed schema information for all tables.
"""

import os
import sys
import traceback
import pyodbc
from typing import List, Tuple, Any
import pyodbc

def get_table_schema(cursor: pyodbc.Cursor, schema_name: str, table_name: str) -> List[Tuple[Any, ...]]:
    """Get detailed schema information for a specific table."""
    query = """
    SELECT 
        c.COLUMN_NAME,
        c.DATA_TYPE,
        c.CHARACTER_MAXIMUM_LENGTH,
        c.IS_NULLABLE,
        COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') as IS_IDENTITY
    FROM INFORMATION_SCHEMA.COLUMNS c
    WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
    ORDER BY c.ORDINAL_POSITION
    """
    cursor.execute(query, (schema_name, table_name))
    return cursor.fetchall()

def main():
    sql_endpoint = os.getenv("FABRIC_SQL_ENDPOINT")
    database = os.getenv("FABRIC_LAKEHOUSE_NAME")
    if not sql_endpoint or not database:
        print("FABRIC_SQL_ENDPOINT and FABRIC_LAKEHOUSE_NAME must be set in the environment.")
        sys.exit(1)

    print(f"Using endpoint: {sql_endpoint}  database: {database}")

    try:
        conn_str = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server=tcp:{sql_endpoint},1433;"
            f"Database={database};"
            "Encrypt=yes;TrustServerCertificate=no;"
            "Authentication=ActiveDirectoryInteractive"
        )

        print("Connecting to the SQL endpoint via ODBC (this will open a browser window for login)...")
        conn = pyodbc.connect(conn_str)
        
        print("Connected. Querying tables and their schemas...")
        with conn.cursor() as cursor:
            # Get all tables
            cursor.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)
            tables = cursor.fetchall()
            
            if not tables:
                print("\nNo tables found.")
                return

            current_schema = None
            for schema_name, table_name in tables:
                # Print schema header if we're in a new schema
                if schema_name != current_schema:
                    current_schema = schema_name
                    print(f"\n\n=== Schema: {schema_name} ===")
                
                print(f"\nTable: {table_name}")
                print("-" * (len(table_name) + 7))
                
                # Get and print column information
                columns = get_table_schema(cursor, schema_name, table_name)
                col_header = "{:<30} {:<15} {:<10} {:<10} {:<8}".format(
                    "Column Name", "Data Type", "Length", "Nullable", "Identity"
                )
                print(col_header)
                print("-" * 73)
                
                for col_name, data_type, max_length, is_nullable, is_identity in columns:
                    # Format max_length to show 'max' for -1 values
                    # Convert max_length to int if it's not None
                    max_length_int = int(max_length) if max_length is not None else None
                    # Format max_length to show 'max' for -1 values
                    length_str = 'max' if max_length_int == -1 else str(max_length) if max_length else 'N/A'
                    col_info = "{:<30} {:<15} {:<10} {:<10} {:<8}".format(
                        col_name, data_type, length_str, is_nullable, bool(is_identity)
                    )
                    print(col_info)

    except Exception as e:
        print("\nError while querying the lakehouse:")
        print(f"Error: {str(e)}")
        traceback.print_exc()
        sys.exit(2)

    print("\nDone.")

if __name__ == "__main__":
    main()