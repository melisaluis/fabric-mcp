#!/usr/bin/env python3
"""
Run this locally to list tables and their schemas in the Fabric Lakehouse SQL endpoint.

Usage (PowerShell):
    $env:FABRIC_SQL_ENDPOINT = "your-sql-endpoint.fabric.windows.net"
    $env:FABRIC_LAKEHOUSE_NAME = "YourLakehouseDatabaseName"
    python describe_lakehouse_tables.py

This script uses ActiveDirectoryInteractive authentication which will prompt for credentials
in your default web browser.

Make sure you have:
    pip install pyodbc
    ODBC Driver 18 for SQL Server installed (matching your Python bitness)
"""

import os
import sys
from typing import List, Tuple, Optional, Union
import pyodbc
from collections import defaultdict

SCHEMA_QUERY = """
SELECT 
    t.TABLE_SCHEMA,
    t.TABLE_NAME,
    c.COLUMN_NAME,
    c.DATA_TYPE,
    c.CHARACTER_MAXIMUM_LENGTH,
    c.NUMERIC_PRECISION,
    c.NUMERIC_SCALE,
    c.IS_NULLABLE,
    c.COLUMN_DEFAULT
FROM 
    INFORMATION_SCHEMA.TABLES t
    JOIN INFORMATION_SCHEMA.COLUMNS c 
        ON t.TABLE_SCHEMA = c.TABLE_SCHEMA 
        AND t.TABLE_NAME = c.TABLE_NAME
WHERE 
    t.TABLE_TYPE = 'BASE TABLE'
ORDER BY 
    t.TABLE_SCHEMA,
    t.TABLE_NAME,
    c.ORDINAL_POSITION
"""

ColumnInfo = Tuple[str, str, str, str, Optional[int], Optional[int], Optional[int], str, Optional[str]]

def list_rows(conn: pyodbc.Connection, sql: str) -> List[ColumnInfo]:
    with conn.cursor() as cur:
        cur.execute(sql)
        return [tuple(None if v is None else v for v in row) for row in cur.fetchall()]  # type: ignore

def format_column_type(data_type: str, char_max_len: Optional[int], num_precision: Optional[int], num_scale: Optional[int]) -> str:
    if char_max_len is not None:
        if char_max_len == -1:
            return f"{data_type}(max)"
        return f"{data_type}({char_max_len})"
    elif num_precision is not None:
        if num_scale is not None and num_scale > 0:
            return f"{data_type}({num_precision},{num_scale})"
        return f"{data_type}({num_precision})"
    return data_type

TableSchema = List[str]
SchemaDict = defaultdict[str, TableSchema]

def main() -> None:
    conn: Optional[pyodbc.Connection] = None
    try:
        sql_endpoint = os.getenv("FABRIC_SQL_ENDPOINT")
        database = os.getenv("FABRIC_LAKEHOUSE_NAME")
        if not sql_endpoint or not database:
            print("FABRIC_SQL_ENDPOINT and FABRIC_LAKEHOUSE_NAME must be set in the environment.", file=sys.stderr)
            sys.exit(1)

        print(f"Using endpoint: {sql_endpoint}  database: {database}")

        conn_str = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={sql_endpoint};"
            f"Database={database};"
            "Authentication=ActiveDirectoryInteractive;"
            "Encrypt=yes;TrustServerCertificate=no"
        )

        print("Connecting to the SQL endpoint via ODBC (browser authentication will open)...")
        conn = pyodbc.connect(conn_str)

        print("\nQuerying table schemas...")
        results = list_rows(conn, SCHEMA_QUERY)
        
        # Group by schema.table
        tables = defaultdict(list)
        for row in results:
            schema, table, col, dtype, max_len, precision, scale, nullable, default = row
            full_name = f"{schema}.{table}"
            col_type = format_column_type(dtype, max_len, precision, scale)
            nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
            default_str = f" DEFAULT {default}" if default else ""
            col_def = f"{col} {col_type} {nullable_str}{default_str}"
            tables[full_name].append(col_def)

        if tables:
            print("\nTable schemas:")
            for table_name in sorted(tables.keys()):
                print(f"\n{table_name}")
                print("  Columns:")
                for col in tables[table_name]:
                    print(f"    {col}")
        else:
            print("\nNo tables found in the lakehouse.")

    except Exception as e:
        print("Error while querying the lakehouse:", file=sys.stderr)
        print(str(e), file=sys.stderr)
        sys.exit(1)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()