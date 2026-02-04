#!/usr/bin/env python3
#"""
#List all tables in the Fabric Lakehouse SQL endpoint, showing schema and table names only.
#Usage (PowerShell):
#    $env:FABRIC_SQL_ENDPOINT = "your-sql-endpoint.fabric.windows.net"
#    $env:FABRIC_LAKEHOUSE_NAME = "YourLakehouseDatabaseName"
#This script uses ActiveDirectoryInteractive authentication which will prompt for credentials
#in your default web browser.
#Make sure you have:
#    pip install pyodbc
#    ODBC Driver 18 for SQL Server installed (matching your Python bitness)
#"""
import os
import sys
import pyodbc
SQL_TABLES_QUERY = """
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_SCHEMA, TABLE_NAME
"""
def list_rows(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
def main():
    sql_endpoint = os.getenv("FABRIC_SQL_ENDPOINT")
    database = os.getenv("FABRIC_LAKEHOUSE_NAME")
    if not sql_endpoint or not database:
        print("FABRIC_SQL_ENDPOINT and FABRIC_LAKEHOUSE_NAME must be set in the environment.")
        sys.exit(1)
    conn_str = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={sql_endpoint};"
    f"Database={database};"
    "Authentication=ActiveDirectoryInteractive;"
    "Encrypt=yes;TrustServerCertificate=no"
    )
    conn = pyodbc.connect(conn_str)
    tables = list_rows(conn, SQL_TABLES_QUERY)
    if tables:
        print("\nTables found:")
        for schema, name in tables:
            print(f" - {schema}.{name}")
    else:
        print("\nNo tables found in the lakehouse.")
    conn.close()

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Run this locally to list tables in the Fabric Lakehouse SQL endpoint.

Usage (PowerShell):
    $env:FABRIC_SQL_ENDPOINT = "your-sql-endpoint.fabric.windows.net"
    $env:FABRIC_LAKEHOUSE_NAME = "YourLakehouseDatabaseName"
    python list_lakehouse_tables.py

This script uses ActiveDirectoryInteractive authentication which will prompt for credentials
in your default web browser.

Make sure you have:
    pip install pyodbc
    ODBC Driver 18 for SQL Server installed (matching your Python bitness)
"""

import os
import sys
from typing import List, Tuple
import pyodbc

SQL_TABLES_QUERY = """
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_SCHEMA, TABLE_NAME
"""

def list_rows(conn: pyodbc.Connection, sql: str) -> List[Tuple[str, ...]]:
    with conn.cursor() as cur:
        cur.execute(sql)
        return [tuple(row) for row in cur.fetchall()]

def main() -> None:
    conn = None
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

        print("\nQuerying tables...")
        tables = list_rows(conn, SQL_TABLES_QUERY)
        if tables:
            print("\nTables found:")
            for schema, name in tables:
                print(f" - {schema}.{name}")
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