"""
Modified version of the script to list datasources and tables.
"""

import os
import sys
import traceback
import pyodbc
from azure.identity import DefaultAzureCredential

SQL_DATASOURCES_QUERY = """
SELECT name, data_source_type, location
FROM sys.external_data_sources
ORDER BY name
"""

SQL_TABLES_QUERY = """
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_SCHEMA, TABLE_NAME
"""

def main():
    sql_endpoint = os.getenv("FABRIC_SQL_ENDPOINT")
    database = os.getenv("FABRIC_LAKEHOUSE_NAME")
    if not sql_endpoint or not database:
        print("FABRIC_SQL_ENDPOINT and FABRIC_LAKEHOUSE_NAME must be set in the environment.")
        sys.exit(1)

    print(f"Using endpoint: {sql_endpoint}  database: {database}")

    try:
        print("Acquiring Azure AD token... (DefaultAzureCredential)")
        credential = DefaultAzureCredential()
        # Get token and use it immediately
        conn_str = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server=tcp:{sql_endpoint},1433;"
            f"Database={database};"
            "Encrypt=yes;TrustServerCertificate=no;"
            "Authentication=ActiveDirectoryMSI"
        )

        print("Connecting to the SQL endpoint via ODBC...")
        conn = pyodbc.connect(conn_str)
        
        print("Connected. Querying external data sources...")
        with conn.cursor() as cursor:
            cursor.execute(SQL_DATASOURCES_QUERY)
            datasources = cursor.fetchall()
            if datasources:
                print("\nExternal data sources:")
                for row in datasources:
                    print(" - ", tuple(row))
            else:
                print("\nNo external data sources found.")

            print("\nQuerying tables...")
            cursor.execute(SQL_TABLES_QUERY)
            tables = cursor.fetchall()
            if tables:
                print("\nTables:")
                for schema, name in tables:
                    print(f" - {schema}.{name}")
            else:
                print("\nNo tables found.")

    except Exception as exc:
        print("\nError while querying the lakehouse:")
        print(f"Error: {str(exc)}")
        traceback.print_exc()
        sys.exit(2)

    print("\nDone.")

if __name__ == "__main__":
    main()