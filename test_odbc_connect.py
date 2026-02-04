import pyodbc

conn_str = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=x6eps4xrq2xudenlfv6naeo3i4-k7qssngppcxuhaxhdc2h6hdjgq.msit-datawarehouse.fabric.microsoft.com;"
    "Database=Starbase;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

try:
    conn = pyodbc.connect(conn_str)
    print("ODBC connection successful!")
    conn.close()
except Exception as e:
    print(f"ODBC connection failed: {e}")
