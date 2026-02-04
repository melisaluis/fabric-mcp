"""
Test script to create a second connection and keep it open.
Run this in one terminal, then run test_local.py in another terminal.
"""
import os
import time
import pyodbc

# Set credentials
os.environ["FABRIC_SQL_ENDPOINT"] = "x6eps4xrq2xudenlfv6naeo3i4-k7qssngppcxuhaxhdc2h6hdjgq.msit-datawarehouse.fabric.microsoft.com"
os.environ["FABRIC_LAKEHOUSE_NAME"] = "Starbase"

sql_endpoint = os.environ["FABRIC_SQL_ENDPOINT"]
database = os.environ["FABRIC_LAKEHOUSE_NAME"]

print("Creating second connection to Starbase...")
print("=" * 60)

# Connection string with interactive auth (same as test_local.py uses)
conn_str = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server={sql_endpoint};"
    f"Database={database};"
    "Authentication=ActiveDirectoryInteractive;"
    "Encrypt=yes;TrustServerCertificate=no"
)

# Create connection
print("Connecting... (you may see Azure login prompt)")
conn = pyodbc.connect(conn_str)

print(f"✅ Connected to {database}")
print("\nThis connection will stay open for 5 minutes.")
print("Open ANOTHER terminal and run: python test_local.py")
print("You should see 2 active sessions!\n")

# Keep connection alive
for i in range(30):
    time.sleep(10)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    cursor.fetchone()
    print(f"[{i+1}/30] Connection still alive... ({10*(i+1)} seconds)")

print("\n✅ Test complete. Closing connection.")
conn.close()
