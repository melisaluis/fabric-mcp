"""
Comprehensive Starbase data analysis - understand schemas and tables.
"""
import os
from fabric_rti_mcp.tools.lakehouse_sql_tool import lakehouse_sql_query, lakehouse_get_schema_stats

os.environ["FABRIC_SQL_ENDPOINT"] = "x6eps4xrq2xudenlfv6naeo3i4-k7qssngppcxuhaxhdc2h6hdjgq.msit-datawarehouse.fabric.microsoft.com"
os.environ["FABRIC_LAKEHOUSE_NAME"] = "Starbase"

print("=" * 100)
print("STARBASE LAKEHOUSE - DATA OVERVIEW")
print("=" * 100)

# Get schema statistics
print("\n1. SCHEMA OVERVIEW")
print("-" * 100)

# Query schema stats directly
schema_query = """
    SELECT 
        s.name AS schema_name,
        COUNT(DISTINCT t.name) AS table_count,
        ISNULL(SUM(p.rows), 0) AS total_rows
    FROM sys.schemas s
    LEFT JOIN sys.tables t ON t.schema_id = s.schema_id
    LEFT JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0, 1)
    WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest', 'db_owner', 'db_accessadmin', 
                         'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader', 
                         'db_datawriter', 'db_denydatareader', 'db_denydatawriter')
    GROUP BY s.name
    HAVING COUNT(DISTINCT t.name) > 0
    ORDER BY SUM(p.rows) DESC
"""

schemas = lakehouse_sql_query(schema_query)

total_tables = sum(s[1] for s in schemas)
total_rows = sum(s[2] for s in schemas)

print(f"\nTotal Schemas: {len(schemas)}")
print(f"Total Tables: {total_tables}")
print(f"Total Rows: {total_rows:,}\n")

for schema_name, table_count, row_count in schemas:
    print(f"[{schema_name:25s}] | {table_count:3d} tables | {row_count:15,} rows")

# Get detailed table info for each schema
print("\n\n2. DETAILED TABLE BREAKDOWN BY SCHEMA")
print("-" * 100)

for schema_name, _, _ in schemas:
    print(f"\n{'='*100}")
    print(f"Schema: {schema_name}")
    print(f"{'='*100}")
    
    # Get tables in this schema with row counts
    tables_query = f"""
        SELECT 
            t.name AS table_name,
            SUM(p.rows) AS row_count
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        LEFT JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
        WHERE s.name = '{schema_name}'
        GROUP BY t.name
        ORDER BY SUM(p.rows) DESC
    """
    
    tables = lakehouse_sql_query(tables_query)
    
    if tables:
        print(f"\n{len(tables)} tables:\n")
        for table_name, row_count in tables:
            # Get column count
            col_query = f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
            """
            col_count = lakehouse_sql_query(col_query)[0][0]
            
            # Categorize by size
            if row_count == 0 or row_count is None:
                size_cat = "EMPTY"
            elif row_count < 100:
                size_cat = "TINY"
            elif row_count < 1000:
                size_cat = "SMALL"
            elif row_count < 100000:
                size_cat = "MEDIUM"
            elif row_count < 1000000:
                size_cat = "LARGE"
            else:
                size_cat = "HUGE"
            
            print(f"  [{table_name:40s}] | {row_count or 0:12,} rows | {col_count:3d} cols | {size_cat:6s}")

# Schema purpose analysis
print("\n\n3. SCHEMA PURPOSE ANALYSIS")
print("-" * 100)

schema_purposes = {
    "Common": "Shared reference data - tenants, capacities, dimensions that other schemas reference",
    "ICM": "Incident Management - tracking incidents, tickets, and support cases",
    "AIR": "Automated Incident Response - automated handling and response to incidents",
    "FW": "Firmware/Framework - firmware versions, updates, device management",
    "SafetyIncidents": "Safety-related incident tracking and management",
    "FC": "Flight Control or Fabric Control - operational control systems",
    "OOS": "Out Of Service - capacity/service availability tracking",
    "DT": "Data/Digital Twin - device or system modeling data",
    "CICD": "Continuous Integration/Continuous Deployment - DevOps pipeline data"
}

print("\nSchema Purposes:\n")
for schema_name, _, _ in schemas:
    purpose = schema_purposes.get(schema_name, "Purpose unclear - needs investigation")
    print(f"[{schema_name:20s}] - {purpose}")

# Find potential key relationships
print("\n\n4. KEY RELATIONSHIP PATTERNS")
print("-" * 100)

print("\nLooking for common ID columns that suggest relationships...\n")

# Find tables with TenantId (likely link to Common.Tenants)
tenant_query = """
    SELECT DISTINCT 
        TABLE_SCHEMA,
        TABLE_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE COLUMN_NAME LIKE '%TenantId%'
    ORDER BY TABLE_SCHEMA, TABLE_NAME
"""
tenant_tables = lakehouse_sql_query(tenant_query)

if tenant_tables:
    print(f">> Tables with TenantId (likely relate to Common.Tenants):")
    for schema, table in tenant_tables:
        print(f"   {schema}.{table}")

# Find tables with common Date/Time columns
date_query = """
    SELECT DISTINCT 
        TABLE_SCHEMA,
        TABLE_NAME,
        COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE (COLUMN_NAME LIKE '%Date' OR COLUMN_NAME LIKE '%Time%')
        AND DATA_TYPE IN ('datetime', 'datetime2', 'date')
    ORDER BY TABLE_SCHEMA, TABLE_NAME
"""
date_columns = lakehouse_sql_query(date_query)

print(f"\n>> Tables with Date/Time tracking (for temporal analysis):")
date_by_schema = {}
for schema, table, col in date_columns:
    if schema not in date_by_schema:
        date_by_schema[schema] = []
    if (schema, table) not in [(s, t) for s, t, _ in date_by_schema[schema]]:
        date_by_schema[schema].append((schema, table, col))

for schema in sorted(date_by_schema.keys()):
    print(f"\n   {schema}:")
    for s, table, col in date_by_schema[schema][:5]:  # Top 5 per schema
        print(f"      {table}.{col}")

print("\n\n" + "=" * 100)
print("ANALYSIS COMPLETE")
print("=" * 100)
