"""
Test if semantic model relationship metadata is available.
"""
import os
from fabric_rti_mcp.tools.lakehouse_sql_tool import lakehouse_sql_query

os.environ["FABRIC_SQL_ENDPOINT"] = "x6eps4xrq2xudenlfv6naeo3i4-k7qssngppcxuhaxhdc2h6hdjgq.msit-datawarehouse.fabric.microsoft.com"
os.environ["FABRIC_LAKEHOUSE_NAME"] = "Starbase"

print("Testing for semantic model relationship metadata...\n")

# Test 1: Traditional SQL Server foreign keys
print("1. Checking sys.foreign_keys (SQL Server style):")
try:
    fks = lakehouse_sql_query("SELECT COUNT(*) FROM sys.foreign_keys")
    print(f"   Found {fks[0][0]} foreign keys")
except Exception as e:
    print(f"   Error: {e}")

# Test 2: TMSCHEMA views (Power BI semantic model)
print("\n2. Checking TMSCHEMA_RELATIONSHIPS (Semantic model):")
try:
    rels = lakehouse_sql_query("SELECT COUNT(*) FROM TMSCHEMA_RELATIONSHIPS")
    print(f"   Found {rels[0][0]} relationships")
except Exception as e:
    print(f"   Error: {str(e)[:100]}")

# Test 3: Check available TMSCHEMA views
print("\n3. Checking available TMSCHEMA views:")
try:
    views = lakehouse_sql_query("""
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME LIKE 'TMSCHEMA%'
        ORDER BY TABLE_NAME
    """)
    if views:
        print(f"   Found {len(views)} TMSCHEMA views:")
        for view in views:
            print(f"      - {view[0]}")
    else:
        print("   No TMSCHEMA views found")
except Exception as e:
    print(f"   Error: {str(e)[:100]}")

# Test 4: Check sys.objects for what's available
print("\n4. What system metadata is available:")
try:
    sys_objects = lakehouse_sql_query("""
        SELECT DISTINCT type_desc
        FROM sys.objects
        WHERE is_ms_shipped = 1
        ORDER BY type_desc
    """)
    print(f"   Found {len(sys_objects)} system object types:")
    for obj in sys_objects[:10]:
        print(f"      - {obj[0]}")
except Exception as e:
    print(f"   Error: {str(e)[:100]}")

# Test 5: Sample some actual foreign keys if they exist
print("\n5. Sample foreign key data (if any):")
try:
    sample = lakehouse_sql_query("""
        SELECT TOP 5
            OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS parent_schema,
            OBJECT_NAME(fk.referenced_object_id) AS parent_table,
            OBJECT_SCHEMA_NAME(fk.parent_object_id) AS child_schema,
            OBJECT_NAME(fk.parent_object_id) AS child_table
        FROM sys.foreign_keys AS fk
    """)
    if sample:
        print(f"   Found {len(sample)} sample relationships:")
        for s in sample:
            print(f"      {s[0]}.{s[1]} ← {s[2]}.{s[3]}")
    else:
        print("   No foreign keys defined")
except Exception as e:
    print(f"   Error: {str(e)[:100]}")
