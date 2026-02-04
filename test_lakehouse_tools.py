"""
Direct Python script to test lakehouse tool functions without MCP server.
"""
from fabric_rti_mcp.tools.lakehouse_sql_tool import lakehouse_list_tables
from fabric_rti_mcp.tools.onboarding_service import profile_table, get_schema_overview, generate_database_guide

# Test lakehouse_list_tables
print("Testing lakehouse_list_tables...")
tables = lakehouse_list_tables()
for schema, table, row_count in tables:
    print(f"Schema: {schema}, Table: {table}, Row count: {row_count}")

# Test get_schema_overview (replace 'your_schema' with a real schema name)
# print("\nTesting get_schema_overview...")
# overview = get_schema_overview('your_schema')
# print(overview)

# Test profile_table (replace 'your_schema' and 'your_table' with real names)
# print("\nTesting profile_table...")
# profile = profile_table('your_schema', 'your_table')
# print(profile)

# Test generate_database_guide
# print("\nTesting generate_database_guide...")
# guide = generate_database_guide()
# print(guide)
