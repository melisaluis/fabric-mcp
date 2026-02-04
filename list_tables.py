from fabric_rti_mcp.tools.lakehouse_sql_tool import lakehouse_sql_query

tables = lakehouse_sql_query("""
    SELECT 
        TABLE_SCHEMA as schema_name,
        TABLE_NAME as table_name,
        TABLE_TYPE as type
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_SCHEMA, TABLE_NAME
""")

print('\nTables in database:')
print('-' * 50)
for schema, table, type in tables:
    print(f'{schema}.{table} ({type})')