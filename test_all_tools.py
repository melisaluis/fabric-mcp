"""
Direct Python script to test all major lakehouse and MCP tools in your repo.
"""
from fabric_rti_mcp.tools.lakehouse_sql_tool import lakehouse_list_tables, lakehouse_sql_query, lakehouse_describe_table, lakehouse_sample_table
from fabric_rti_mcp.tools.onboarding_service import profile_table, get_schema_overview, generate_database_guide
from fabric_rti_mcp.tools.query_history_mcp_tools import view_query_history
from fabric_rti_mcp.tools.query_suggestions_service import get_exploration_guide
from fabric_rti_mcp.tools.semantic_model_service import get_semantic_model_info

print("Testing lakehouse_list_tables...")
try:
    tables = lakehouse_list_tables()
    print("lakehouse_list_tables output:", tables)
except Exception as e:
    print("lakehouse_list_tables error:", e)

print("\nTesting lakehouse_sql_query...")
try:
    result = lakehouse_sql_query("SELECT TOP 1 * FROM INFORMATION_SCHEMA.TABLES")
    print("lakehouse_sql_query output:", result)
except Exception as e:
    print("lakehouse_sql_query error:", e)

print("\nTesting generate_database_guide...")
try:
    guide = generate_database_guide()
    print("generate_database_guide output:", guide)
except Exception as e:
    print("generate_database_guide error:", e)

print("\nTesting view_query_history...")
try:
    history = view_query_history()
    print("view_query_history output:", history)
except Exception as e:
    print("view_query_history error:", e)

print("\nTesting get_exploration_guide...")
try:
    guide = get_exploration_guide("Starbase")
    print("get_exploration_guide output:", guide)
except Exception as e:
    print("get_exploration_guide error:", e)

print("\nTesting get_semantic_model_info...")
try:
    info = get_semantic_model_info()
    print("get_semantic_model_info output:", info)
except Exception as e:
    print("get_semantic_model_info error:", e)
