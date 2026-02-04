from dotenv import load_dotenv
load_dotenv()
from fabric_rti_mcp.tools.query_suggestions_mcp_tools import get_starter_queries, get_schema_exploration_guide

if __name__ == "__main__":
    schema = "Starbase_Lakehouse"
    table = "<your_table_name>"  # Replace with a real table name for best results
    print("--- Starter Queries ---")
    print(get_starter_queries(schema, table))
    print("\n--- Schema Exploration Guide ---")
    print(get_schema_exploration_guide(schema))
