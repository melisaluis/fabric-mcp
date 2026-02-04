from dotenv import load_dotenv
load_dotenv()
from fabric_rti_mcp.tools.query_history_mcp_tools import view_query_history, view_active_sessions, view_table_usage_stats

if __name__ == "__main__":
    print("--- Query History ---")
    print(view_query_history())
    print("\n--- Active Sessions ---")
    print(view_active_sessions())
    print("\n--- Table Usage Stats ---")
    print(view_table_usage_stats())
