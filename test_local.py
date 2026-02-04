"""Quick test script for local MCP development."""
import os
from fabric_rti_mcp.tools.lakehouse_sql_tool import (
    lakehouse_list_tables,
    lakehouse_get_schema_stats,
    lakehouse_describe_table,
    lakehouse_sample_table,
    lakehouse_find_potential_relationships,
    lakehouse_find_relationships,
    lakehouse_sql_query,
)
from fabric_rti_mcp.tools.query_history_tool import get_query_history, get_active_sessions, get_table_usage_stats

# Set your credentials here
os.environ["FABRIC_SQL_ENDPOINT"] = "x6eps4xrq2xudenlfv6naeo3i4-k7qssngppcxuhaxhdc2h6hdjgq.msit-datawarehouse.fabric.microsoft.com"
os.environ["FABRIC_LAKEHOUSE_NAME"] = "Starbase"

def main():
    print("=" * 60)
    print("Testing Fabric Lakehouse MCP Tools (Local Development)")
    print("=" * 60)
    
    try:
        print("\n1. Query History Analysis")
        print("=" * 60)
        history = get_query_history(top_n=100)  # Try to get up to 100
        print(f"Found {len(history)} queries in cache\n")
        
        if len(history) > 0:
            # Find oldest and newest
            oldest = min(history, key=lambda x: x[2])  # last_execution_time
            newest = max(history, key=lambda x: x[2])
            
            print(f"Oldest query in cache: {oldest[2]}")
            print(f"Newest query in cache: {newest[2]}")
            print(f"Cache retention span: {(newest[2] - oldest[2])}")
            print()
            
            # Show summary
            print("Query Summary:")
            print("-" * 70)
            for i, row in enumerate(history[:10], 1):  # Show first 10
                print(f"{i}. Executions: {row[0]:3d} | Last Run: {row[2]} | CPU: {row[3]:.0f}ms")
                query_text = str(row[7])[:80].replace('\n', ' ')
                print(f"   {query_text}...")
                print()

        
        # Uncomment to test other functions
        # print("\n3. Describing a table...")
        # print(lakehouse_describe_table("schema_name", "table_name"))
        
        # print("\n4. Sampling a table...")
        # print(lakehouse_sample_table("schema_name", "table_name", limit=5))
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure to update the credentials at the top of this file!")

if __name__ == "__main__":
    main()
