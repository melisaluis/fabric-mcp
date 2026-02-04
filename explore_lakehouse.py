"""
Script to explore your Fabric Lakehouse schema
Run this after setting your actual FABRIC_SQL_ENDPOINT and FABRIC_LAKEHOUSE_NAME
"""
import os
import sys

# Add the module to path
sys.path.insert(0, os.path.dirname(__file__))

from fabric_rti_mcp.tools.lakehouse_sql_tool import (
    lakehouse_list_tables,
    lakehouse_describe_table
)

def main():
    # Set your actual values here
    os.environ["FABRIC_SQL_ENDPOINT"] = "x6eps4xrq2xudenlfv6naeo3i4-k7qssngppcxuhaxhdc2h6hdjgq.msit-datawarehouse.fabric.microsoft.com"
    os.environ["FABRIC_LAKEHOUSE_NAME"] = "Starbase"
    
    print("=" * 80)
    print("FABRIC LAKEHOUSE SCHEMA EXPLORER")
    print("=" * 80)
    
    try:
        # List all tables
        print("\n📊 Listing all tables...\n")
        tables = lakehouse_list_tables()
        
        if not tables:
            print("No tables found in the lakehouse.")
            return
        
        print(f"Found {len(tables)} table(s):\n")
        
        for schema_name, table_name, row_count in tables:
            row_count_str = f"{row_count:,}" if row_count is not None else "unknown"
            print(f"  • {schema_name}.{table_name} ({row_count_str} rows)")
        
        # Get detailed schema for each table
        print("\n" + "=" * 80)
        print("DETAILED TABLE SCHEMAS")
        print("=" * 80)
        
        for schema_name, table_name, _ in tables:
            print(f"\n📋 Table: {schema_name}.{table_name}")
            print("-" * 80)
            
            columns = lakehouse_describe_table(schema_name, table_name)
            
            print(f"{'Column Name':<30} {'Data Type':<20} {'Nullable':<10}")
            print("-" * 80)
            
            for col in columns:
                col_name = col[0]
                data_type = col[1]
                is_nullable = "YES" if col[5] == 1 else "NO"
                print(f"{col_name:<30} {data_type:<20} {is_nullable:<10}")
        
        print("\n" + "=" * 80)
        print("✅ Schema exploration complete!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure:")
        print("  1. You've set the correct FABRIC_SQL_ENDPOINT and FABRIC_LAKEHOUSE_NAME")
        print("  2. You're logged in via 'az login'")
        print("  3. You have access to the Lakehouse")

if __name__ == "__main__":
    main()
