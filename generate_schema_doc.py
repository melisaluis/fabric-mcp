"""Generate comprehensive schema documentation for Starbase lakehouse."""
import os
from fabric_rti_mcp.tools.lakehouse_sql_tool import (
    lakehouse_list_tables,
    lakehouse_get_schema_stats,
    lakehouse_describe_table,
    lakehouse_find_potential_relationships,
)

# Set your credentials here
os.environ["FABRIC_SQL_ENDPOINT"] = "x6eps4xrq2xudenlfv6naeo3i4-k7qssngppcxuhaxhdc2h6hdjgq.msit-datawarehouse.fabric.microsoft.com"
os.environ["FABRIC_LAKEHOUSE_NAME"] = "Starbase"

def main():
    print("=" * 80)
    print("STARBASE LAKEHOUSE - DATA SCHEMA DOCUMENTATION")
    print("=" * 80)
    
    try:
        print("\n\n📋 SECTION 1: SCHEMA OVERVIEW")
        print("-" * 80)
        stats = lakehouse_get_schema_stats()
        print(f"\n{'Schema Name':<25} {'Tables':<10} {'Columns':<10} {'Primary Keys':<15}")
        print("-" * 80)
        for schema in stats[:10]:  # Top 10 schemas
            schema_name, table_count, column_count, pk_count = schema
            if table_count > 0:  # Skip empty schemas
                print(f"{schema_name:<25} {table_count:<10} {column_count:<10} {pk_count:<15}")
        
        print("\n\n📊 SECTION 2: DETAILED TABLE SCHEMAS")
        print("=" * 80)
        
        # Get list of tables grouped by schema
        tables = lakehouse_list_tables()
        schemas_to_document = ['Common', 'ICM', 'AIR', 'FW', 'SafetyIncidents']  # Main schemas
        
        for schema_name in schemas_to_document:
            schema_tables = [t for t in tables if t[0] == schema_name]
            if not schema_tables:
                continue
                
            print(f"\n\n{'='*80}")
            print(f"📁 SCHEMA: {schema_name} ({len(schema_tables)} tables)")
            print(f"{'='*80}")
            
            for table_info in schema_tables[:8]:  # First 8 tables per schema
                schema, table_name, row_count = table_info
                print(f"\n┌─ Table: {schema}.{table_name}")
                print(f"│  Row Count: {row_count if row_count is not None else 'Unknown'}")
                
                # Get table structure
                try:
                    columns = lakehouse_describe_table(schema, table_name)
                    print(f"│  Columns: {len(columns)}")
                    print("│")
                    for col in columns[:15]:  # First 15 columns
                        col_name, data_type, max_length, precision, scale, is_nullable, is_identity = col
                        nullable = "NULL" if is_nullable else "NOT NULL"
                        identity = " [IDENTITY]" if is_identity else ""
                        
                        # Format data type with length/precision
                        if data_type in ['varchar', 'nvarchar', 'char', 'nchar'] and max_length:
                            type_str = f"{data_type}({max_length if max_length != -1 else 'MAX'})"
                        elif data_type in ['decimal', 'numeric'] and precision:
                            type_str = f"{data_type}({precision},{scale})"
                        else:
                            type_str = data_type
                        
                        print(f"│    • {col_name:<35} {type_str:<20} {nullable}{identity}")
                    
                    if len(columns) > 15:
                        print(f"│    ... and {len(columns) - 15} more columns")
                except Exception as e:
                    print(f"│  ⚠️  Error getting columns: {str(e)[:50]}")
                
                print("└" + "─" * 78)
            
            if len(schema_tables) > 8:
                print(f"\n   📄 ... and {len(schema_tables) - 8} more tables in {schema_name} schema")
        
        print("\n\n🔗 SECTION 3: DATA RELATIONSHIPS")
        print("=" * 80)
        potential_rels = lakehouse_find_potential_relationships()
        print(f"\nFound {len(potential_rels)} potential relationships based on column naming patterns:\n")
        
        # Group by linking column
        from collections import defaultdict
        by_column = defaultdict(list)
        for rel in potential_rels:
            by_column[rel[4]].append((rel[0], rel[1], rel[2], rel[3]))
        
        for col_name, relationships in sorted(by_column.items()):
            print(f"\n🔑 Linking Column: {col_name}")
            for schema1, table1, schema2, table2 in relationships[:5]:
                print(f"   • {schema1}.{table1} ↔ {schema2}.{table2}")
            if len(relationships) > 5:
                print(f"   ... and {len(relationships) - 5} more relationships")
        
        print("\n\n" + "=" * 80)
        print("✅ Schema documentation complete!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
