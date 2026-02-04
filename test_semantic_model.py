"""
Test semantic model relationship querying.
"""
import os
from fabric_rti_mcp.tools.semantic_model_service import (
    find_semantic_model_for_lakehouse,
    get_semantic_model_relationships,
    get_lakehouse_semantic_relationships,
    get_semantic_model_info
)

# Set environment variables
os.environ["FABRIC_SQL_ENDPOINT"] = "x6eps4xrq2xudenlfv6naeo3i4-k7qssngppcxuhaxhdc2h6hdjgq.msit-datawarehouse.fabric.microsoft.com"
os.environ["FABRIC_LAKEHOUSE_NAME"] = "Starbase"
os.environ["FABRIC_WORKSPACE_ID"] = "3429e157-78cf-43af-82e7-18b47f1c6934"  # HMCClinic2026 workspace

print("=" * 80)
print("Testing Semantic Model Relationship Querying")
print("=" * 80)

print("\n1. Finding semantic model for Starbase lakehouse...")
print("-" * 80)

try:
    workspace_id = os.environ["FABRIC_WORKSPACE_ID"]
    lakehouse_name = os.environ["FABRIC_LAKEHOUSE_NAME"]
    
    dataset_id = find_semantic_model_for_lakehouse(workspace_id, lakehouse_name)
    
    if dataset_id:
        print(f"✅ Found semantic model: {dataset_id}")
    else:
        print("❌ No semantic model found")
        print("\nThis could mean:")
        print("  - Lakehouse doesn't have a default semantic model")
        print("  - Semantic model has a different name")
        print("  - Need to create semantic model in Fabric portal")
        
except Exception as e:
    print(f"❌ Error: {e}")

print("\n2. Getting relationship definitions...")
print("-" * 80)

try:
    relationships = get_lakehouse_semantic_relationships()
    
    if relationships:
        print(f"✅ Found {len(relationships)} relationships:\n")
        for rel in relationships:
            from_table, from_col, to_table, to_col, rel_name, cross_filter = rel
            print(f"  {rel_name}:")
            print(f"    {from_table}[{from_col}] → {to_table}[{to_col}]")
            print(f"    Cross-filtering: {cross_filter}")
            print()
    else:
        print("No relationships found in semantic model")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

print("\n3. Getting full semantic model info...")
print("-" * 80)

try:
    info = get_semantic_model_info()
    
    if info.get("found"):
        print(f"✅ Semantic Model Found")
        print(f"   Lakehouse: {info['lakehouse_name']}")
        print(f"   Dataset ID: {info['dataset_id']}")
        print(f"   Relationships: {info['relationship_count']}")
    else:
        print(f"❌ {info.get('message')}")
        
except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "=" * 80)
print("Test Complete")
print("=" * 80)
