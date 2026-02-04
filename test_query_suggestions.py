"""
Test the query suggestions tools.
"""
import os
from fabric_rti_mcp.tools.query_suggestions_service import suggest_queries_for_table, get_exploration_path

# Set environment variables for testing
os.environ["FABRIC_SQL_ENDPOINT"] = "x6eps4xrq2xudenlfv6naeo3i4-k7qssngppcxuhaxhdc2h6hdjgq.msit-datawarehouse.fabric.microsoft.com"
os.environ["FABRIC_LAKEHOUSE_NAME"] = "Starbase"

def test_query_suggestions():
    """Test query suggestions for a table."""
    print("=" * 80)
    print("Testing Query Suggestions")
    print("=" * 80)
    
    # Test with ICM.Incidents table
    print("\n1. Getting starter queries for ICM.Incidents...")
    print("-" * 80)
    
    suggestions = suggest_queries_for_table("ICM", "Incidents")
    
    print(f"\nFound {len(suggestions)} suggestions:\n")
    
    for i, suggestion in enumerate(suggestions, 1):
        print(f"{i}. {suggestion['description']}")
        print(f"   Purpose: {suggestion['purpose']}")
        print(f"   Query:")
        for line in suggestion['query'].split('\n'):
            print(f"     {line}")
        print()

def test_exploration_guide():
    """Test exploration guide for a schema."""
    print("\n" + "=" * 80)
    print("Testing Schema Exploration Guide")
    print("=" * 80)
    
    print("\n2. Getting exploration guide for Common schema...")
    print("-" * 80)
    
    guide = get_exploration_path("Common")
    
    print(f"\nSchema: {guide['schema']}")
    print(f"Total tables: {guide['total_tables']}")
    print("\nExploration Steps:")
    
    for step in guide.get('exploration_steps', []):
        print(f"\n  Step {step['step']}: {step['action']}")
        print(f"    Why: {step['reason']}")
        if step.get('tables'):
            print(f"    Tables: {', '.join(step['tables'])}")

if __name__ == "__main__":
    try:
        test_query_suggestions()
        test_exploration_guide()
        
        print("\n" + "=" * 80)
        print("✅ All tests completed successfully!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
