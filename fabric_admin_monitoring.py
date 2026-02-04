"""
Fabric Admin Monitoring - Try admin-level APIs for query history.

Uses Fabric Admin APIs which may have broader access to monitoring data.
"""
import requests
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
WORKSPACE_ID = "3429e157-78cf-43af-82e7-18b47f1c6934"  # HMCClinic2026
LAKEHOUSE_ID = "64501f02-0063-4bbe-a2cd-4c132a32b289"  # Starbase_Lakehouse

def get_access_token():
    """Get Azure access token for Fabric APIs."""
    credential = DefaultAzureCredential()
    token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")
    return token.token

def get_capacity_id():
    """Get the capacity ID for the workspace."""
    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    response = requests.get(
        f"{FABRIC_API_BASE}/workspaces/{WORKSPACE_ID}",
        headers=headers
    )
    
    if response.status_code == 200:
        workspace = response.json()
        capacity_id = workspace.get("capacityId")
        print(f"Workspace: {workspace.get('displayName')}")
        print(f"Capacity ID: {capacity_id}")
        return capacity_id
    else:
        print(f"Error getting workspace: {response.status_code}")
        print(response.text)
        return None

def try_admin_apis():
    """Try various admin API endpoints."""
    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # List of admin API endpoints to try
    endpoints = [
        # Activity events (Power BI style)
        {
            "url": f"{FABRIC_API_BASE}/admin/activityevents",
            "params": {
                "startDateTime": (datetime.utcnow() - timedelta(hours=24)).isoformat() + "Z",
                "endDateTime": datetime.utcnow().isoformat() + "Z"
            }
        },
        # Workspace info
        {
            "url": f"{FABRIC_API_BASE}/admin/workspaces/{WORKSPACE_ID}/info",
            "params": {}
        },
        # Lakehouse as dataset
        {
            "url": f"{FABRIC_API_BASE}/admin/workspaces/{WORKSPACE_ID}/datasets/{LAKEHOUSE_ID}",
            "params": {}
        },
        # Refresh history
        {
            "url": f"{FABRIC_API_BASE}/admin/workspaces/{WORKSPACE_ID}/datasets/{LAKEHOUSE_ID}/refreshes",
            "params": {}
        },
    ]
    
    for endpoint in endpoints:
        print(f"\n{'=' * 60}")
        print(f"Trying: {endpoint['url']}")
        print('=' * 60)
        
        response = requests.get(
            endpoint['url'],
            headers=headers,
            params=endpoint['params']
        )
        
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("\n✓ SUCCESS! Response:")
            print("-" * 60)
            import json
            print(json.dumps(data, indent=2)[:1000])  # First 1000 chars
            if len(json.dumps(data)) > 1000:
                print(f"\n... (truncated, total length: {len(json.dumps(data))} chars)")
        else:
            print(f"✗ Error: {response.text[:300]}")

def try_sql_analytics_endpoint():
    """
    Try the SQL Analytics Endpoint monitoring.
    This might have query history via Fabric-specific DMVs.
    """
    print(f"\n{'=' * 60}")
    print("SQL Analytics Endpoint Monitoring")
    print('=' * 60)
    
    from fabric_rti_mcp.tools.query_history_tool import get_query_history
    
    # Try to get extended history from SQL endpoint
    print("\nQuerying sys.dm_exec_query_stats (SQL endpoint DMV)...")
    history = get_query_history(top_n=200)  # Request more rows
    
    if history:
        print(f"\nFound {len(history)} queries in cache")
        
        # Analyze time span
        if len(history) > 1:
            times = [h[2] for h in history]  # creation_time
            oldest = min(times)
            newest = max(times)
            span = newest - oldest
            print(f"Time span: {span}")
            print(f"Oldest: {oldest}")
            print(f"Newest: {newest}")
    
    # Try other DMVs that might have longer history
    print("\n\nTrying alternative DMVs...")
    from fabric_rti_mcp.tools.lakehouse_sql_tool import execute_sql_query
    
    dmv_queries = [
        ("sys.dm_exec_requests", "SELECT * FROM sys.dm_exec_requests"),
        ("sys.dm_exec_sessions", "SELECT * FROM sys.dm_exec_sessions"),
        ("sys.dm_resource_governor_workload_groups", "SELECT * FROM sys.dm_resource_governor_workload_groups"),
    ]
    
    for dmv_name, query in dmv_queries:
        try:
            print(f"\n  Trying {dmv_name}...")
            result = execute_sql_query(query)
            print(f"    ✓ Available - {len(result)} rows")
        except Exception as e:
            print(f"    ✗ Error: {str(e)[:100]}")

def main():
    print("=" * 60)
    print("Fabric Admin Monitoring API Explorer")
    print("=" * 60)
    
    try:
        # Get capacity ID
        print("\nStep 1: Getting workspace capacity...")
        print("-" * 60)
        capacity_id = get_capacity_id()
        
        # Try admin APIs
        print("\n\nStep 2: Trying admin APIs...")
        print("-" * 60)
        try_admin_apis()
        
        # Try SQL Analytics endpoint DMVs
        print("\n\nStep 3: Checking SQL Analytics endpoint DMVs...")
        print("-" * 60)
        try_sql_analytics_endpoint()
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
