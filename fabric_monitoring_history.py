"""
Fabric Monitoring Query History - Uses Fabric REST APIs to get extended query history.

This leverages Fabric's built-in monitoring which has 30+ days retention.
Requires admin permissions.
"""
import os
import requests
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential

# Configuration
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
WORKSPACE_ID = None  # We'll need to find this
LAKEHOUSE_ID = None  # We'll need to find this

def get_access_token():
    """Get Azure access token for Fabric APIs."""
    credential = DefaultAzureCredential()
    token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")
    return token.token

def list_workspaces():
    """List all Fabric workspaces to find the one containing Starbase."""
    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    response = requests.get(
        f"{FABRIC_API_BASE}/workspaces",
        headers=headers
    )
    
    if response.status_code == 200:
        workspaces = response.json().get("value", [])
        print(f"Found {len(workspaces)} workspaces:\n")
        for ws in workspaces:
            print(f"  - {ws['displayName']} (ID: {ws['id']})")
        return workspaces
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return []

def find_lakehouse_in_workspace(workspace_id):
    """Find Starbase lakehouse in a workspace."""
    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    response = requests.get(
        f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items?type=Lakehouse",
        headers=headers
    )
    
    if response.status_code == 200:
        items = response.json().get("value", [])
        for item in items:
            if "Starbase" in item.get("displayName", ""):
                print(f"\nFound Starbase:")
                print(f"  Display Name: {item['displayName']}")
                print(f"  Lakehouse ID: {item['id']}")
                print(f"  Type: {item['type']}")
                return item
        print("Starbase lakehouse not found in this workspace")
    else:
        print(f"Error: {response.status_code} - {response.text}")
    
    return None

def get_capacity_metrics(days_back=7):
    """
    Get capacity metrics including query activity.
    This uses the Fabric Capacity Metrics API.
    """
    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Calculate time range
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days_back)
    
    # Try the monitoring/metrics endpoint
    url = f"{FABRIC_API_BASE}/capacities/metrics"
    params = {
        "startTime": start_time.isoformat() + "Z",
        "endTime": end_time.isoformat() + "Z"
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    print(f"\nAttempting to get capacity metrics...")
    print(f"URL: {url}")
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Response: {response.text}")
        return None

def get_lakehouse_query_insights(workspace_id, lakehouse_id, days_back=7):
    """
    Try to get query insights for the lakehouse.
    Different API endpoint that might have query history.
    """
    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Try different possible endpoints
    endpoints = [
        f"{FABRIC_API_BASE}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/queryInsights",
        f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{lakehouse_id}/monitoring",
        f"{FABRIC_API_BASE}/admin/workspaces/{workspace_id}/datasets/{lakehouse_id}/queryLog",
    ]
    
    for endpoint in endpoints:
        print(f"\nTrying: {endpoint}")
        response = requests.get(endpoint, headers=headers)
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            print("Success! Data found:")
            data = response.json()
            print(data)
            return data
        elif response.status_code != 404:
            print(f"Response: {response.text[:200]}")
    
    return None

def main():
    print("=" * 60)
    print("Fabric Monitoring Query History")
    print("=" * 60)
    
    try:
        # Step 1: List workspaces
        print("\nStep 1: Finding workspaces...")
        print("-" * 60)
        workspaces = list_workspaces()
        
        if not workspaces:
            print("\nNo workspaces found or access denied.")
            return
        
        # Step 2: Search for Starbase in workspaces
        print("\n\nStep 2: Searching for Starbase lakehouse...")
        print("-" * 60)
        
        starbase_info = None
        starbase_workspace_id = None
        
        for ws in workspaces:
            lakehouse = find_lakehouse_in_workspace(ws['id'])
            if lakehouse:
                starbase_info = lakehouse
                starbase_workspace_id = ws['id']
                break
        
        if not starbase_info:
            print("\nStarbase lakehouse not found in any workspace.")
            print("Please verify the lakehouse name or your permissions.")
            return
        
        # Step 3: Try to get query insights
        print("\n\nStep 3: Attempting to get query insights...")
        print("-" * 60)
        
        insights = get_lakehouse_query_insights(
            starbase_workspace_id,
            starbase_info['id'],
            days_back=7
        )
        
        # Step 4: Try capacity metrics
        print("\n\nStep 4: Attempting to get capacity metrics...")
        print("-" * 60)
        metrics = get_capacity_metrics(days_back=7)
        
        if not insights and not metrics:
            print("\n\nNo query history APIs returned data.")
            print("\nPossible reasons:")
            print("  1. Query insights not enabled on this lakehouse")
            print("  2. Different API authentication required")
            print("  3. Feature not available in your Fabric tier")
            print("\nRecommendation: Use Option 1 (Background Logger) instead.")
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
