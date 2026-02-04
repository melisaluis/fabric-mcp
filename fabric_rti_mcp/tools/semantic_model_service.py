"""
Fabric Semantic Model service - queries Power BI/Fabric semantic layer for relationships.

Uses Fabric REST APIs and XMLA endpoint to access semantic model metadata
that's not available through SQL Analytics endpoint.
"""
import os
from typing import List, Dict, Any, Optional, Tuple
import requests
from pyadomd import Pyadomd
from azure.identity import DefaultAzureCredential


def _get_fabric_token() -> str:
    """Get access token for Fabric APIs."""
    credential = DefaultAzureCredential()
    token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")
    return token.token


def find_semantic_model_for_lakehouse(workspace_id: str, lakehouse_name: str) -> Optional[str]:
    """
    Find the semantic model (dataset) associated with a lakehouse.
    
    Args:
        workspace_id: Fabric workspace ID
        lakehouse_name: Name of the lakehouse
    
    Returns:
        Semantic model ID if found, None otherwise
    """
    token = _get_fabric_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Get all datasets in workspace
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Failed to get datasets: {response.status_code} - {response.text}")
    
    datasets = response.json().get("value", [])
    
    # Look for dataset matching lakehouse name
    # Lakehouses typically create a default semantic model with the same name
    for dataset in datasets:
        if lakehouse_name.lower() in dataset.get("name", "").lower():
            return dataset["id"]
    
    return None


def get_semantic_model_relationships(workspace_id: str, dataset_id: str) -> List[Dict[str, Any]]:
    """
    Get relationships from a semantic model using XMLA/TOM (Tabular Object Model).
    
    Args:
        workspace_id: Fabric workspace ID
        dataset_id: Semantic model/dataset ID
    
    Returns:
        List of relationship definitions
    """
    token = _get_fabric_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # XMLA query
    xmla_query = {
        "queries": [
            {
                "query": "EVALUATE TMSCHEMA_RELATIONSHIPS"
            }
        ]
    }
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"

    relationships = []
    try:
        response = requests.post(url, headers=headers, json=xmla_query)
        print(f"[DEBUG] XMLA response status: {response.status_code}")
        results = response.json()
        print(f"[DEBUG] XMLA response: {results}")
        if "results" in results and len(results["results"]) > 0:
            rows = results["results"][0].get("tables", [{}])[0].get("rows", [])
            for row in rows:
                relationships.append({
                    "name": row.get("Name", ""),
                    "from_table": row.get("FromTable", ""),
                    "from_column": row.get("FromColumn", ""),
                    "to_table": row.get("ToTable", ""),
                    "to_column": row.get("ToColumn", ""),
                        "cross_filtering": row.get("CrossFilterDirection", ""),
                    "is_active": row.get("IsActive", True)
                })
    except Exception as e:
        print(f"Error querying relationships (XMLA): {e}")

    # Always also try DMV query and merge results
    try:
        dmv_rels = _get_relationships_via_dmv(workspace_id, dataset_id, token)
        print(f"[DEBUG] DMV relationships found: {len(dmv_rels)}")
        # Merge, avoiding duplicates
        seen = set((r["name"], r["from_table"], r["from_column"], r["to_table"], r["to_column"]) for r in relationships)
        for rel in dmv_rels:
            key = (rel["name"], rel["from_table"], rel["from_column"], rel["to_table"], rel["to_column"])
            if key not in seen:
                relationships.append(rel)
    except Exception as e:
        print(f"Error querying relationships (DMV): {e}")

    print(f"[DEBUG] Total relationships returned: {len(relationships)}")
    return relationships


def _get_relationships_via_dmv(workspace_id: str, dataset_id: str, token: str) -> List[Dict[str, Any]]:
    """
    Alternative method: Query relationships using DMV (Dynamic Management Views).
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # DAX query to get relationships from $SYSTEM.TMSCHEMA_RELATIONSHIPS
    dax_query = """
    {
      "queries": [
        {
          "query": "EVALUATE $SYSTEM.TMSCHEMA_RELATIONSHIPS"
        }
      ]
    }
    """
    
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
    
    try:
        response = requests.post(url, headers=headers, data=dax_query)
        
        if response.status_code != 200:
            return []
        
        results = response.json()
        relationships = []
        
        if "results" in results and len(results["results"]) > 0:
            rows = results["results"][0].get("tables", [{}])[0].get("rows", [])
            for row in rows:
                relationships.append({
                    "name": row.get("Name", ""),
                    "from_table": row.get("FromTable", ""),
                    "from_column": row.get("FromColumn", ""),
                    "to_table": row.get("ToTable", ""),
                    "to_column": row.get("ToColumn", ""),
                    "cross_filtering": row.get("CrossFilterDirection", ""),
                    "is_active": row.get("IsActive", True)
                })
        
        return relationships
        
    except Exception as e:
        print(f"DMV query failed: {e}")
        return []


def get_lakehouse_semantic_relationships() -> List[Tuple[str, str, str, str, str, str]]:
    """
    Get semantic model relationships for the configured lakehouse.
    
    Reads FABRIC_WORKSPACE_ID and FABRIC_LAKEHOUSE_NAME from environment,
    finds the associated semantic model, and returns its relationships.
    
    Returns:
        List of tuples: (from_table, from_column, to_table, to_column, relationship_name, cross_filtering)
    """
    workspace_link = os.getenv("FABRIC_API_BASE")
    dataset_name = "Starbase General Model"

    if not workspace_link:
        raise ValueError("FABRIC_API_BASE environment variable not set")

    # Accept either workspace name or full link
    if workspace_link.startswith("powerbi://"):
        conn_str = f"Provider=MSOLAP;Data Source={workspace_link};Initial Catalog={dataset_name};"
    else:
        conn_str = f"Provider=MSOLAP;Data Source=powerbi://api.powerbi.com/v1.0/myorg/{workspace_link};Initial Catalog={dataset_name};"

    rel_query = "SELECT * FROM $SYSTEM.TMSCHEMA_RELATIONSHIPS"
    table_query = "SELECT * FROM $SYSTEM.TMSCHEMA_TABLES"
    col_query = "SELECT * FROM $SYSTEM.TMSCHEMA_COLUMNS"
    results = []
    try:
        with Pyadomd(conn_str) as conn:
            # Fetch relationships
            with conn.cursor() as cur:
                cur.execute(rel_query)
                rel_cols = [col[0] for col in cur.description]
                rel_rows = [dict(zip(rel_cols, row)) for row in cur.fetchall()]

            # Fetch tables
            with conn.cursor() as cur:
                cur.execute(table_query)
                table_cols = [col[0] for col in cur.description]
                table_rows = [dict(zip(table_cols, row)) for row in cur.fetchall()]
                print("[DEBUG] Table keys:", table_rows[0].keys() if table_rows else "No rows")
                table_id_to_name = {t.get("ID", t.get("Id")): t.get("Name", t.get("name", "")) for t in table_rows}

            # Fetch columns
            with conn.cursor() as cur:
                cur.execute(col_query)
                col_cols = [col[0] for col in cur.description]
                col_rows = [dict(zip(col_cols, row)) for row in cur.fetchall()]
                print("[DEBUG] Column keys:", col_rows[0].keys() if col_rows else "No rows")
                col_id_to_name = {c.get("ID", c.get("Id")): c.get("Name", c.get("name", "")) for c in col_rows}

            # Build readable relationships
            print("[DEBUG] Relationship keys:", rel_rows[0].keys() if rel_rows else "No rows")
            for rel in rel_rows:
                from_table = table_id_to_name.get(rel.get("FromTableID", rel.get("FromTableId")), "")
                from_col = col_id_to_name.get(rel.get("FromColumnID", rel.get("FromColumnId")), "")
                to_table = table_id_to_name.get(rel.get("ToTableID", rel.get("ToTableId")), "")
                to_col = col_id_to_name.get(rel.get("ToColumnID", rel.get("ToColumnId")), "")
                name = rel.get("Name", rel.get("name", ""))
                cross_filter = rel.get("CrossFilteringBehavior", "")
                results.append((from_table, from_col, to_table, to_col, name, cross_filter))
    except Exception as e:
        print(f"Error querying semantic model relationships via pyadomd: {e}")
        return []

    return results


def get_semantic_model_info() -> Dict[str, Any]:
    """
    Get information about the semantic model associated with the lakehouse.
    
    Returns:
        Dictionary with semantic model details including relationships, tables, and measures
    """
    workspace_id = os.getenv("FABRIC_WORKSPACE_ID")
    lakehouse_name = os.getenv("FABRIC_LAKEHOUSE_NAME")
    
    if not workspace_id or not lakehouse_name:
        raise ValueError("FABRIC_WORKSPACE_ID and FABRIC_LAKEHOUSE_NAME must be set")
    
    dataset_id = find_semantic_model_for_lakehouse(workspace_id, lakehouse_name)
    
    if not dataset_id:
        return {
            "found": False,
            "message": f"No semantic model found for lakehouse '{lakehouse_name}'"
        }
    
    relationships = get_semantic_model_relationships(workspace_id, dataset_id)
    
    return {
        "found": True,
        "workspace_id": workspace_id,
        "dataset_id": dataset_id,
        "lakehouse_name": lakehouse_name,
        "relationship_count": len(relationships),
        "relationships": relationships
    }
