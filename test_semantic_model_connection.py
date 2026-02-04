"""
Test script to verify semantic model connection using MCP tool.
"""
import os
from dotenv import load_dotenv
load_dotenv()
from fabric_rti_mcp.tools.lakehouse_sql_tool import test_semantic_model_connection

if __name__ == "__main__":
    try:
        result = test_semantic_model_connection()
        print("✅ Semantic model connection successful:", result)
    except Exception as e:
        print("❌ Semantic model connection failed:", str(e))
