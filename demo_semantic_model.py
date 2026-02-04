from dotenv import load_dotenv
load_dotenv()
from fabric_rti_mcp.tools.semantic_model_mcp_tools import get_semantic_relationships, get_semantic_model_details

if __name__ == "__main__":
    print("--- Semantic Relationships ---")
    print(get_semantic_relationships())
    print("\n--- Semantic Model Details ---")
    print(get_semantic_model_details())
