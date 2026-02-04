from dotenv import load_dotenv
load_dotenv()

from fabric_rti_mcp.tools.lakehouse_sql_tool import lakehouse_list_tables_safe

if __name__ == "__main__":
    try:
        results = lakehouse_list_tables_safe()
        print("Results from lakehouse_list_tables_safe:")
        for row in results:
            print(row)
    except Exception as e:
        print(f"Exception: {e}")
