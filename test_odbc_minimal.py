import os
import logging
import pyodbc
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
import traceback

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def get_access_token():
    try:
        credential = DefaultAzureCredential()
        token = credential.get_token("https://database.windows.net/.default")
        logging.info(f"Access token length: {len(token.token)}")
        logging.info(f"Access token startswith: {token.token[:10]}")
        logging.info(f"Access token first 100 chars: {token.token[:100]}")
        return token.token
    except Exception as e:
        logging.error(f"Failed to acquire Azure AD token: {e}")
        return None

def try_odbc_connection(driver_name, sql_endpoint, db_name, access_token):
    conn_str = (
        f"Driver={{{driver_name}}};"
        f"Server={sql_endpoint};"
        f"Database={db_name};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout=30;"
    )
    conn_str_interactive = (
        f"Driver={{{driver_name}}};"
        f"Server={sql_endpoint};"
        f"Database={db_name};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout=30;"
        f"Authentication=ActiveDirectoryInteractive;"
    )
    logging.info(f"Trying ODBC connection with driver: {driver_name}")
    logging.info(f"Connection string: {conn_str}")
    # Try standard token connection
    try:
        conn = pyodbc.connect(
            conn_str,
            attrs_before={
                1256: access_token  # SQL_COPT_SS_ACCESS_TOKEN
            }
        )
        logging.info("ODBC connection established successfully.")
        cursor = conn.cursor()
        cursor.execute("SELECT TOP 1 * FROM INFORMATION_SCHEMA.TABLES")
        row = cursor.fetchone()
        logging.info(f"Sample row: {row}")
        conn.close()
        return True
    except pyodbc.Error as db_err:
        logging.error(f"pyodbc.Error: {db_err}")
        logging.error(traceback.format_exc())
        if hasattr(db_err, 'args'):
            for idx, arg in enumerate(db_err.args):
                logging.error(f"ODBC error arg[{idx}]: {arg}")
        try:
            diag = pyodbc.drivers()
            logging.error(f"Available ODBC drivers: {diag}")
        except Exception as diag_err:
            logging.error(f"Error getting ODBC drivers: {diag_err}")
        logging.info("Trying ODBC connection with Authentication=ActiveDirectoryInteractive...")
        logging.info(f"Connection string: {conn_str_interactive}")
        try:
            conn = pyodbc.connect(
                conn_str_interactive
            )
            logging.info("ODBC connection established successfully (Interactive).")
            cursor = conn.cursor()
            cursor.execute("SELECT TOP 1 * FROM INFORMATION_SCHEMA.TABLES")
            row = cursor.fetchone()
            logging.info(f"Sample row: {row}")
            conn.close()
            return True
        except Exception as e2:
            logging.error(f"Interactive connection failed: {e2}")
            logging.error(traceback.format_exc())
        return False
    except Exception as e:
        logging.error(f"General Exception: {e}")
        logging.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    SQL_ENDPOINT = os.getenv("FABRIC_SQL_ENDPOINT")
    LAKEHOUSE_NAME = os.getenv("FABRIC_LAKEHOUSE_NAME")

    logging.info(f"SQL_ENDPOINT={SQL_ENDPOINT}")
    logging.info(f"LAKEHOUSE_NAME={LAKEHOUSE_NAME}")

    if not SQL_ENDPOINT or not LAKEHOUSE_NAME:
        logging.error("Missing FABRIC_SQL_ENDPOINT or FABRIC_LAKEHOUSE_NAME environment variable.")
        exit(1)

    logging.info("Acquiring Azure AD access token...")
    access_token = get_access_token()
    if not access_token:
        logging.error("Could not acquire access token. Exiting.")
        exit(1)

    logging.info("Attempting ODBC connection with Driver 18...")
    success = try_odbc_connection("ODBC Driver 18 for SQL Server", SQL_ENDPOINT, LAKEHOUSE_NAME, access_token)
    if not success:
        logging.info("ODBC Driver 18 failed. Attempting ODBC connection with Driver 17...")
        try_odbc_connection("ODBC Driver 17 for SQL Server", SQL_ENDPOINT, LAKEHOUSE_NAME, access_token)
    logging.info("ODBC connection test script completed.")
