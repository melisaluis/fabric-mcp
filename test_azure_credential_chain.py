import logging
from azure.identity import DefaultAzureCredential, AzureCliCredential, EnvironmentCredential, ManagedIdentityCredential, ChainedTokenCredential

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

RESOURCE = "https://database.windows.net/.default"

logging.info("Testing Azure credential chain for Fabric Lakehouse authentication...")

# Try AzureCliCredential
try:
    cli_cred = AzureCliCredential()
    cli_token = cli_cred.get_token(RESOURCE)
    logging.info(f"AzureCliCredential: SUCCESS, token startswith={cli_token.token[:10]}")
except Exception as e:
    logging.error(f"AzureCliCredential: FAILED, {e}")

# Try EnvironmentCredential
try:
    env_cred = EnvironmentCredential()
    env_token = env_cred.get_token(RESOURCE)
    logging.info(f"EnvironmentCredential: SUCCESS, token startswith={env_token.token[:10]}")
except Exception as e:
    logging.error(f"EnvironmentCredential: FAILED, {e}")

# Try ManagedIdentityCredential
try:
    mi_cred = ManagedIdentityCredential()
    mi_token = mi_cred.get_token(RESOURCE)
    logging.info(f"ManagedIdentityCredential: SUCCESS, token startswith={mi_token.token[:10]}")
except Exception as e:
    logging.error(f"ManagedIdentityCredential: FAILED, {e}")

# Try DefaultAzureCredential
try:
    default_cred = DefaultAzureCredential()
    default_token = default_cred.get_token(RESOURCE)
    logging.info(f"DefaultAzureCredential: SUCCESS, token startswith={default_token.token[:10]}")
except Exception as e:
    logging.error(f"DefaultAzureCredential: FAILED, {e}")

logging.info("Credential test complete.")
