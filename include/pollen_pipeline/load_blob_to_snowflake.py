from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import snowflake.connector
import os
import json


# ============================================================
# 1. Load environment variables
# ============================================================
load_dotenv()

azure_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
azure_container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")

snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
snowflake_user = os.getenv("SNOWFLAKE_USER")
snowflake_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
snowflake_database = os.getenv("SNOWFLAKE_DATABASE")
snowflake_schema = os.getenv("SNOWFLAKE_SCHEMA")
snowflake_role = os.getenv("SNOWFLAKE_ROLE")

snowflake_private_key = os.getenv("SNOWFLAKE_PRIVATE_KEY")
snowflake_private_key_passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")


# ============================================================
# 2. Validate required environment variables
# ============================================================
required_env_vars = {
    "AZURE_STORAGE_CONNECTION_STRING": azure_connection_string,
    "AZURE_STORAGE_CONTAINER_NAME": azure_container_name,
    "SNOWFLAKE_ACCOUNT": snowflake_account,
    "SNOWFLAKE_USER": snowflake_user,
    "SNOWFLAKE_WAREHOUSE": snowflake_warehouse,
    "SNOWFLAKE_DATABASE": snowflake_database,
    "SNOWFLAKE_SCHEMA": snowflake_schema,
    "SNOWFLAKE_ROLE": snowflake_role,
    "SNOWFLAKE_PRIVATE_KEY": snowflake_private_key,
}

missing_vars = [
    key for key, value in required_env_vars.items()
    if not value
]

if missing_vars:
    raise ValueError(f"Missing environment variables: {missing_vars}")


# ============================================================
# 3. Connect to Azure Blob Storage
# ============================================================
blob_service_client = BlobServiceClient.from_connection_string(
    azure_connection_string
)

container_client = blob_service_client.get_container_client(
    azure_container_name
)

blob_list = list(container_client.list_blobs())

if not blob_list:
    print("No blobs found.")
    raise SystemExit(0)


# ============================================================
# 4. Read latest JSON blob
# ============================================================
json_blobs = [
    blob for blob in blob_list
    if blob.name.endswith(".json")
]

if not json_blobs:
    print("No JSON blobs found.")
    raise SystemExit(0)

latest_blob = max(json_blobs, key=lambda blob: blob.last_modified)

print(f"Latest blob: {latest_blob.name}")

blob_client = container_client.get_blob_client(latest_blob.name)
blob_data = blob_client.download_blob().readall()

json_data = json.loads(blob_data)


# ============================================================
# 5. Load Snowflake private key from environment variable
# ============================================================
# Astronomer often stores private key as one line with literal "\n".
# This converts "\n" back into real line breaks.
snowflake_private_key = snowflake_private_key.replace("\\n", "\n").strip()

private_key_password = (
    snowflake_private_key_passphrase.encode("utf-8")
    if snowflake_private_key_passphrase
    else None
)

try:
    private_key = serialization.load_pem_private_key(
        snowflake_private_key.encode("utf-8"),
        password=private_key_password,
        backend=default_backend(),
    )
except TypeError as e:
    raise ValueError(
        "Private key loading failed. The key is probably encrypted, "
        "but SNOWFLAKE_PRIVATE_KEY_PASSPHRASE is missing or wrong."
    ) from e
except ValueError as e:
    raise ValueError(
        "Private key loading failed. The private key format may be broken, "
        "or the passphrase is incorrect."
    ) from e


# Convert private key to DER PKCS8 bytes for Snowflake connector
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)


# ============================================================
# 6. Connect to Snowflake using key-pair authentication
# ============================================================
print("SNOWFLAKE_ACCOUNT:", snowflake_account)
print("SNOWFLAKE_USER:", snowflake_user)
print("SNOWFLAKE_ROLE:", snowflake_role)
print("SNOWFLAKE_WAREHOUSE:", snowflake_warehouse)
print("SNOWFLAKE_DATABASE:", snowflake_database)
print("SNOWFLAKE_SCHEMA:", snowflake_schema)

snowflake_connection = snowflake.connector.connect(
    account=snowflake_account,
    user=snowflake_user,
    private_key=private_key_bytes,
    role=snowflake_role,
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    schema=snowflake_schema,
)

cursor = snowflake_connection.cursor()

print("Connected to Snowflake successfully using key-pair authentication.")


# ============================================================
# 7. Create RAW table if it does not exist
# ============================================================
cursor.execute("""
CREATE TABLE IF NOT EXISTS RAW_POLLEN (
    RAW_JSON VARIANT,
    LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
""")


# ============================================================
# 8. Insert JSON into Snowflake
# ============================================================
cursor.execute(
    """
    INSERT INTO RAW_POLLEN (RAW_JSON)
    SELECT PARSE_JSON(%s)
    """,
    (json.dumps(json_data),),
)

snowflake_connection.commit()

print("JSON loaded into Snowflake RAW.RAW_POLLEN successfully.")


# ============================================================
# 9. Close connection
# ============================================================
cursor.close()
snowflake_connection.close()

print("Snowflake connection closed.")