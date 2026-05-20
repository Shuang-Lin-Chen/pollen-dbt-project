from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import snowflake.connector
import os
import json

load_dotenv()

connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")

snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
snowflake_user = os.getenv("SNOWFLAKE_USER")
snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
snowflake_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
snowflake_database = os.getenv("SNOWFLAKE_DATABASE")
snowflake_schema = os.getenv("SNOWFLAKE_SCHEMA")
snowflake_role = os.getenv("SNOWFLAKE_ROLE")

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

blob_list = list(container_client.list_blobs())

if not blob_list:
    print("No blobs found.")
    exit()

latest_blob = max(blob_list, key=lambda x: x.last_modified)
print(f"Latest blob: {latest_blob.name}")

blob_client = container_client.get_blob_client(latest_blob.name)
blob_data = blob_client.download_blob().readall()

json_data = json.loads(blob_data)

conn = snowflake.connector.connect(
    account=snowflake_account,
    user=snowflake_user,
    password=snowflake_password,
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    schema=snowflake_schema,
    role=snowflake_role,
)

cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS RAW_POLLEN (
    RAW_JSON VARIANT,
    LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
""")

cursor.execute(
    """
    INSERT INTO RAW_POLLEN (RAW_JSON)
    SELECT PARSE_JSON(%s)
    """,
    (json.dumps(json_data),)
)

conn.commit()

print("JSON loaded into Snowflake RAW.RAW_POLLEN successfully.")

cursor.close()
conn.close()