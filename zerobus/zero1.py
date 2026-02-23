# Install SDK - https://github.com/databricks/zerobus-sdk-py
# %pip install databricks-zerobus-ingest-sdk

# Databricks Workspace Information

# Specify your AWS region manually (spark.databricks.region not available in serverless)
# Supported regions: us-east-1, us-east-2, us-west-2, eu-central-1, ap-southeast-1, ap-southeast-2, ap-northeast-1, ca-central-1, eu-west-1
DATABRICKS_REGION = os.environ.get("DATABRICKS_REGION", "us-west-2")

DATABRICKS_WORKSPACE_ID = os.environ.get("DATABRICKS_WORKSPACE_ID", "4453572645853920")
DATABRICKS_WORKSPACE_URL = os.environ.get("DATABRICKS_WORKSPACE_URL", "")

ZEROBUS_INGEST_URL = os.environ.get("ZEROBUS_SERVER_ENDPOINT",
    f"https://{DATABRICKS_WORKSPACE_ID}.zerobus.{DATABRICKS_REGION}.cloud.databricks.com")

CLIENT_ID = os.environ.get("DATABRICKS_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("DATABRICKS_CLIENT_SECRET", "")

CATALOG = os.environ.get("DATABRICKS_CATALOG", "edsp-dcartm")
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "default")
TABLE = "zerobus_ingest_test"

# Init SDK
import os
import json
import logging
from databricks import sql as dbsql
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
DATABRICKS_HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", "")

if not DATABRICKS_TOKEN:
    print("ERROR: DATABRICKS_TOKEN not set. Run:")
    print("  export DATABRICKS_TOKEN=dapi...")
    exit(1)

print(f"Connecting to Databricks SQL: {DATABRICKS_HTTP_PATH}")
db_conn = dbsql.connect(
    server_hostname="nvidia-edsp-or1.cloud.databricks.com",
    http_path=DATABRICKS_HTTP_PATH,
    access_token=DATABRICKS_TOKEN,
)
print(f"Connected to Databricks SQL: {DATABRICKS_HTTP_PATH}")
cursor = db_conn.cursor()

# Create table
print(f"Creating table {CATALOG}.{SCHEMA}.{TABLE}")
cursor.execute(f"CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{TABLE}` (id INT, device STRING, payload STRING)")

# Grant permissions to the service principal
print(f"Granting permissions to {CLIENT_ID}")
cursor.execute(f"GRANT USE CATALOG ON CATALOG `{CATALOG}` TO `{CLIENT_ID}`")
cursor.execute(f"GRANT USE SCHEMA ON SCHEMA `{CATALOG}`.`{SCHEMA}` TO `{CLIENT_ID}`")
cursor.execute(f"GRANT MODIFY, SELECT ON TABLE `{CATALOG}`.`{SCHEMA}`.`{TABLE}` TO `{CLIENT_ID}`")
print("Table created and permissions granted.")

# Configure logging (optional but recommended)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Configuration
server_endpoint = ZEROBUS_INGEST_URL
workspace_url = DATABRICKS_WORKSPACE_URL

table_name = f"{CATALOG}.{SCHEMA}.{TABLE}"
client_id = CLIENT_ID
client_secret = CLIENT_SECRET

# Initialize SDK
sdk = ZerobusSdk(server_endpoint, unity_catalog_url=workspace_url)

# Configure table properties
table_properties = TableProperties(table_name)

# Configure stream with JSON record type
options = StreamConfigurationOptions(record_type=RecordType.JSON)

# Create stream and start writing data

stream = sdk.create_stream(client_id, client_secret, table_properties, options)

try:
    # Ingest records
    for i in range(100):
        # Create JSON record
        record_dict = {
            "id": i,
            "device": f"sensor-{i % 10}",
            "payload": json.dumps({"temp": 20 + (i % 15), "humidity": 50 + (i % 40)}),
        }

        stream.ingest_record(record_dict)

        print(f"Ingested record {i + 1}")
    stream.flush()
    print("Successfully ingested 100 records!")
finally:
    stream.close()

# Display results
print("\nSync results:")
cursor.execute(f"SELECT id, device, payload FROM `{CATALOG}`.`{SCHEMA}`.`{TABLE}` LIMIT 10")
for row in cursor.fetchall():
    print(f"  {row}")

# AsyncIO Client Implementation
import asyncio
from zerobus.sdk.aio import ZerobusSdk as ZerobusSdkAsync

async def main():
    # Configuration
    server_endpoint = ZEROBUS_INGEST_URL
    workspace_url = DATABRICKS_WORKSPACE_URL

    table_name = f"{CATALOG}.{SCHEMA}.{TABLE}"
    client_id = CLIENT_ID
    client_secret = CLIENT_SECRET

    # Initialize SDK
    sdk = ZerobusSdkAsync(server_endpoint, unity_catalog_url=workspace_url)

    # Configure table properties
    table_properties = TableProperties(table_name)

    # Configure stream with JSON record type
    options = StreamConfigurationOptions(record_type=RecordType.JSON)

    # Create stream
    stream = await sdk.create_stream(client_id, client_secret, table_properties, options)

    try:
        # Ingest records
        for i in range(100_000):
            # Create JSON record
            record_dict = {
                "id": i,
                "device": f"sensor-async-{i % 10}",
                "payload": json.dumps({"temp": 20 + (i % 15), "humidity": 50 + (i % 40)}),
            }

            future = stream.ingest_record(record_dict)
            await future  # Optional: Wait for durability confirmation

            if i % 100 == 0:
              print(f"Ingested record {i + 1}")

        print("Successfully ingested 100_000 records!")
    finally:
        await stream.close()

asyncio.run(main())

# Display async results
print("\nAsync results:")
cursor.execute(f"SELECT id, device, payload FROM `{CATALOG}`.`{SCHEMA}`.`{TABLE}` WHERE device LIKE 'sensor-async-%' LIMIT 10")
for row in cursor.fetchall():
    print(f"  {row}")

cursor.close()
db_conn.close()