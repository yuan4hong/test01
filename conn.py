from databricks import sql
import os

connection = sql.connect(
                        server_hostname = "nvidia-edsp-or1.cloud.databricks.com",
                        http_path = "/sql/1.0/warehouses/fee15d0c1610eca9",
                        access_token = os.environ.get("DATABRICKS_TOKEN"))

cursor = connection.cursor()

cursor.execute("SELECT * FROM `edsp-dcartm`.schema1.tab01")
for row in cursor.fetchall():
    print(row)

cursor.close()
connection.close()