import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization

with open(os.path.expanduser("~/.ssh/snowflake/rsa_key.p8"), "rb") as f:
    cle = serialization.load_pem_private_key(f.read(), password=None)

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user="AIRFLOW_SVC",
    private_key=cle,
    role="TRANSFORMER",
    warehouse="NYC_TAXI_WH",
)
print(conn.cursor().execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()").fetchone())