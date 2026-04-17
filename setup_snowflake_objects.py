"""Create Snowflake objects for Cost Optimization Framework."""
import snowflake.connector

TOKEN = open("Connect-token-secret.txt").read().strip()

conn = snowflake.connector.connect(
    account="chc70950.us-east-1",
    user="SRINIVAS",
    role="ACCOUNTADMIN",
    authenticator="programmatic_access_token",
    token=TOKEN,
)

statements = [
    "CREATE DATABASE IF NOT EXISTS COST_OPTIMIZATION_DB",
    "CREATE SCHEMA IF NOT EXISTS COST_OPTIMIZATION_DB.STAGING",
    "CREATE SCHEMA IF NOT EXISTS COST_OPTIMIZATION_DB.INTERMEDIATE",
    "CREATE SCHEMA IF NOT EXISTS COST_OPTIMIZATION_DB.PUBLICATION",
    "CREATE SCHEMA IF NOT EXISTS COST_OPTIMIZATION_DB.SEEDS",
    """CREATE WAREHOUSE IF NOT EXISTS COST_OPT_WH
       WITH WAREHOUSE_SIZE='XSMALL'
       AUTO_SUSPEND=60
       AUTO_RESUME=TRUE
       INITIALLY_SUSPENDED=TRUE""",
    "GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE ACCOUNTADMIN",
]

cur = conn.cursor()
for sql in statements:
    print(f"Running: {sql[:60]}...")
    cur.execute(sql)
    print(f"  -> {cur.fetchone()[0]}")

cur.close()
conn.close()
print("\nAll Snowflake objects created successfully.")
