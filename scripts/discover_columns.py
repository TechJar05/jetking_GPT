# scripts/discover_columns.py
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
cur = conn.cursor()

# Discover all JSON keys dynamically
query = """
SELECT DISTINCT KEY AS column_name
FROM raw_excel_data,
LATERAL FLATTEN(input => data)
ORDER BY column_name;
"""

cur.execute(query)
columns = [row[0] for row in cur.fetchall()]

print("✅ Discovered columns in Excel data:")
for col in columns:
    print("-", col)

cur.close()
conn.close()
