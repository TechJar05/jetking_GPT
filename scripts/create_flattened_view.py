# scripts/create_flattened_view.py
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

# Step 1: Get dynamic columns
cur.execute("""
SELECT DISTINCT KEY AS column_name
FROM raw_excel_data,
LATERAL FLATTEN(input => data)
ORDER BY column_name;
""")
columns = [row[0] for row in cur.fetchall()]

# Step 2: Generate select clause
select_clause = ",\n".join([f'data:"{col}"::string AS "{col}"' for col in columns])
view_sql = f"""
CREATE OR REPLACE VIEW flattened_students AS
SELECT
    id,
    file_name,
    uploaded_at,
    {select_clause}
FROM raw_excel_data;
"""

cur.execute(view_sql)
print("✅ Flattened view 'flattened_students' created successfully!")

cur.close()
conn.close()
