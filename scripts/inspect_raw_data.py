import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

# Don't use quote_plus for direct connector - only needed for SQLAlchemy URIs
user = os.getenv('SNOWFLAKE_USER')
password = os.getenv('SNOWFLAKE_PASSWORD')
account = os.getenv('SNOWFLAKE_ACCOUNT')
database = os.getenv('SNOWFLAKE_DATABASE')
schema = os.getenv('SNOWFLAKE_SCHEMA')
warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
role = os.getenv('SNOWFLAKE_ROLE')

print("🔍 Using configuration:")
print(f"   Account: {account}")
print(f"   Database: {database}")
print(f"   Schema: {schema}")
print(f"   Warehouse: {warehouse}")
print(f"   Role: {role}")
print(f"   User: {user}")

SNOWFLAKE_CONFIG = {
    "user": user,
    "password": password,
    "account": account,
    "warehouse": warehouse,
    "database": database,
    "schema": schema,
    "role": role
}

print("\n🔗 Connecting to Snowflake...")
conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
cur = conn.cursor()

print("✅ Connected successfully!\n")

# Set context explicitly
cur.execute(f"USE DATABASE {database}")
cur.execute(f"USE SCHEMA {schema}")
cur.execute(f"USE WAREHOUSE {warehouse}")
print(f"✅ Context set to {database}.{schema}\n")

# Check source table
print("🔍 Checking RAW_EXCEL_DATA...")
cur.execute("SELECT COUNT(*) FROM RAW_EXCEL_DATA")
count = cur.fetchone()[0]
print(f"   ✅ Found {count} records\n")

if count == 0:
    print("⚠️  No data in RAW_EXCEL_DATA. Please upload Excel files first.")
    cur.close()
    conn.close()
    exit(1)

# Drop and create view
print("🗑️  Dropping old view...")
cur.execute("DROP VIEW IF EXISTS FLATTENED_STUDENTS")

print("📝 Creating FLATTENED_STUDENTS view...")
cur.execute("""
CREATE OR REPLACE VIEW FLATTENED_STUDENTS AS
SELECT
    ID,
    FILE_NAME,
    UPLOADED_AT,
    f.key::STRING AS COLUMN_NAME,
    f.value::STRING AS VALUE
FROM RAW_EXCEL_DATA,
LATERAL FLATTEN(input => DATA) f
""")

print("✅ View created!\n")

# Verify
print("🔍 Verifying view...")
cur.execute("SELECT COUNT(*) FROM FLATTENED_STUDENTS")
view_count = cur.fetchone()[0]
print(f"   ✅ {view_count} records in view\n")

# Show columns
print("📋 Available columns:")
cur.execute("SELECT DISTINCT COLUMN_NAME FROM FLATTENED_STUDENTS ORDER BY COLUMN_NAME LIMIT 20")
for row in cur.fetchall():
    print(f"   - {row[0]}")

# Show sample
print("\n📊 Sample records:")
cur.execute("SELECT * FROM FLATTENED_STUDENTS LIMIT 3")
for i, row in enumerate(cur.fetchall(), 1):
    print(f"\n   Record {i}:")
    print(f"      ID: {row[0]}")
    print(f"      FILE: {row[1]}")
    print(f"      COLUMN: {row[3]}")
    print(f"      VALUE: {row[4]}")

conn.commit()
cur.close()
conn.close()

print("\n✅✅✅ SUCCESS! FLATTENED_STUDENTS view is ready.")
print("\nYou can now run: python test.py")