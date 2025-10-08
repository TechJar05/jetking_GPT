import snowflake.connector
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()

user = quote_plus(os.getenv('SNOWFLAKE_USER'))
password = quote_plus(os.getenv('SNOWFLAKE_PASSWORD'))
account = os.getenv('SNOWFLAKE_ACCOUNT')
database = os.getenv('SNOWFLAKE_DATABASE')
schema = os.getenv('SNOWFLAKE_SCHEMA')
warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
role = os.getenv('SNOWFLAKE_ROLE')

SNOWFLAKE_CONFIG = {
    "user": user,
    "password": password,
    "account": account,
    "warehouse": warehouse,
    "database": database,
    "schema": schema,
    "role": role
}

print("🔗 Connecting to Snowflake...")
conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
cur = conn.cursor()

print(f"✅ Connected to {database}.{schema}")

# First, check if raw_excel_data has data
print("\n🔍 Checking source table...")
cur.execute("SELECT COUNT(*) FROM raw_excel_data")
count = cur.fetchone()[0]
print(f"   Records in raw_excel_data: {count}")

if count == 0:
    print("\n⚠️  WARNING: raw_excel_data is empty. Upload data first!")
    cur.close()
    conn.close()
    exit(1)

# Check data structure
print("\n🔍 Inspecting data structure...")
cur.execute("SELECT data, TYPEOF(data) FROM raw_excel_data LIMIT 1")
sample = cur.fetchone()
print(f"   Data type: {sample[1]}")
print(f"   Sample: {str(sample[0])[:200]}...")

# Drop existing view
print("\n🗑️  Dropping old view if exists...")
cur.execute("DROP VIEW IF EXISTS flattened_students")
print("   ✅ Old view dropped")

# Create flattened view - UPPERCASE for Snowflake convention
print("\n📝 Creating FLATTENED_STUDENTS view...")

create_view_sql = """
CREATE OR REPLACE VIEW FLATTENED_STUDENTS AS
SELECT
    id,
    file_name,
    uploaded_at,
    f.key::STRING AS column_name,
    f.value::STRING AS value
FROM raw_excel_data,
LATERAL FLATTEN(input => data) f
"""

try:
    cur.execute(create_view_sql)
    print("   ✅ View created successfully")
    
    # Verify view was created
    print("\n✅ Verifying view creation...")
    cur.execute("SELECT COUNT(*) FROM FLATTENED_STUDENTS")
    view_count = cur.fetchone()[0]
    print(f"   Records in FLATTENED_STUDENTS: {view_count}")
    
    # Show sample from view
    print("\n📊 Sample data from view:")
    cur.execute("SELECT * FROM FLATTENED_STUDENTS LIMIT 5")
    samples = cur.fetchall()
    col_names = [desc[0] for desc in cur.description]
    
    for i, row in enumerate(samples, 1):
        print(f"\n   Row {i}:")
        for col, val in zip(col_names, row):
            print(f"      {col}: {val}")
    
    # Show unique columns
    print("\n📋 Available column names in data:")
    cur.execute("SELECT DISTINCT column_name FROM FLATTENED_STUDENTS ORDER BY column_name")
    columns = cur.fetchall()
    for col in columns:
        print(f"      - {col[0]}")
    
    conn.commit()
    print("\n✅✅✅ View 'FLATTENED_STUDENTS' created and verified successfully!")
    
except Exception as e:
    print(f"\n❌ Error creating view: {e}")
    import traceback
    traceback.print_exc()
    conn.rollback()

finally:
    cur.close()
    conn.close()