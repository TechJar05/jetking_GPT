# from sqlalchemy import create_engine, text
# import pandas as pd
# import os
# from dotenv import load_dotenv
# from urllib.parse import quote_plus

# load_dotenv()

# # --- MySQL Credentials ---
# mysql_user = quote_plus(os.getenv("MYSQL_USER"))
# mysql_password = quote_plus(os.getenv("MYSQL_PASSWORD"))
# mysql_host = os.getenv("MYSQL_HOST", "localhost")
# mysql_port = os.getenv("MYSQL_PORT", "3306")
# mysql_database = os.getenv("MYSQL_DATABASE")

# # --- Build MySQL URI ---
# mysql_uri = f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_database}"

# # --- Create SQLAlchemy Engine ---
# engine = create_engine(mysql_uri)

# # --- Tables to Test ---
# tables = ["customer_users", "bookings", "customer_sources", "campaigns"]


# for table in tables:
#     try:
#         with engine.connect() as conn:
#             # Use SQLAlchemy text() for queries
#             result = conn.execute(text(f"SELECT COUNT(*) AS total FROM {table}"))
#             count = result.scalar()
#             print(f"{table}: ✅ {count} records found")
#     except Exception as e:
#         print(f"{table}: ❌ {e}")




# from sqlalchemy import inspect
# inspector = inspect(engine)
# print(inspector.get_table_names())



from sqlalchemy import create_engine, text, inspect
import pandas as pd
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

# --- Load Environment Variables ---
load_dotenv()

# --- MySQL Credentials ---
mysql_user = quote_plus(os.getenv("MYSQL_USER"))
mysql_password = quote_plus(os.getenv("MYSQL_PASSWORD"))
mysql_host = os.getenv("MYSQL_HOST", "localhost")
mysql_port = os.getenv("MYSQL_PORT", "3306")
mysql_database = os.getenv("MYSQL_DATABASE")

# --- Build MySQL URI ---
mysql_uri = f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_database}"

# --- Create SQLAlchemy Engine ---
engine = create_engine(mysql_uri)

# --- Initialize Inspector ---
inspector = inspect(engine)

# --- Get All Tables ---
tables = inspector.get_table_names()

print("\n📋 Tables Found in Database:\n", tables)

# --- Iterate Over Tables ---
for table in tables:
    print(f"\n🔹 Table: {table}")
    
    # Get column info
    try:
        columns = inspector.get_columns(table)
        for col in columns:
            print(f"   - {col['name']} ({col['type']})")
    except Exception as e:
        print(f"   ⚠️ Could not fetch columns: {e}")

    # Get record count
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
            print(f"   ✅ Total Records: {count}")
    except Exception as e:
        print(f"   ❌ Could not count records: {e}")

print("\n✅ Metadata extraction complete.")
