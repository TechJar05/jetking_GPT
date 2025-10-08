import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv
import decimal
import json

# Load credentials
load_dotenv(dotenv_path=".env")

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

print("Snowflake account:", os.getenv("SNOWFLAKE_ACCOUNT"))

# Connect to Snowflake
conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
cur = conn.cursor()

# Ensure correct DB and schema
cur.execute("USE DATABASE TRAINING_DB;")
cur.execute("USE SCHEMA PUBLIC;")

folder_path = "excel_files"  # folder with Excel files

def serialize_value(v):
    if pd.isna(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return float(v)
    return v

# Loop through Excel files
for file_name in os.listdir(folder_path):
    if file_name.endswith(".xlsx") or file_name.endswith(".xls"):
        file_path = os.path.join(folder_path, file_name)
        print(f"Processing {file_name} ...")
        
        df = pd.read_excel(file_path)
        print(f"Columns detected: {list(df.columns)}")
        
        for _, row in df.iterrows():
            row_dict = {k: serialize_value(v) for k, v in row.to_dict().items()}
            # Convert dict to JSON string
            json_data = json.dumps(row_dict)
            
            # Cast JSON string to VARIANT using PARSE_JSON
            cur.execute(
                "INSERT INTO RAW_EXCEL_DATA (file_name, data) SELECT %s, PARSE_JSON(%s)",
                (file_name, json_data)
            )

conn.commit()
print("✅ All Excel files loaded into Snowflake successfully.")
cur.close()
conn.close()