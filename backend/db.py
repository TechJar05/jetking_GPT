# import os
# import snowflake.connector
# from dotenv import load_dotenv

# load_dotenv(dotenv_path=".env")  # adjust if needed

# def get_snowflake_connection():
#     conn = snowflake.connector.connect(
#         user=os.getenv("SNOWFLAKE_USER"),
#         password=os.getenv("SNOWFLAKE_PASSWORD"),
#         account=os.getenv("SNOWFLAKE_ACCOUNT"),
#         warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
#         database=os.getenv("SNOWFLAKE_DATABASE"),
#         schema=os.getenv("SNOWFLAKE_SCHEMA"),
#         role=os.getenv("SNOWFLAKE_ROLE")
#     )
#     return conn






# import os
# from dotenv import load_dotenv
# from urllib.parse import quote_plus
# from langchain_community.utilities import SQLDatabase

# load_dotenv()

# user = quote_plus(os.getenv("SNOWFLAKE_USER"))
# password = quote_plus(os.getenv("SNOWFLAKE_PASSWORD"))
# account = os.getenv("SNOWFLAKE_ACCOUNT")
# database = os.getenv("SNOWFLAKE_DATABASE")
# schema = os.getenv("SNOWFLAKE_SCHEMA")
# warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
# role = os.getenv("SNOWFLAKE_ROLE")

# snowflake_uri = (
#     f"snowflake://{user}:{password}"
#     f"@{account}/{database}"
#     f"/{schema}?warehouse={warehouse}&role={role}"
# )

# db = SQLDatabase.from_uri(snowflake_uri, view_support=True)




import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
from langchain_community.utilities import SQLDatabase

load_dotenv()

# --- MySQL Credentials ---
mysql_user = quote_plus(os.getenv("MYSQL_USER"))
mysql_password = quote_plus(os.getenv("MYSQL_PASSWORD"))
mysql_host = os.getenv("MYSQL_HOST", "localhost")
mysql_port = os.getenv("MYSQL_PORT", "3306")
mysql_database = os.getenv("MYSQL_DATABASE")

# --- Build MySQL URI ---
# Using PyMySQL driver
mysql_uri = f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_database}"

# --- Connect to MySQL ---
mysql_db = SQLDatabase.from_uri(mysql_uri, view_support=True)

print("✅ Connected to MySQL successfully!")
