# test.py
from langchain_community.utilities import SQLDatabase
from langchain_openai import ChatOpenAI
from langchain.chains import create_sql_query_chain
from langchain_community.tools import QuerySQLDatabaseTool
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()

# URL-encode credentials to handle special characters
user = quote_plus(os.getenv('SNOWFLAKE_USER'))
password = quote_plus(os.getenv('SNOWFLAKE_PASSWORD'))
account = os.getenv('SNOWFLAKE_ACCOUNT')
database = os.getenv('SNOWFLAKE_DATABASE')
schema = os.getenv('SNOWFLAKE_SCHEMA')
warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
role = os.getenv('SNOWFLAKE_ROLE')

# Create Snowflake connection string
snowflake_uri = (
    f"snowflake://{user}:{password}"
    f"@{account}/{database}"
    f"/{schema}?warehouse={warehouse}&role={role}"
)

# Connect to Snowflake
db = SQLDatabase.from_uri(snowflake_uri)
llm = ChatOpenAI(temperature=0, model_name="gpt-4")

# Debug: list available tables
print("Available tables:", db.get_usable_table_names())

# Use the flattened view for dynamic columns
FLATTENED_VIEW = "flattened_students"
if FLATTENED_VIEW not in db.get_usable_table_names():
    raise ValueError(f"Flattened view '{FLATTENED_VIEW}' not found. Run create_flattened_view.py first.")

# Create SQL query chain
write_query = create_sql_query_chain(llm, db, top_k=5)
execute_query = QuerySQLDatabaseTool(db=db)

# Answer chain for NL queries
answer_prompt = PromptTemplate.from_template(
    """You are an assistant that answers user questions based on the following SQL query and its result.

Question: {question}
SQL Query: {query}
SQL Result: {result}
Answer: """
)
answer = answer_prompt | llm | StrOutputParser()

# Complete chain: generate SQL -> execute -> format answer
chain = (
    RunnablePassthrough.assign(query=write_query).assign(
        result=lambda x: execute_query.invoke(x["query"].replace("FROM raw_excel_data", f"FROM {FLATTENED_VIEW}"))
    )
    | answer
)

# Example query
query = "Who are the top 5 students by Net Fee?"
result = chain.invoke({"question": query})
print("\n🔹 Result:\n", result)
