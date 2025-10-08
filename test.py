import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

from langchain_community.utilities import SQLDatabase
from langchain_openai import ChatOpenAI
from langchain.chains import create_sql_query_chain
from langchain_community.tools import QuerySQLDatabaseTool
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough

# Load environment variables
load_dotenv()

# --- Snowflake credentials ---
user = quote_plus(os.getenv("SNOWFLAKE_USER"))
password = quote_plus(os.getenv("SNOWFLAKE_PASSWORD"))
account = os.getenv("SNOWFLAKE_ACCOUNT")
database = os.getenv("SNOWFLAKE_DATABASE")
schema = os.getenv("SNOWFLAKE_SCHEMA")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
role = os.getenv("SNOWFLAKE_ROLE")

# --- Snowflake connection URI with view_support=True ---
snowflake_uri = (
    f"snowflake://{user}:{password}"
    f"@{account}/{database}"
    f"/{schema}?warehouse={warehouse}&role={role}"
)

print("🔗 Connecting to Snowflake...")

# CRITICAL: Add view_support=True to see views
db = SQLDatabase.from_uri(snowflake_uri, view_support=True)

# Check what tables/views are available
available_tables = db.get_usable_table_names()
print("✅ Available tables/views:", available_tables)

# Find the flattened view
FLATTENED_VIEW = None
for table in available_tables:
    if 'FLATTENED_STUDENTS' == table.upper():
        FLATTENED_VIEW = table
        break

if not FLATTENED_VIEW:
    print("\n❌ ERROR: Could not find FLATTENED_STUDENTS view!")
    print("Available tables:", available_tables)
    print("\nTroubleshooting:")
    print("1. Make sure you ran: python scripts/inspect_raw_data.py")
    print("2. Check that the view exists in Snowflake")
    print("3. Verify your .env file has correct DATABASE and SCHEMA")
    exit(1)

print(f"✅ Using view: {FLATTENED_VIEW}")
print("\n📋 View schema:")
print(db.get_table_info())

# Get available columns from the data
print("\n📊 Discovering available columns...")
try:
    columns_query = f"SELECT DISTINCT COLUMN_NAME FROM {FLATTENED_VIEW} ORDER BY COLUMN_NAME LIMIT 50"
    columns_result = db.run(columns_query)
    print("Available columns:", columns_result)
except Exception as e:
    print(f"⚠️  Could not fetch columns: {e}")
    columns_result = "Unknown"

# --- Initialize LLM ---
llm = ChatOpenAI(temperature=0, model_name="gpt-4")

# --- Enhanced SQL prompt for EAV structure ---
sql_prompt = PromptTemplate.from_template(
    """You are a SQL expert working with Snowflake and an EAV (Entity-Attribute-Value) data model.

Database Schema:
{table_info}

Available Excel Column Names:
{available_columns}

DATA MODEL EXPLANATION:
The table {view_name} uses EAV format with these columns:
- ID: Unique identifier for each student record
- FILE_NAME: Source Excel file
- UPLOADED_AT: Upload timestamp
- COLUMN_NAME: The Excel column header (e.g., 'Name', 'Net Fee', 'Email ID')
- VALUE: The cell value for that column

CRITICAL SQL PATTERNS:

1. Finding a student by name:
SELECT ID 
FROM {view_name}
WHERE COLUMN_NAME = 'Name' 
  AND UPPER(VALUE) LIKE '%SEARCH_NAME%'

2. Getting multiple attributes for one student (REQUIRED PATTERN):
SELECT 
    ID,
    MAX(CASE WHEN COLUMN_NAME = 'Name' THEN VALUE END) AS name,
    MAX(CASE WHEN COLUMN_NAME = 'Net Fee' THEN VALUE END) AS net_fee,
    MAX(CASE WHEN COLUMN_NAME = 'Email ID' THEN VALUE END) AS email
FROM {view_name}
WHERE ID IN (
    SELECT ID FROM {view_name}
    WHERE COLUMN_NAME = 'Name' AND UPPER(VALUE) LIKE '%VINAY SAHU%'
)
GROUP BY ID

3. Top N students by numeric column:
WITH student_fees AS (
    SELECT 
        ID,
        MAX(CASE WHEN COLUMN_NAME = 'Name' THEN VALUE END) AS name,
        TRY_CAST(MAX(CASE WHEN COLUMN_NAME = 'Net Fee' THEN VALUE END) AS NUMBER) AS net_fee
    FROM {view_name}
    GROUP BY ID
)
SELECT name, net_fee
FROM student_fees
WHERE net_fee IS NOT NULL
ORDER BY net_fee DESC
LIMIT 5

IMPORTANT RULES:
- Column names in the data are EXACT (case-sensitive): Use the exact names from Available Excel Column Names
- For name matching: Use UPPER(VALUE) LIKE '%NAME%' for flexibility
- For numeric comparisons: Always use TRY_CAST(VALUE AS NUMBER)
- Always use MAX() with CASE WHEN for pivoting
- GROUP BY ID to collapse rows

Question: {input}

Generate ONLY the SQL query, no explanation or formatting."""
)

def generate_sql(question: str) -> str:
    """Generate SQL with full context"""
    return (sql_prompt | llm | StrOutputParser()).invoke({
        "input": question,
        "table_info": db.get_table_info(),
        "view_name": FLATTENED_VIEW,
        "available_columns": columns_result
    })

execute_query = QuerySQLDatabaseTool(db=db)

# --- Answer generation ---
answer_prompt = PromptTemplate.from_template(
    """Based on the SQL results, provide a clear, conversational answer.

Question: {question}
SQL Query: {query}
Results: {result}

Provide a natural language answer. Format numbers with commas if appropriate.
If no data found, explain that clearly."""
)

answer_chain = answer_prompt | llm | StrOutputParser()

# --- Complete pipeline ---
def ask_question(question: str):
    """Process a question end-to-end"""
    print(f"\n{'='*70}")
    print(f"💬 Question: {question}")
    print(f"{'='*70}")
    
    try:
        # Generate SQL
        sql_query = generate_sql(question)
        print(f"\n🔍 Generated SQL:\n{sql_query}\n")
        
        # Execute query
        result = execute_query.invoke(sql_query)
        print(f"📊 Raw Results:\n{result}\n")
        
        # Generate answer
        answer = answer_chain.invoke({
            "question": question,
            "query": sql_query,
            "result": result
        })
        print(f"✅ Answer:\n{answer}")
        
        return answer
        
    except Exception as e:
        error_msg = f"❌ Error: {str(e)}"
        print(error_msg)
        import traceback
        traceback.print_exc()
        return None

# --- Test queries ---
if __name__ == "__main__":
    queries = [
        "Who are the top 5 students by Net Fee?",
        "What is the pending fees of student keshav?",
        "What is the email ID of student Karthik Shibu",
    ]
    
    for q in queries:
        ask_question(q)