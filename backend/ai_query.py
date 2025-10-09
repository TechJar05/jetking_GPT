import os
import re
from dotenv import load_dotenv
from urllib.parse import quote_plus
from langchain_community.utilities import SQLDatabase
from langchain_openai import ChatOpenAI
from langchain_community.tools import QuerySQLDatabaseTool
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser

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

snowflake_uri = (
    f"snowflake://{user}:{password}"
    f"@{account}/{database}/{schema}?warehouse={warehouse}&role={role}"
)

print("=" * 70)
print("🤖 STUDENT DATA CHATBOT (FASTAPI MODE)")
print("=" * 70)
print("🔗 Connecting to Snowflake...")

# --- Snowflake Connection ---
# remove include_tables to prevent ValueError
db = SQLDatabase.from_uri(
    snowflake_uri,
    view_support=True,
    sample_rows_in_table_info=0,
    max_string_length=100,
)

# Minimal get_table_info override
def minimal_get_table_info(_=None):
    return """
TABLE: unified_students
Columns (quoted where needed):
"Student_Name", "Gender", "Course", "First_Name", "Last_Name",
"Enrollment_Date", "Address", "Center_Name", "Paid_Amount",
"Balance_Due_Amount", STUDENT_ID, FILE_COUNT, SOURCE_FILES
"""

db.get_table_info = minimal_get_table_info

# --- Detect Available Tables ---
available_tables = db.get_usable_table_names()
print("✅ Available tables/views:", available_tables)

FLATTENED_VIEW = next((t for t in available_tables if "FLATTENED_STUDENTS" in t.upper()), None)
UNIFIED_VIEW = next((t for t in available_tables if "UNIFIED_STUDENTS" in t.upper()), None)

if not FLATTENED_VIEW:
    print("\n❌ ERROR: FLATTENED_STUDENTS view not found!")
else:
    print(f"✅ Using views: {FLATTENED_VIEW}, {UNIFIED_VIEW}")

# --- Detect Columns ---
print("\n🔍 Detecting actual column names from unified_students...")
KNOWN_COLUMNS = []

try:
    describe_result = db.run("DESCRIBE TABLE unified_students")
    col_matches = re.findall(r"\('([^']+)',\s*'[^']+',\s*'COLUMN'", str(describe_result))
    if col_matches:
        KNOWN_COLUMNS = col_matches
        print(f"✅ Found {len(KNOWN_COLUMNS)} columns via DESCRIBE.")
    else:
        raise Exception("DESCRIBE parse failed.")
except Exception as e:
    print(f"⚠️ DESCRIBE failed: {e}")
    try:
        col_query = f"""
        SELECT COLUMN_NAME 
        FROM {database}.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = 'UNIFIED_STUDENTS'
        ORDER BY ORDINAL_POSITION
        """
        col_result = db.run(col_query)
        KNOWN_COLUMNS = re.findall(r"\('([^']+)'", str(col_result))
        print(f"✅ Found {len(KNOWN_COLUMNS)} columns via INFORMATION_SCHEMA.")
    except Exception as e2:
        print(f"⚠️ Fallback used: {e2}")
        KNOWN_COLUMNS = [
            "STUDENT_ID", "Student_Name", "Gender", "Course", "Fee_Type",
            "Enrollment_Date", "Address", "DOB", "First_Name", "Last_Name",
            "Guardian_Name", "Enrollment_No", "Center_Name",
            "Paid_Amount", "Total_Payable_Amount", "Balance_Due_Amount",
            "FILE_COUNT", "SOURCE_FILES"
        ]

print(f"📋 Columns detected: {', '.join(KNOWN_COLUMNS[:10])}...")

# --- Column Quoting ---
def quote_column(col):
    if col in ["STUDENT_ID", "FILE_COUNT", "SOURCE_FILES", "LAST_UPDATED"]:
        return col
    return f'"{col}"'

quoted_columns = [quote_column(c) for c in KNOWN_COLUMNS]
columns_display = ", ".join(quoted_columns[:20])

# --- Minimal Schema for Prompt ---
MINIMAL_SCHEMA = f"""
DATABASE: {database}.{schema}
TABLE: unified_students
Columns: {columns_display}
{'... and ' + str(len(KNOWN_COLUMNS) - 20) + ' more' if len(KNOWN_COLUMNS) > 20 else ''}
Use quotes for mixed-case columns, e.g., "Student_Name"
"""

print("\n📋 Schema loaded successfully.")

# --- LLM ---
llm = ChatOpenAI(temperature=0, model_name="gpt-4o-mini")

# --- SQL Prompt ---
sql_prompt = PromptTemplate.from_template(
    """Generate valid Snowflake SQL for the question below.

Schema:
{schema}

Rules:
1. Use unified_students table.
2. Use underscores, not spaces (Student_Name, not Student Name).
3. Quote mixed-case columns.
4. Do not quote STUDENT_ID, FILE_COUNT, SOURCE_FILES.

Examples:
Q: Gender of student Keshav?
A: SELECT "Gender" FROM unified_students WHERE UPPER("Student_Name") LIKE '%KESHAV%'

Q: Students enrolled in 2024?
A: SELECT "Student_Name", "Enrollment_Date" FROM unified_students WHERE "Enrollment_Date" LIKE '2024%'

Question: {input}

SQL (no markdown):"""
)

def generate_sql(question):
    raw = (sql_prompt | llm | StrOutputParser()).invoke({"input": question, "schema": MINIMAL_SCHEMA})
    sql = raw.strip().removeprefix("```sql").removeprefix("```").removesuffix("```").strip()
    return sql

# --- Limited Query Tool ---
class LimitedQueryTool(QuerySQLDatabaseTool):
    def _run(self, query: str):
        if "SELECT" in query.upper() and "LIMIT" not in query.upper() and "COUNT" not in query.upper():
            query = query.rstrip(";") + " LIMIT 100"
        result = super()._run(query)
        if len(result) > 4000:
            result = result[:4000] + "\n... (truncated)"
        return result

execute_query = LimitedQueryTool(db=db)

# --- Answer Prompt ---
answer_prompt = PromptTemplate.from_template(
    """Answer briefly (2–3 sentences).

Question: {question}
Results: {result}

Answer:"""
)
answer_chain = answer_prompt | llm | StrOutputParser()

# -------------------------------------------------------------------
# ✅ FastAPI endpoint-compatible function
# -------------------------------------------------------------------
def ask_question(question: str):
    try:
        sql = generate_sql(question)
        result = execute_query.invoke(sql)
        if not result:
            return {
                "question": question,
                "sql_query": sql,
                "result": [],
                "answer": "No matching data found."
            }
        answer = answer_chain.invoke({"question": question, "result": result[:1500]})
        return {
            "question": question,
            "sql_query": sql,
            "result": result,
            "answer": answer.strip()
        }
    except Exception as e:
        return {
            "question": question,
            "sql_query": None,
            "result": None,
            "answer": f"Error: {str(e)}"
        }
