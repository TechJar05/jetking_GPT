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
db = SQLDatabase.from_uri(
    snowflake_uri,
    view_support=True,
    sample_rows_in_table_info=0,
    max_string_length=100,
)

# Will be set after detecting table name
UNIFIED_TABLE_NAME = None

# Minimal get_table_info override
def minimal_get_table_info(_=None):
    return f"""
TABLE: {UNIFIED_TABLE_NAME}
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

# Determine the actual table name (could be UNIFIED_STUDENTS, unified_students, or "unified_students")
UNIFIED_TABLE_NAME = UNIFIED_VIEW if UNIFIED_VIEW else "unified_students"

if not FLATTENED_VIEW:
    print("\n❌ ERROR: FLATTENED_STUDENTS view not found!")
else:
    print(f"✅ Using views: {FLATTENED_VIEW}, {UNIFIED_TABLE_NAME}")

# --- Detect Columns ---
print("\n🔍 Detecting actual column names from unified_students...")
KNOWN_COLUMNS = []

try:
    describe_result = db.run(f"DESCRIBE TABLE {UNIFIED_TABLE_NAME}")
    col_matches = re.findall(r"\('([^']+)',\s*'[^']+',\s*'COLUMN'", str(describe_result))
    if col_matches:
        KNOWN_COLUMNS = col_matches
        print(f"✅ Found {len(KNOWN_COLUMNS)} columns via DESCRIBE.")
    else:
        raise Exception("DESCRIBE parse failed.")
except Exception as e:
    print(f"⚠️ DESCRIBE failed: {e}")
    try:
        # Extract just the table name without schema for INFORMATION_SCHEMA query
        table_name_only = UNIFIED_TABLE_NAME.split('.')[-1].replace('"', '')
        col_query = f"""
        SELECT COLUMN_NAME 
        FROM {database}.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{table_name_only}'
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

# --- Enhanced Schema with Examples ---
ENHANCED_SCHEMA = f"""
DATABASE: {database}.{schema}
TABLE: {UNIFIED_TABLE_NAME}

KEY COLUMNS:
- "Student_Name": Full name of student (use UPPER() + LIKE for search)
- "First_Name", "Last_Name": Name components
- "Gender": Male/Female/Other
- "Course": Course enrolled in
- "Enrollment_Date": Date of enrollment (format: YYYY-MM-DD or similar)
- "Center_Name": Training center location
- "Paid_Amount": Amount paid by student
- "Balance_Due_Amount": Outstanding balance
- STUDENT_ID: Unique identifier (no quotes)
- FILE_COUNT: Number of source files (no quotes)

All columns: {columns_display}

QUERY PATTERNS:
1. Name search: Use UPPER("Student_Name") LIKE '%NAME%'
2. Date filters: Use "Enrollment_Date" with LIKE '2024%' or date comparisons
3. Aggregations: Use COUNT(*), SUM(), AVG() with GROUP BY
4. Multiple conditions: Combine with AND/OR
5. IMPORTANT: Always use table name: {UNIFIED_TABLE_NAME}
"""

print("\n📋 Enhanced schema loaded successfully.")

# --- LLM ---
llm = ChatOpenAI(temperature=0, model_name="gpt-4o-mini")

# -------------------------------------------------------------------
# ✅ Query Normalization Layer
# -------------------------------------------------------------------

normalization_prompt = PromptTemplate.from_template(
    """You are a query understanding assistant. Analyze the user's question and extract structured information.

User Question: {question}

Extract and normalize:
1. **Intent**: What does the user want? (e.g., find_student, count_students, get_payment_info, list_courses, aggregate_data)
2. **Entities**: Extract key information:
   - Student names (handle typos/variations)
   - Dates/Years
   - Courses
   - Gender
   - Centers
   - Numeric values
3. **Filters**: What conditions to apply?
4. **Aggregation**: Any counting, summing, averaging needed?
5. **Normalized Question**: Rewrite the question clearly for SQL generation

Output format:
Intent: <intent>
Entities: <key entities found>
Filters: <conditions to apply>
Aggregation: <if any>
Normalized Question: <clear rewritten question>

Examples:

User Question: "wat is gender of keshav"
Intent: find_student_attribute
Entities: student_name=Keshav, attribute=Gender
Filters: Student name contains 'Keshav'
Aggregation: None
Normalized Question: What is the gender of the student named Keshav?

User Question: "how many stdents enrolld in 2024"
Intent: count_students
Entities: year=2024
Filters: Enrollment date in year 2024
Aggregation: COUNT
Normalized Question: How many students enrolled in the year 2024?

User Question: "show me all students from delhi who owes money"
Intent: list_students
Entities: location=Delhi, payment_status=has_balance
Filters: Address contains 'Delhi' AND Balance_Due_Amount > 0
Aggregation: None
Normalized Question: List all students from Delhi who have an outstanding balance.

Now analyze:
User Question: {question}
"""
)

normalize_chain = normalization_prompt | llm | StrOutputParser()

# --- Enhanced SQL Generation Prompt ---
enhanced_sql_prompt = PromptTemplate.from_template(
    """You are an expert SQL query generator for Snowflake. Generate a precise SQL query based on the normalized question and schema.

Schema:
{schema}

Normalized Query Information:
{normalized_info}

STRICT RULES:
1. Always use table name: {table_name}
2. Quote mixed-case columns with double quotes: "Student_Name", "Gender", "Course"
3. Never quote: STUDENT_ID, FILE_COUNT, SOURCE_FILES
4. For name searches: Use UPPER("Student_Name") LIKE UPPER('%name%')
5. For date filters: Use "Enrollment_Date" with appropriate LIKE or comparison
6. For numeric filters: Direct comparison on "Paid_Amount", "Balance_Due_Amount"
7. Always handle NULL values appropriately
8. Use proper aggregation functions: COUNT(*), SUM(), AVG()
9. Add GROUP BY when using aggregations with other columns
10. Return only the SQL query, no explanations

EXAMPLES:

Q: What is the gender of the student named Keshav?
SQL: SELECT "Student_Name", "Gender" FROM {table_name} WHERE UPPER("Student_Name") LIKE UPPER('%Keshav%')

Q: How many students enrolled in the year 2024?
SQL: SELECT COUNT(*) as student_count FROM {table_name} WHERE "Enrollment_Date" LIKE '2024%'

Q: List all students from Delhi who have an outstanding balance.
SQL: SELECT "Student_Name", "Address", "Balance_Due_Amount" FROM {table_name} WHERE UPPER("Address") LIKE UPPER('%Delhi%') AND "Balance_Due_Amount" > 0

Q: What is the total amount paid by all students?
SQL: SELECT SUM("Paid_Amount") as total_paid FROM {table_name} WHERE "Paid_Amount" IS NOT NULL

Q: Show students grouped by course with count
SQL: SELECT "Course", COUNT(*) as student_count FROM {table_name} GROUP BY "Course" ORDER BY student_count DESC

Now generate SQL for:
{normalized_info}

SQL Query (no markdown, no explanation):"""
)

def normalize_query(question):
    """Normalize and understand user's natural language question"""
    try:
        normalized = normalize_chain.invoke({"question": question})
        print(f"\n🔍 Normalized Query:\n{normalized}")
        return normalized
    except Exception as e:
        print(f"⚠️ Normalization warning: {e}")
        return f"Intent: general_query\nNormalized Question: {question}"

def generate_sql(question):
    """Generate SQL with normalization"""
    # Step 1: Normalize the query
    normalized_info = normalize_query(question)
    
    # Step 2: Generate SQL from normalized query
    raw = (enhanced_sql_prompt | llm | StrOutputParser()).invoke({
        "schema": ENHANCED_SCHEMA,
        "normalized_info": normalized_info,
        "table_name": UNIFIED_TABLE_NAME
    })
    
    sql = raw.strip().removeprefix("```sql").removeprefix("```").removesuffix("```").strip()
    
    # Step 3: Validate and clean SQL
    sql = validate_and_clean_sql(sql)
    
    print(f"\n📝 Generated SQL:\n{sql}")
    return sql

def validate_and_clean_sql(sql):
    """Validate and clean the generated SQL safely"""
    # ✅ 1. Remove comments
    sql = re.sub(r'--.*', '', sql)  # Remove single-line comments
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.S)  # Remove multi-line comments

    # ✅ 2. Trim whitespace
    sql = sql.strip()

    # ✅ 3. Ensure it starts with SELECT (for read-only safety)
    if not sql.upper().startswith("SELECT"):
        raise ValueError("Generated query must start with SELECT")

    # ✅ 4. Disallow dangerous SQL operations
    dangerous_keywords = [
        "DROP", "DELETE", "UPDATE", "INSERT",
        "TRUNCATE", "ALTER", "CREATE", "EXEC", "CALL"
    ]
    for word in dangerous_keywords:
        if re.search(rf'\b{word}\b', sql, re.IGNORECASE):
            raise ValueError(f"Query contains dangerous operation: {word}")

    # ✅ 5. Replace incorrect table name references if needed
    sql = re.sub(
        r'\bunified_students\b',
        UNIFIED_TABLE_NAME,
        sql,
        flags=re.IGNORECASE
    )

    # ✅ 6. Ensure correct table name is present
    if UNIFIED_TABLE_NAME.lower() not in sql.lower():
        raise ValueError(f"Query must use {UNIFIED_TABLE_NAME} table")

    # ✅ 7. Normalize spaces
    sql = re.sub(r'\s+', ' ', sql).strip()

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

# --- Enhanced Answer Prompt ---
enhanced_answer_prompt = PromptTemplate.from_template(
    """Provide a clear, concise answer to the user's question based on the query results.

Original Question: {question}
Query Results: {result}

Guidelines:
- Answer directly and conversationally (2-4 sentences)
- Include specific numbers, names, or values from results
- If no results: explain that no matching data was found
- If multiple results: summarize key findings
- Be precise with numbers and dates

Answer:"""
)
answer_chain = enhanced_answer_prompt | llm | StrOutputParser()

# -------------------------------------------------------------------
# ✅ FastAPI endpoint-compatible function with enhanced error handling
# -------------------------------------------------------------------
def ask_question(question: str):
    """
    Main function to process natural language questions with normalization
    """
    try:
        # Validate input
        if not question or len(question.strip()) < 3:
            return {
                "question": question,
                "sql_query": None,
                "result": None,
                "answer": "Please provide a valid question."
            }
        
        print(f"\n{'='*70}")
        print(f"❓ Question: {question}")
        print(f"{'='*70}")
        
        # Generate SQL with normalization
        sql = generate_sql(question)
        
        # Execute query
        result = execute_query.invoke(sql)
        
        # Handle empty results
        if not result or result.strip() == "[]":
            return {
                "question": question,
                "sql_query": sql,
                "result": [],
                "answer": "No matching data found for your query."
            }
        
        # Generate natural language answer
        answer = answer_chain.invoke({
            "question": question,
            "result": result[:1500]
        })
        
        return {
            "question": question,
            "sql_query": sql,
            "result": result,
            "answer": answer.strip()
        }
        
    except ValueError as ve:
        print(f"❌ Validation Error: {ve}")
        return {
            "question": question,
            "sql_query": None,
            "result": None,
            "answer": f"Invalid query: {str(ve)}"
        }
    except Exception as e:
        print(f"❌ Error: {e}")
        return {
            "question": question,
            "sql_query": None,
            "result": None,
            "answer": f"I encountered an error processing your question. Please try rephrasing it or make it more specific."
        }

# --- Optional: Interactive testing mode ---
if __name__ == "__main__":
    print("\n" + "="*70)
    print("🎯 Interactive Testing Mode")
    print("="*70)
    
    test_questions = [
        "wat is gender of keshav",
        "how many stdents enrolld in 2024",
        "show me all students from delhi who owes money",
        "total amount paid by everyone",
        "list all courses with student count"
    ]
    
    print("\n🧪 Testing with sample questions:\n")
    for q in test_questions:
        result = ask_question(q)
        print(f"\n{'='*70}")
        print(f"Q: {q}")
        print(f"A: {result['answer']}")
        print(f"SQL: {result['sql_query']}")
        print(f"{'='*70}")