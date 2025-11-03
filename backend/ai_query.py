# import os
# import re
# from dotenv import load_dotenv
# from urllib.parse import quote_plus
# from langchain_community.utilities import SQLDatabase
# from langchain_openai import ChatOpenAI
# from langchain_community.tools import QuerySQLDatabaseTool
# from langchain_core.prompts import PromptTemplate
# from langchain_core.output_parsers import StrOutputParser

# # Load environment variables
# load_dotenv()

# # --- Snowflake credentials ---
# user = quote_plus(os.getenv("SNOWFLAKE_USER"))
# password = quote_plus(os.getenv("SNOWFLAKE_PASSWORD"))
# account = os.getenv("SNOWFLAKE_ACCOUNT")
# database = os.getenv("SNOWFLAKE_DATABASE")
# schema = os.getenv("SNOWFLAKE_SCHEMA")
# warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
# role = os.getenv("SNOWFLAKE_ROLE")

# snowflake_uri = (
#     f"snowflake://{user}:{password}"
#     f"@{account}/{database}/{schema}?warehouse={warehouse}&role={role}"
# )

# print("=" * 70)
# print("🤖 STUDENT DATA CHATBOT (FASTAPI MODE)")
# print("=" * 70)
# print("🔗 Connecting to Snowflake...")

# # --- Snowflake Connection ---
# db = SQLDatabase.from_uri(
#     snowflake_uri,
#     view_support=True,
#     sample_rows_in_table_info=0,
#     max_string_length=100,
# )

# # Will be set after detecting table name
# UNIFIED_TABLE_NAME = None

# # Minimal get_table_info override
# def minimal_get_table_info(_=None):
#     return f"""
# TABLE: {UNIFIED_TABLE_NAME}
# Columns (quoted where needed):
# "Student_Name", "Gender", "Course", "First_Name", "Last_Name",
# "Enrollment_Date", "Address", "Center_Name", "Paid_Amount",
# "Balance_Due_Amount", STUDENT_ID, FILE_COUNT, SOURCE_FILES
# """

# db.get_table_info = minimal_get_table_info

# # --- Detect Available Tables ---
# available_tables = db.get_usable_table_names()
# print("✅ Available tables/views:", available_tables)

# FLATTENED_VIEW = next((t for t in available_tables if "FLATTENED_STUDENTS" in t.upper()), None)
# UNIFIED_VIEW = next((t for t in available_tables if "UNIFIED_STUDENTS" in t.upper()), None)

# # Determine the actual table name (could be UNIFIED_STUDENTS, unified_students, or "unified_students")
# UNIFIED_TABLE_NAME = UNIFIED_VIEW if UNIFIED_VIEW else "unified_students"

# if not FLATTENED_VIEW:
#     print("\n❌ ERROR: FLATTENED_STUDENTS view not found!")
# else:
#     print(f"✅ Using views: {FLATTENED_VIEW}, {UNIFIED_TABLE_NAME}")

# # --- Detect Columns ---
# print("\n🔍 Detecting actual column names from unified_students...")
# KNOWN_COLUMNS = []

# try:
#     describe_result = db.run(f"DESCRIBE TABLE {UNIFIED_TABLE_NAME}")
#     col_matches = re.findall(r"\('([^']+)',\s*'[^']+',\s*'COLUMN'", str(describe_result))
#     if col_matches:
#         KNOWN_COLUMNS = col_matches
#         print(f"✅ Found {len(KNOWN_COLUMNS)} columns via DESCRIBE.")
#     else:
#         raise Exception("DESCRIBE parse failed.")
# except Exception as e:
#     print(f"⚠️ DESCRIBE failed: {e}")
#     try:
#         # Extract just the table name without schema for INFORMATION_SCHEMA query
#         table_name_only = UNIFIED_TABLE_NAME.split('.')[-1].replace('"', '')
#         col_query = f"""
#         SELECT COLUMN_NAME 
#         FROM {database}.INFORMATION_SCHEMA.COLUMNS 
#         WHERE TABLE_SCHEMA = '{schema}' 
#         AND TABLE_NAME = '{table_name_only}'
#         ORDER BY ORDINAL_POSITION
#         """
#         col_result = db.run(col_query)
#         KNOWN_COLUMNS = re.findall(r"\('([^']+)'", str(col_result))
#         print(f"✅ Found {len(KNOWN_COLUMNS)} columns via INFORMATION_SCHEMA.")
#     except Exception as e2:
#         print(f"⚠️ Fallback used: {e2}")
#         KNOWN_COLUMNS = [
#             "STUDENT_ID", "Student_Name", "Gender", "Course", "Fee_Type",
#             "Enrollment_Date", "Address", "DOB", "First_Name", "Last_Name",
#             "Guardian_Name", "Enrollment_No", "Center_Name",
#             "Paid_Amount", "Total_Payable_Amount", "Balance_Due_Amount",
#             "FILE_COUNT", "SOURCE_FILES"
#         ]

# print(f"📋 Columns detected: {', '.join(KNOWN_COLUMNS[:10])}...")

# # --- Column Quoting ---
# def quote_column(col):
#     if col in ["STUDENT_ID", "FILE_COUNT", "SOURCE_FILES", "LAST_UPDATED"]:
#         return col
#     return f'"{col}"'

# quoted_columns = [quote_column(c) for c in KNOWN_COLUMNS]
# columns_display = ", ".join(quoted_columns[:20])

# # --- Enhanced Schema with Examples ---
# ENHANCED_SCHEMA = f"""
# DATABASE: {database}.{schema}
# TABLE: {UNIFIED_TABLE_NAME}

# KEY COLUMNS:
# - "Student_Name": Full name of student (use UPPER() + LIKE for search)
# - "First_Name", "Last_Name": Name components
# - "Gender": Male/Female/Other
# - "Course": Course enrolled in
# - "Enrollment_Date": Date of enrollment (format: YYYY-MM-DD or similar)
# - "Center_Name": Training center location
# - "Paid_Amount": Amount paid by student
# - "Balance_Due_Amount": Outstanding balance
# - STUDENT_ID: Unique identifier (no quotes)
# - FILE_COUNT: Number of source files (no quotes)

# All columns: {columns_display}

# QUERY PATTERNS:
# 1. Name search: Use UPPER("Student_Name") LIKE '%NAME%'
# 2. Date filters: Use "Enrollment_Date" with LIKE '2024%' or date comparisons
# 3. Aggregations: Use COUNT(*), SUM(), AVG() with GROUP BY
# 4. Multiple conditions: Combine with AND/OR
# 5. IMPORTANT: Always use table name: {UNIFIED_TABLE_NAME}
# """

# print("\n📋 Enhanced schema loaded successfully.")

# # --- LLM ---
# llm = ChatOpenAI(temperature=0, model_name="gpt-4o-mini")

# # -------------------------------------------------------------------
# # ✅ Query Normalization Layer
# # -------------------------------------------------------------------

# normalization_prompt = PromptTemplate.from_template(
#     """You are a query understanding assistant. Analyze the user's question and extract structured information.

# User Question: {question}

# Extract and normalize:
# 1. **Intent**: What does the user want? (e.g., find_student, count_students, get_payment_info, list_courses, aggregate_data)
# 2. **Entities**: Extract key information:
#    - Student names (handle typos/variations)
#    - Dates/Years
#    - Courses
#    - Gender
#    - Centers
#    - Numeric values
# 3. **Filters**: What conditions to apply?
# 4. **Aggregation**: Any counting, summing, averaging needed?
# 5. **Normalized Question**: Rewrite the question clearly for SQL generation

# Output format:
# Intent: <intent>
# Entities: <key entities found>
# Filters: <conditions to apply>
# Aggregation: <if any>
# Normalized Question: <clear rewritten question>

# Examples:

# User Question: "wat is gender of keshav"
# Intent: find_student_attribute
# Entities: student_name=Keshav, attribute=Gender
# Filters: Student name contains 'Keshav'
# Aggregation: None
# Normalized Question: What is the gender of the student named Keshav?

# User Question: "how many stdents enrolld in 2024"
# Intent: count_students
# Entities: year=2024
# Filters: Enrollment date in year 2024
# Aggregation: COUNT
# Normalized Question: How many students enrolled in the year 2024?

# User Question: "show me all students from delhi who owes money"
# Intent: list_students
# Entities: location=Delhi, payment_status=has_balance
# Filters: Address contains 'Delhi' AND Balance_Due_Amount > 0
# Aggregation: None
# Normalized Question: List all students from Delhi who have an outstanding balance.

# Now analyze:
# User Question: {question}
# """
# )

# normalize_chain = normalization_prompt | llm | StrOutputParser()

# # --- Enhanced SQL Generation Prompt ---
# enhanced_sql_prompt = PromptTemplate.from_template(
#     """You are an expert SQL query generator for Snowflake. Generate a precise SQL query based on the normalized question and schema.

# Schema:
# {schema}

# Normalized Query Information:
# {normalized_info}

# STRICT RULES:
# 1. Always use table name: {table_name}
# 2. Quote mixed-case columns with double quotes: "Student_Name", "Gender", "Course"
# 3. Never quote: STUDENT_ID, FILE_COUNT, SOURCE_FILES
# 4. For name searches: Use UPPER("Student_Name") LIKE UPPER('%name%')
# 5. For date filters: Use "Enrollment_Date" with appropriate LIKE or comparison
# 6. For numeric filters: Direct comparison on "Paid_Amount", "Balance_Due_Amount"
# 7. Always handle NULL values appropriately
# 8. Use proper aggregation functions: COUNT(*), SUM(), AVG()
# 9. Add GROUP BY when using aggregations with other columns
# 10. Return only the SQL query, no explanations

# EXAMPLES:

# Q: What is the gender of the student named Keshav?
# SQL: SELECT "Student_Name", "Gender" FROM {table_name} WHERE UPPER("Student_Name") LIKE UPPER('%Keshav%')

# Q: How many students enrolled in the year 2024?
# SQL: SELECT COUNT(*) as student_count FROM {table_name} WHERE "Enrollment_Date" LIKE '2024%'

# Q: List all students from Delhi who have an outstanding balance.
# SQL: SELECT "Student_Name", "Address", "Balance_Due_Amount" FROM {table_name} WHERE UPPER("Address") LIKE UPPER('%Delhi%') AND "Balance_Due_Amount" > 0

# Q: What is the total amount paid by all students?
# SQL: SELECT SUM("Paid_Amount") as total_paid FROM {table_name} WHERE "Paid_Amount" IS NOT NULL

# Q: Show students grouped by course with count
# SQL: SELECT "Course", COUNT(*) as student_count FROM {table_name} GROUP BY "Course" ORDER BY student_count DESC

# Now generate SQL for:
# {normalized_info}

# SQL Query (no markdown, no explanation):"""
# )

# def normalize_query(question):
#     """Normalize and understand user's natural language question"""
#     try:
#         normalized = normalize_chain.invoke({"question": question})
#         print(f"\n🔍 Normalized Query:\n{normalized}")
#         return normalized
#     except Exception as e:
#         print(f"⚠️ Normalization warning: {e}")
#         return f"Intent: general_query\nNormalized Question: {question}"

# def generate_sql(question):
#     """Generate SQL with normalization"""
#     # Step 1: Normalize the query
#     normalized_info = normalize_query(question)
    
#     # Step 2: Generate SQL from normalized query
#     raw = (enhanced_sql_prompt | llm | StrOutputParser()).invoke({
#         "schema": ENHANCED_SCHEMA,
#         "normalized_info": normalized_info,
#         "table_name": UNIFIED_TABLE_NAME
#     })
    
#     sql = raw.strip().removeprefix("```sql").removeprefix("```").removesuffix("```").strip()
    
#     # Step 3: Validate and clean SQL
#     sql = validate_and_clean_sql(sql)
    
#     print(f"\n📝 Generated SQL:\n{sql}")
#     return sql

# def validate_and_clean_sql(sql):
#     """Validate and clean the generated SQL safely"""
#     # ✅ 1. Remove comments
#     sql = re.sub(r'--.*', '', sql)  # Remove single-line comments
#     sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.S)  # Remove multi-line comments

#     # ✅ 2. Trim whitespace
#     sql = sql.strip()

#     # ✅ 3. Ensure it starts with SELECT (for read-only safety)
#     if not sql.upper().startswith("SELECT"):
#         raise ValueError("Generated query must start with SELECT")

#     # ✅ 4. Disallow dangerous SQL operations
#     dangerous_keywords = [
#         "DROP", "DELETE", "UPDATE", "INSERT",
#         "TRUNCATE", "ALTER", "CREATE", "EXEC", "CALL"
#     ]
#     for word in dangerous_keywords:
#         if re.search(rf'\b{word}\b', sql, re.IGNORECASE):
#             raise ValueError(f"Query contains dangerous operation: {word}")

#     # ✅ 5. Replace incorrect table name references if needed
#     sql = re.sub(
#         r'\bunified_students\b',
#         UNIFIED_TABLE_NAME,
#         sql,
#         flags=re.IGNORECASE
#     )

#     # ✅ 6. Ensure correct table name is present
#     if UNIFIED_TABLE_NAME.lower() not in sql.lower():
#         raise ValueError(f"Query must use {UNIFIED_TABLE_NAME} table")

#     # ✅ 7. Normalize spaces
#     sql = re.sub(r'\s+', ' ', sql).strip()

#     return sql

# # --- Limited Query Tool ---
# class LimitedQueryTool(QuerySQLDatabaseTool):
#     def _run(self, query: str):
#         if "SELECT" in query.upper() and "LIMIT" not in query.upper() and "COUNT" not in query.upper():
#             query = query.rstrip(";") + " LIMIT 100"
#         result = super()._run(query)
#         if len(result) > 4000:
#             result = result[:4000] + "\n... (truncated)"
#         return result

# execute_query = LimitedQueryTool(db=db)

# # --- Enhanced Answer Prompt ---
# enhanced_answer_prompt = PromptTemplate.from_template(
#     """Provide a clear, concise answer to the user's question based on the query results.

# Original Question: {question}
# Query Results: {result}

# Guidelines:
# - Answer directly and conversationally (2-4 sentences)
# - Include specific numbers, names, or values from results
# - If no results: explain that no matching data was found
# - If multiple results: summarize key findings
# - Be precise with numbers and dates

# Answer:"""
# )
# answer_chain = enhanced_answer_prompt | llm | StrOutputParser()

# # -------------------------------------------------------------------
# # ✅ FastAPI endpoint-compatible function with enhanced error handling
# # -------------------------------------------------------------------
# def ask_question(question: str):
#     """
#     Main function to process natural language questions with normalization
#     """
#     try:
#         # Validate input
#         if not question or len(question.strip()) < 3:
#             return {
#                 "question": question,
#                 "sql_query": None,
#                 "result": None,
#                 "answer": "Please provide a valid question."
#             }
        
#         print(f"\n{'='*70}")
#         print(f"❓ Question: {question}")
#         print(f"{'='*70}")
        
#         # Generate SQL with normalization
#         sql = generate_sql(question)
        
#         # Execute query
#         result = execute_query.invoke(sql)
        
#         # Handle empty results
#         if not result or result.strip() == "[]":
#             return {
#                 "question": question,
#                 "sql_query": sql,
#                 "result": [],
#                 "answer": "No matching data found for your query."
#             }
        
#         # Generate natural language answer
#         answer = answer_chain.invoke({
#             "question": question,
#             "result": result[:1500]
#         })
        
#         return {
#             "question": question,
#             "sql_query": sql,
#             "result": result,
#             "answer": answer.strip()
#         }
        
#     except ValueError as ve:
#         print(f"❌ Validation Error: {ve}")
#         return {
#             "question": question,
#             "sql_query": None,
#             "result": None,
#             "answer": f"Invalid query: {str(ve)}"
#         }
#     except Exception as e:
#         print(f"❌ Error: {e}")
#         return {
#             "question": question,
#             "sql_query": None,
#             "result": None,
#             "answer": f"I encountered an error processing your question. Please try rephrasing it or make it more specific."
#         }

# # --- Optional: Interactive testing mode ---
# if __name__ == "__main__":
#     print("\n" + "="*70)
#     print("🎯 Interactive Testing Mode")
#     print("="*70)
    
#     test_questions = [
#         "wat is gender of keshav",
#         "how many stdents enrolld in 2024",
#         "show me all students from delhi who owes money",
#         "total amount paid by everyone",
#         "list all courses with student count"
#     ]
    
#     print("\n🧪 Testing with sample questions:\n")
#     for q in test_questions:
#         result = ask_question(q)
#         print(f"\n{'='*70}")
#         print(f"Q: {q}")
#         print(f"A: {result['answer']}")
#         print(f"SQL: {result['sql_query']}")
#         print(f"{'='*70}")\
    
    
    
    
    
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

# --- MySQL credentials ---
host = os.getenv("MYSQL_HOST", "localhost")
port = os.getenv("MYSQL_PORT", "3306")
user = quote_plus(os.getenv("MYSQL_USER", "root"))
password = quote_plus(os.getenv("MYSQL_PASSWORD", ""))
database = os.getenv("MYSQL_DATABASE", "your_database_name")

mysql_uri = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

print("=" * 70)
print("🤖 JETKING DATA CHATBOT (FASTAPI MODE)")
print("=" * 70)
print("🔗 Connecting to MySQL...")

# --- MySQL Connection ---
db = SQLDatabase.from_uri(
    mysql_uri,
    view_support=True,
    sample_rows_in_table_info=2,  # Include sample rows for better context
    max_string_length=100,
)

# Detect Available Tables
available_tables = db.get_usable_table_names()
print(f"✅ Connected! Found {len(available_tables)} tables")
print(f"📋 Sample tables: {', '.join(available_tables[:30])}...")

# --- Build Enhanced Schema Information ---
def build_enhanced_schema():
    """Build detailed schema information from the database"""
    schema_info = f"""
DATABASE: {database}

KEY TABLES AND THEIR PURPOSES:

1. CUSTOMER TABLES:
   - customer_users: Main customer/student information
     Columns: id, customer_id, user_id, is_active, created_by, created_at, updated_at
   
2. BRANCH/CENTER TABLES:
   - branch: Training center/branch information
     Columns: id, name, status, latitude, longitude, address, city_id, centre_id, 
              branch_email, branch_mobile, zone_id, center_type_id
   - course_center: Links courses to centers
   - course_zone: Links courses to zones

3. CAMPAIGN TABLES:
   - campaigns: Marketing campaign information
     Columns: id, name, from_date, to_date, budget, spent, status, description, 
              lead_target, contact_target, opportunity_target, conversion_target
   - campaign_has_branch: Links campaigns to branches
   - campaign_medium: Campaign medium/source details

4. CALL CENTER:
   - callcenter_calls: Call records
     Columns: id, cc_callid, call_type, did, dialstatus, user_id, customer_mobile,
              start_time, end_time, duration, audio_file

5. CITIES & LOCATIONS:
   - cities: City information (48,321 records)
     Columns: id, name, state_id, status
   - countries: Country information

6. CERTIFICATIONS:
   - certifications: Student certifications
     Columns: id, student_id, name, institue, duration, year_of_passing, class

7. CONFIGURATION:
   - configuration: System configuration settings
   - access_acl: Access control settings

IMPORTANT NOTES:
- Use backticks (`) for table names if they contain special characters
- Most tables have: id, created_at, updated_at, created_by, updated_by
- Status fields typically use: 1=active, 0=inactive
- Date fields use DATE or DATETIME format
- Always check for NULL values in optional fields

COMMON QUERY PATTERNS:
1. Find active records: WHERE status = 1 OR is_active = 1
2. Date filtering: WHERE created_at >= '2024-01-01'
3. Text search: WHERE name LIKE '%search%'
4. Joins: Use proper foreign keys (e.g., branch.city_id = cities.id)
"""
    return schema_info

ENHANCED_SCHEMA = build_enhanced_schema()
print("\n📋 Enhanced schema loaded successfully.")

# --- LLM ---
llm = ChatOpenAI(temperature=0, model_name="gpt-4o-mini")

# -------------------------------------------------------------------
# ✅ Query Normalization Layer
# -------------------------------------------------------------------

normalization_prompt = PromptTemplate.from_template(
    """You are a query understanding assistant for a training institute database. 
Analyze the user's question and extract structured information.

User Question: {question}

Extract and normalize:
1. **Intent**: What does the user want? (e.g., find_customer, count_records, get_branch_info, 
   list_campaigns, aggregate_data, call_records, certification_info)
2. **Entities**: Extract key information:
   - Customer/Student names or IDs
   - Branch/Center names or locations
   - Campaign names or dates
   - Course information
   - Dates/Years
   - Phone numbers
   - Certification details
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

User Question: "how many branches are there in mumbai"
Intent: count_records
Entities: table=branch, location=Mumbai
Filters: Active branches in Mumbai (city name)
Aggregation: COUNT
Normalized Question: Count all active branches located in Mumbai

User Question: "show me all campaigns running this month"
Intent: list_campaigns
Entities: table=campaigns, time_period=current_month
Filters: Campaigns where current date is between from_date and to_date
Aggregation: None
Normalized Question: List all active campaigns running in the current month

User Question: "total calls made in 2024"
Intent: count_records
Entities: table=callcenter_calls, year=2024
Filters: Calls made in year 2024
Aggregation: COUNT
Normalized Question: Count total calls made during the year 2024

Now analyze:
User Question: {question}
"""
)

normalize_chain = normalization_prompt | llm | StrOutputParser()

# --- Enhanced SQL Generation Prompt ---
enhanced_sql_prompt = PromptTemplate.from_template(
    """You are an expert MySQL query generator. Generate a precise SQL query based on the normalized question and schema.

Schema:
{schema}

Normalized Query Information:
{normalized_info}

STRICT RULES FOR MYSQL:
1. Use backticks (`) for table/column names with special characters or reserved words
2. For text search: Use LIKE with % wildcards: WHERE name LIKE '%text%'
3. For case-insensitive search: Use LOWER() or UPPER(): WHERE LOWER(name) LIKE LOWER('%text%')
4. Date filtering: Use DATE() function or direct comparison: WHERE DATE(created_at) = '2024-01-01'
5. Status checks: WHERE status = 1 (active) or status = 0 (inactive)
6. Always use proper JOINs for related tables
7. Handle NULL values: WHERE column IS NOT NULL or COALESCE(column, default)
8. Use proper aggregation: COUNT(*), SUM(), AVG(), GROUP BY
9. Limit results for large datasets: LIMIT 100
10. Return only the SQL query, no explanations

AVAILABLE TABLES (use these exact names):
- customer_users, branch, campaigns, campaign_has_branch, campaign_medium
- callcenter_calls, cities, countries, certifications, configuration
- course_center, course_zone, conversion_logs, contest_performances
- And others from the schema above

EXAMPLES:

Q: Count all active branches in Mumbai
SQL: SELECT COUNT(*) as branch_count FROM branch WHERE status = 1 AND LOWER(address) LIKE '%mumbai%'

Q: List all campaigns with their budgets
SQL: SELECT id, name, budget, spent, from_date, to_date FROM campaigns WHERE status = 1 ORDER BY created_at DESC LIMIT 100

Q: Show total calls made in January 2024
SQL: SELECT COUNT(*) as total_calls FROM callcenter_calls WHERE start_time >= '2024-01-01' AND start_time < '2024-02-01'

Q: Find branches with their city names
SQL: SELECT b.id, b.name as branch_name, c.name as city_name, b.address FROM branch b LEFT JOIN cities c ON b.city_id = c.id WHERE b.status = 1 LIMIT 100

Q: Get certification count by student
SQL: SELECT student_id, COUNT(*) as cert_count FROM certifications GROUP BY student_id ORDER BY cert_count DESC LIMIT 100

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
        "normalized_info": normalized_info
    })
    
    sql = raw.strip().removeprefix("```sql").removeprefix("```").removesuffix("```").strip()
    
    # Step 3: Validate and clean SQL
    sql = validate_and_clean_sql(sql)
    
    print(f"\n📝 Generated SQL:\n{sql}")
    return sql

def validate_and_clean_sql(sql):
    """Validate and clean the generated SQL safely"""
    # ✅ 1. Remove comments
    sql = re.sub(r'--.*', '', sql)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.S)

    # ✅ 2. Trim whitespace
    sql = sql.strip()

    # ✅ 3. Ensure it starts with SELECT (for read-only safety)
    if not sql.upper().startswith("SELECT"):
        raise ValueError("Generated query must start with SELECT")

    # ✅ 4. Disallow dangerous SQL operations
    dangerous_keywords = [
        "DROP", "DELETE", "UPDATE", "INSERT",
        "TRUNCATE", "ALTER", "CREATE", "EXEC", "CALL",
        "GRANT", "REVOKE", "LOAD", "OUTFILE"
    ]
    for word in dangerous_keywords:
        if re.search(rf'\b{word}\b', sql, re.IGNORECASE):
            raise ValueError(f"Query contains dangerous operation: {word}")

    # ✅ 5. Ensure query uses valid tables
    valid_tables = [
        'customer_users', 'branch', 'campaigns', 'callcenter_calls',
        'cities', 'countries', 'certifications', 'configuration',
        'course_center', 'course_zone', 'campaign_has_branch',
        'campaign_medium', 'conversion_logs', 'contest_performances',
        'budgets', 'banks', 'category', 'cast_category'
    ]
    
    # Check if at least one valid table is referenced
    has_valid_table = any(table.lower() in sql.lower() for table in valid_tables)
    if not has_valid_table:
        raise ValueError("Query must reference at least one valid table from the database")

    # ✅ 6. Normalize spaces
    sql = re.sub(r'\s+', ' ', sql).strip()

    return sql

# --- Limited Query Tool ---
class LimitedQueryTool(QuerySQLDatabaseTool):
    def _run(self, query: str):
        # Add LIMIT if not present and not using COUNT/aggregation
        if ("SELECT" in query.upper() and 
            "LIMIT" not in query.upper() and 
            not any(agg in query.upper() for agg in ["COUNT(", "SUM(", "AVG(", "MAX(", "MIN("])):
            query = query.rstrip(";") + " LIMIT 100"
        
        result = super()._run(query)
        
        # Truncate very long results
        if len(result) > 5000:
            result = result[:5000] + "\n... (truncated for readability)"
        
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
- Format numbers for readability (e.g., 1,234 instead of 1234)

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
        if not result or result.strip() == "[]" or result.strip() == "":
            return {
                "question": question,
                "sql_query": sql,
                "result": [],
                "answer": "No matching data found for your query. The database might not contain records matching your criteria."
            }
        
        # Generate natural language answer
        answer = answer_chain.invoke({
            "question": question,
            "result": result[:2000]  # Limit result size for answer generation
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
            "answer": f"I encountered an error processing your question: {str(e)}. Please try rephrasing it or make it more specific."
        }

# --- Optional: Interactive testing mode ---
if __name__ == "__main__":
    print("\n" + "="*70)
    print("🎯 Interactive Testing Mode")
    print("="*70)
    
    test_questions = [
        "how many branches are there",
        "show me all campaigns",
        "total calls made in 2024",
        "list all cities in the database",
        "how many certifications were issued",
        "show me branches in mumbai"
    ]
    
    print("\n🧪 Testing with sample questions:\n")
    for q in test_questions:
        try:
            result = ask_question(q)
            print(f"\n{'='*70}")
            print(f"Q: {q}")
            print(f"A: {result['answer']}")
            print(f"SQL: {result['sql_query']}")
            print(f"{'='*70}")
        except Exception as e:
            print(f"\n❌ Error testing '{q}': {e}")
    
    # Interactive mode
    print("\n" + "="*70)
    print("💬 Interactive Mode - Type your questions (or 'quit' to exit)")
    print("="*70)
    
    while True:
        user_input = input("\n❓ Your question: ").strip()
        
        if user_input.lower() in ['quit', 'exit', 'q']:
            print("\n👋 Goodbye!")
            break
        
        if not user_input:
            continue
        
        result = ask_question(user_input)
        print(f"\n💡 Answer: {result['answer']}")
        print(f"\n🔍 SQL Used: {result['sql_query']}")