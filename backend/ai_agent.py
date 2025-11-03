# import os
# import time
# import pandas as pd
# from sqlalchemy import create_engine
# from langchain_community.chat_models import ChatOpenAI
# from langchain_community.utilities import SQLDatabase
# from langchain_community.agent_toolkits import create_sql_agent
# from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit

# # ---------------------------
# # PostgreSQL Configuration
# # --------------------------
# PG_USER = "jetbotgpt"

# PG_PASSWORD = "rracIlAQr4iNtn0vbl9Sp58nGlwTNmHm"

# PG_HOST = "dpg-d3njt7s9c44c73eb42sg-a.oregon-postgres.render.com"

# PG_PORT = "5432"

# PG_DB = "jetbotgpt"

# # Load OpenAI API key from environment (.env)
# OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
# os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY

# # ---------------------------
# # Create PostgreSQL engine and database
# # ---------------------------
# engine = create_engine(
#     f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
# )
# db = SQLDatabase(engine, include_tables=["students"])

# # ---------------------------
# # Initialize GPT with retry logic
# # ---------------------------
# llm = ChatOpenAI(
#     temperature=0,
#     model_name="gpt-3.5-turbo",
#     request_timeout=60,
#     max_retries=3,
# )

# # ---------------------------
# # Create SQL Agent
# # ---------------------------
# toolkit = SQLDatabaseToolkit(db=db, llm=llm)
# agent_executor = create_sql_agent(
#     llm=llm,
#     toolkit=toolkit,
#     verbose=False,
#     agent_type="openai-tools",
#     handle_parsing_errors=True,
#     max_iterations=5,
#     max_execution_time=60,
#     return_intermediate_steps=False,
# )

# # ---------------------------
# # Query with retry
# # ---------------------------
# def query_with_retry(query, max_attempts=3, delay=2):
#     for attempt in range(max_attempts):
#         try:
#             result = agent_executor.invoke({"input": query})
#             return result['output']
#         except Exception as e:
#             error_msg = str(e)
#             if "500" in error_msg or "server_error" in error_msg:
#                 if attempt < max_attempts - 1:
#                     time.sleep(delay * (2 ** attempt))
#                     continue
#                 else:
#                     return "❌ OpenAI API issue. Try again later."
#             elif "rate_limit" in error_msg.lower():
#                 if attempt < max_attempts - 1:
#                     time.sleep(60)
#                     continue
#                 else:
#                     return "❌ Rate limit exceeded. Wait a minute."
#             else:
#                 return f"❌ Error: {error_msg}"
#     return "❌ Max retry attempts reached."

# # ---------------------------
# # Direct SQL fallback
# # ---------------------------
# def execute_direct_sql(query_description):
#     query_map = {
#         "total centres": "SELECT COUNT(DISTINCT center) as total_centers FROM students",
#         "list centres": "SELECT DISTINCT center FROM students ORDER BY center",
#         "all centres": "SELECT DISTINCT center FROM students ORDER BY center",
#         "count students": "SELECT COUNT(*) as total_students FROM students",
#         "total students": "SELECT COUNT(*) as total_students FROM students",
#         "centers with outstanding": """
#             SELECT center, COUNT(*) as student_count, SUM(outstanding) as total_outstanding 
#             FROM students 
#             WHERE outstanding > 0 
#             GROUP BY center 
#             ORDER BY total_outstanding DESC
#         """,
#     }
#     query_lower = query_description.lower()
#     for key, sql in query_map.items():
#         if key in query_lower:
#             try:
#                 df = pd.read_sql(sql, engine)
#                 return df.to_dict(orient="records")
#             except Exception as e:
#                 return {"error": str(e)}
#     return None

# # ---------------------------
# # Main query function
# # ---------------------------
# def ask_question(query):
#     # Try direct SQL fallback first
#     direct_result = execute_direct_sql(query)
#     if direct_result:
#         return {"source": "sql", "result": direct_result}

#     # Else use GPT agent
#     gpt_result = query_with_retry(query)
#     return {"source": "gpt", "result": gpt_result}

# ---------------------------------------------------------------------------





import os
import re
import time
from dotenv import load_dotenv
from urllib.parse import quote_plus
from langchain_community.utilities import SQLDatabase
from langchain_openai import ChatOpenAI
from langchain_community.tools import QuerySQLDatabaseTool
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from sqlalchemy import create_engine, text

# Load environment variables
load_dotenv()

# ---------------------------
# MySQL Configuration
# ---------------------------
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DATABASE", "your_database_name")

# Load OpenAI API key
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY not found in environment variables")

os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY

# ---------------------------
# MySQL Connection URI
# ---------------------------
user = quote_plus(MYSQL_USER)
password = quote_plus(MYSQL_PASSWORD)
mysql_uri = f"mysql+pymysql://{user}:{password}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"

print("=" * 70)
print("🤖 JETKING ENHANCED AI AGENT (WITH QUERY NORMALIZATION)")
print("=" * 70)
print("🔗 Connecting to MySQL...")

# ---------------------------
# Create SQLAlchemy Engine (for health checks)
# ---------------------------
try:
    engine = create_engine(
        mysql_uri,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False
    )
    
    # Test connection
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    
    print("✅ MySQL connection established successfully")
    
except Exception as e:
    print(f"❌ Database connection error: {e}")
    raise

# ---------------------------
# MySQL Connection with LangChain
# ---------------------------
try:
    db = SQLDatabase.from_uri(
        mysql_uri,
        view_support=True,
        sample_rows_in_table_info=2,
        max_string_length=100,
    )
    
    available_tables = db.get_usable_table_names()
    print(f"✅ Connected! Found {len(available_tables)} tables")
    print(f"📋 Tables: {', '.join(available_tables[:15])}...")
    
except Exception as e:
    print(f"❌ Database connection error: {e}")
    raise

# ---------------------------
# Build Enhanced Schema Information (ALL TABLES)
# ---------------------------
def build_enhanced_schema():
    """Build comprehensive schema information from the database"""
    schema_info = f"""
DATABASE: {MYSQL_DB}

╔════════════════════════════════════════════════════════════════════╗
║                      COMPLETE TABLE REFERENCE                       ║
╚════════════════════════════════════════════════════════════════════╝

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. CUSTOMER & STUDENT TABLES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 customer_users - Main customer/student information
   Columns: id, customer_id, user_id, is_active (1=active), 
            created_by, created_at, updated_at, updated_by
   Purpose: Links customers to user accounts

📋 customer_follow_ups - Customer follow-up records
   Columns: id, customer_id, user_id, followup_date, status, remarks
   Purpose: Track customer interaction history

📋 customer_notes - Customer interaction notes
   Columns: id, customer_id, note, created_by, created_at
   Purpose: Store notes about customer interactions

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
2. BRANCH & CENTER TABLES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🏢 branch - Training centers/branches
   Columns: id, name, status (1=active), latitude, longitude, address,
            city_id, centre_id, branch_email, branch_mobile, zone_id,
            center_type_id, created_at, updated_at
   Purpose: Store branch location and contact information

🏢 branch_users - Staff assigned to branches
   Columns: id, branch_id, user_id, is_active, created_at
   Purpose: Map employees to their branches

🏢 center_type - Types of training centers
   Columns: id, name, description, status
   Purpose: Categorize branches (e.g., franchise, company-owned)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
3. CAMPAIGN & MARKETING TABLES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📢 campaigns - Marketing campaigns
   Columns: id, name, from_date, to_date, budget, spent, status,
            description, lead_target, contact_target, 
            opportunity_target, conversion_target, created_at
   Purpose: Track marketing campaign performance

📢 campaign_has_branch - Campaign-branch mapping
   Columns: id, campaign_id, branch_id, created_at
   Purpose: Link campaigns to specific branches

📢 campaign_medium - Campaign sources/mediums
   Columns: id, campaign_id, medium_name, source, created_at
   Purpose: Track campaign channels (e.g., Google Ads, Facebook)

📢 campaign_leads - Leads generated from campaigns
   Columns: id, campaign_id, customer_id, status, created_at
   Purpose: Track lead generation from campaigns

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
4. CALL CENTER & COMMUNICATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

☎️ callcenter_calls - Call records
   Columns: id, cc_callid, call_type, did, dialstatus, user_id,
            customer_mobile, start_time, end_time, duration,
            audio_file, created_at
   Purpose: Track all inbound/outbound calls

☎️ callcenter_queue - Call queue management
   Columns: id, queue_name, max_wait_time, status
   Purpose: Manage call routing and waiting

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
5. LOCATION TABLES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🌍 cities - City information (48,321+ records)
   Columns: id, name, state_id, status (1=active)
   Purpose: Store city master data

🌍 states - State/province information
   Columns: id, name, country_id, status
   Purpose: Store state master data

🌍 countries - Country information
   Columns: id, name, code, status
   Purpose: Store country master data

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
6. CERTIFICATION & EDUCATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎓 certifications - Student certifications
   Columns: id, student_id, name, institue, duration, 
            year_of_passing, class, marks, created_at
   Purpose: Store student certification records

🎓 course_center - Course-center mapping
   Columns: id, course_id, center_id, status, created_at
   Purpose: Track which courses are offered at which centers

🎓 course_zone - Course-zone mapping
   Columns: id, course_id, zone_id, status
   Purpose: Track course availability by zone

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
7. CONVERSION & PERFORMANCE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💰 conversion_logs - Lead conversion tracking
   Columns: id, customer_id, from_status, to_status, 
            converted_by, converted_at, remarks
   Purpose: Track lead-to-customer conversion pipeline

💰 contest_performances - Performance tracking
   Columns: id, user_id, contest_id, score, rank, created_at
   Purpose: Track employee/student performance in contests

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
8. FINANCIAL TABLES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💵 budgets - Budget allocations
   Columns: id, department, amount, fiscal_year, status
   Purpose: Track departmental budgets

💵 banks - Bank information
   Columns: id, bank_name, branch_name, account_number, ifsc_code
   Purpose: Store banking details for transactions

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
9. BOOKINGS & PAYMENTS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📅 bookings - Student course bookings
   Columns: id, customer_id, course_id, booking_date, status
   Purpose: Track course enrollments

📅 booking_payment_slabs - Payment installments
   Columns: id, booking_id, amount, due_date, paid_date, status
   Purpose: Manage payment schedules

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
10. CONFIGURATION & SYSTEM
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚙️ configuration - System settings
   Columns: id, config_key, config_value, description, status
   Purpose: Store application configuration

⚙️ access_acl - Access control lists
   Columns: id, role_id, resource, permission, status
   Purpose: Manage user permissions


╔════════════════════════════════════════════════════════════════════╗
║                      QUERY WRITING GUIDELINES                       ║
╚════════════════════════════════════════════════════════════════════╝

✅ MYSQL SYNTAX RULES:
1. Use backticks (`) for reserved words or special characters
2. Text search: WHERE name LIKE '%text%' (case-insensitive by default)
3. Case-sensitive: WHERE BINARY name = 'Text'
4. Date filtering: WHERE DATE(created_at) = '2024-01-01'
5. Status checks: WHERE status = 1 (active) or is_active = 1
6. NULL handling: WHERE column IS NOT NULL or COALESCE(column, default)
7. Aggregations: Always use GROUP BY with non-aggregated columns
8. Joins: Use proper foreign keys (e.g., branch.city_id = cities.id)
9. Limit results: Add LIMIT 100 for large datasets

📊 COMMON QUERY PATTERNS:

Pattern 1: Count active records
SELECT COUNT(*) FROM table_name WHERE status = 1

Pattern 2: List with details
SELECT id, name, created_at FROM table_name 
WHERE status = 1 ORDER BY created_at DESC LIMIT 100

Pattern 3: Search by name
SELECT * FROM table_name 
WHERE LOWER(name) LIKE LOWER('%search%')

Pattern 4: Date range
SELECT * FROM table_name 
WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31'

Pattern 5: Join tables
SELECT b.name as branch, c.name as city 
FROM branch b 
LEFT JOIN cities c ON b.city_id = c.id 
WHERE b.status = 1

Pattern 6: Group by aggregation
SELECT branch_id, COUNT(*) as student_count 
FROM customer_users 
WHERE is_active = 1 
GROUP BY branch_id 
ORDER BY student_count DESC
"""
    return schema_info

ENHANCED_SCHEMA = build_enhanced_schema()
print("✅ Enhanced schema with ALL tables loaded successfully")

# ---------------------------
# Initialize LLM
# ---------------------------
llm = ChatOpenAI(
    temperature=0,
    model_name="gpt-4o-mini",
    request_timeout=90,
    max_retries=3
)

# ---------------------------
# Query Normalization Layer
# ---------------------------
normalization_prompt = PromptTemplate.from_template(
    """You are a query understanding assistant for a training institute database system.
Analyze the user's question and extract structured information.

User Question: {question}

Extract and normalize:
1. **Intent**: What does the user want?
   - count_records: Counting/aggregating data
   - list_records: Listing/showing records
   - find_specific: Finding specific record
   - aggregate_data: Sum, average, etc.
   - compare_data: Comparing entities
   - trend_analysis: Time-based analysis

2. **Tables Involved**: Which tables are needed?
   - Customer: customer_users, customer_follow_ups, customer_notes
   - Branch: branch, branch_users, center_type
   - Campaign: campaigns, campaign_has_branch, campaign_medium, campaign_leads
   - Calls: callcenter_calls, callcenter_queue
   - Location: cities, states, countries
   - Education: certifications, course_center, course_zone
   - Financial: budgets, banks, conversion_logs
   - Bookings: bookings, booking_payment_slabs
   - System: configuration, access_acl

3. **Entities**: Extract key information:
   - Names (customers, branches, campaigns)
   - Locations (cities, states, addresses)
   - Dates/Time periods
   - Status (active/inactive)
   - Numeric values (budget, count, amount)
   - Phone numbers, emails

4. **Filters**: What conditions to apply?
   - Status filters (active/inactive)
   - Date ranges
   - Text matching
   - Numeric comparisons
   - Relationships (joins needed)

5. **Aggregation**: Any calculations needed?
   - COUNT, SUM, AVG, MIN, MAX
   - GROUP BY requirements
   - ORDER BY preferences

6. **Normalized Question**: Rewrite clearly for SQL generation

Output format:
Intent: <intent>
Tables: <tables needed>
Entities: <key entities>
Filters: <conditions>
Aggregation: <if any>
Normalized Question: <clear rewritten question>

Examples:

User Question: "how many branches in mumbai"
Intent: count_records
Tables: branch, cities
Entities: location=Mumbai
Filters: branch.status=1, cities.name LIKE '%Mumbai%'
Aggregation: COUNT(*)
Normalized Question: Count all active branches located in Mumbai city

User Question: "show campaigns with budget over 100000"
Intent: list_records
Tables: campaigns
Entities: budget_threshold=100000
Filters: status=1, budget > 100000
Aggregation: None
Normalized Question: List all active campaigns with budget exceeding 100,000

Now analyze:
User Question: {question}
"""
)

normalize_chain = normalization_prompt | llm | StrOutputParser()

# ---------------------------
# SQL Generation with Enhanced Prompts
# ---------------------------
enhanced_sql_prompt = PromptTemplate.from_template(
    """You are an expert MySQL query generator for a training institute database.
Generate a precise, optimized SQL query based on the normalized question.

Schema Information:
{schema}

Normalized Query Analysis:
{normalized_info}

STRICT SQL GENERATION RULES:

✅ SYNTAX REQUIREMENTS:
1. Always start with SELECT (read-only queries)
2. Use proper MySQL syntax
3. Use backticks (`) only for reserved words/special chars
4. End with semicolon is optional

✅ FILTERING RULES:
5. Text search: WHERE LOWER(column) LIKE LOWER('%text%')
6. Date filters: WHERE DATE(column) = '2024-01-01' or BETWEEN
7. Status: WHERE status = 1 or is_active = 1
8. NULL checks: WHERE column IS NOT NULL

✅ JOIN REQUIREMENTS:
9. Use proper foreign keys for joins
10. Prefer LEFT JOIN for optional relationships
11. Use table aliases for readability

✅ AGGREGATION RULES:
12. Use COUNT(*), SUM(), AVG(), MIN(), MAX() properly
13. Always include GROUP BY for non-aggregated columns
14. Add HAVING for filtered aggregations

✅ PERFORMANCE:
15. Add LIMIT 100 for large result sets (unless COUNT)
16. Use indexes: id, status, created_at, updated_at

✅ OUTPUT FORMAT:
17. Return ONLY the SQL query
18. No markdown, no backticks, no explanations
19. Single line or properly formatted multi-line

NOW GENERATE SQL FOR:
{normalized_info}

Return only the SQL query:"""
)

def normalize_query(question):
    """Normalize and understand user's natural language question"""
    try:
        normalized = normalize_chain.invoke({"question": question})
        print(f"\n🔍 Normalized Query Analysis:")
        print(normalized)
        return normalized
    except Exception as e:
        print(f"⚠️ Normalization warning: {e}")
        return f"Intent: general_query\nNormalized Question: {question}"

def generate_sql(question):
    """Generate SQL with normalization and validation"""
    try:
        # Step 1: Normalize the query
        normalized_info = normalize_query(question)
        
        # Step 2: Generate SQL from normalized query
        raw_sql = (enhanced_sql_prompt | llm | StrOutputParser()).invoke({
            "schema": ENHANCED_SCHEMA,
            "normalized_info": normalized_info
        })
        
        # Step 3: Clean SQL
        sql = raw_sql.strip().removeprefix("```sql").removeprefix("```").removesuffix("```").strip()
        
        # Step 4: Validate SQL
        sql = validate_and_clean_sql(sql)
        
        print(f"\n📝 Generated SQL:")
        print(sql)
        return sql
        
    except Exception as e:
        print(f"❌ SQL Generation Error: {e}")
        raise ValueError(f"Failed to generate valid SQL: {str(e)}")

def validate_and_clean_sql(sql):
    """Comprehensive SQL validation and cleaning"""
    
    # 1. Remove comments
    sql = re.sub(r'--.*', '', sql)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.S)
    
    # 2. Trim whitespace
    sql = sql.strip()
    
    # 3. Ensure SELECT only (read-only)
    if not sql.upper().startswith("SELECT"):
        raise ValueError("Query must start with SELECT (read-only)")
    
    # 4. Block dangerous operations
    dangerous_keywords = [
        "DROP", "DELETE", "UPDATE", "INSERT", "TRUNCATE",
        "ALTER", "CREATE", "GRANT", "REVOKE", "EXEC",
        "EXECUTE", "CALL", "LOAD", "OUTFILE", "INFILE"
    ]
    for keyword in dangerous_keywords:
        if re.search(rf'\b{keyword}\b', sql, re.IGNORECASE):
            raise ValueError(f"Dangerous operation not allowed: {keyword}")
    
    # 5. Normalize whitespace
    sql = re.sub(r'\s+', ' ', sql).strip()
    
    # 6. Ensure reasonable length
    if len(sql) > 5000:
        raise ValueError("Query too complex (> 5000 characters)")
    
    return sql

# ---------------------------
# Query Execution with Limits
# ---------------------------
class LimitedQueryTool(QuerySQLDatabaseTool):
    """Enhanced query tool with automatic limits and truncation"""
    
    def _run(self, query: str):
        # Add LIMIT if missing (and not using aggregation)
        if ("SELECT" in query.upper() and 
            "LIMIT" not in query.upper() and 
            not any(agg in query.upper() for agg in ["COUNT(", "SUM(", "AVG(", "MAX(", "MIN(", "GROUP BY"])):
            query = query.rstrip(";") + " LIMIT 100"
        
        # Execute query
        result = super()._run(query)
        
        # Truncate very long results
        if len(result) > 6000:
            result = result[:6000] + "\n... (truncated for readability. Use more specific filters.)"
        
        return result

execute_query = LimitedQueryTool(db=db)

# ---------------------------
# Answer Generation
# ---------------------------
answer_generation_prompt = PromptTemplate.from_template(
    """Generate a clear, conversational answer based on the query results.

Original Question: {question}
SQL Query: {sql_query}
Query Results: {result}

Guidelines:
- Answer in 2-4 complete sentences
- Include specific numbers, names, dates from results
- Format large numbers with commas (e.g., 1,234 not 1234)
- If no results: explain why
- If multiple results: summarize key findings
- Be precise but conversational
- Don't mention technical terms unless natural

Answer:"""
)

answer_chain = answer_generation_prompt | llm | StrOutputParser()

# ---------------------------
# Main Query Function (FastAPI Compatible)
# ---------------------------
def ask_question(question: str, context: str = None):
    """
    Process natural language questions with advanced normalization and validation.
    
    Args:
        question (str): Natural language question
        context (str, optional): Additional context
        
    Returns:
        dict: Response with answer and metadata
    """
    try:
        # Input validation
        if not question or len(question.strip()) < 3:
            return {
                "success": False,
                "question": question,
                "error": "Please provide a valid question (minimum 3 characters)",
                "source": "validation"
            }
        
        # Enhance question with context
        if context:
            enhanced_question = f"{question}\nContext: {context}"
        else:
            enhanced_question = question
        
        print(f"\n{'='*70}")
        print(f"❓ Question: {question}")
        if context:
            print(f"📝 Context: {context}")
        print(f"{'='*70}")
        
        # Generate and execute SQL
        sql = generate_sql(enhanced_question)
        result = execute_query.invoke(sql)
        
        # Handle empty results
        if not result or result.strip() in ["[]", "", "()"]:
            return {
                "success": True,
                "question": question,
                "sql_query": sql,
                "result": [],
                "answer": "No matching data found in the database for your query. Try adjusting your search criteria.",
                "source": "ai_agent"
            }
        
        # Generate natural language answer
        answer = answer_chain.invoke({
            "question": question,
            "sql_query": sql,
            "result": result[:2500]
        })
        
        return {
            "success": True,
            "question": question,
            "sql_query": sql,
            "result": result,
            "answer": answer.strip(),
            "source": "ai_agent"
        }
        
    except ValueError as ve:
        print(f"❌ Validation Error: {ve}")
        return {
            "success": False,
            "question": question,
            "sql_query": None,
            "result": None,
            "error": str(ve),
            "source": "validation_error"
        }
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return {
            "success": False,
            "question": question,
            "sql_query": None,
            "result": None,
            "error": f"I encountered an error processing your question: {str(e)}. Please try rephrasing it.",
            "source": "processing_error"
        }

# ---------------------------
# Health Check Function
# ---------------------------
def health_check():
    """
    Check if database and AI services are healthy
    """
    health = {
        "database": False,
        "openai": False,
        "agent": False
    }
    
    # Check database
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        health["database"] = True
        print("✅ Database health check passed")
    except Exception as e:
        print(f"❌ Database health check failed: {e}")
    
    # Check OpenAI
    try:
        test_response = llm.invoke("test")
        health["openai"] = True
        print("✅ OpenAI health check passed")
    except Exception as e:
        print(f"❌ OpenAI health check failed: {e}")
    
    # Check agent (overall health)
    health["agent"] = health["database"] and health["openai"]
    
    return health

# ---------------------------
# Testing Mode
# ---------------------------
if __name__ == "__main__":
    print("\n" + "="*70)
    print("🧪 Testing Enhanced AI Agent")
    print("="*70)
    
    # Run health check
    print("\n🏥 Running Health Check...")
    health_status = health_check()
    print(f"\nHealth Status: {health_status}")
    
    # Test questions
    test_questions = [
        "How many branches are there?",
        "Show me campaigns with budget over 100000",
        "Total calls made this year",
        "List all active branches in Mumbai"
    ]
    
    print("\n" + "="*70)
    print("🧪 Testing Sample Questions")
    print("="*70)
    
    for q in test_questions:
        try:
            result = ask_question(q)
            print(f"\n{'='*70}")
            print(f"Q: {q}")
            print(f"A: {result.get('answer', result.get('error'))}")
            print(f"SQL: {result.get('sql_query')}")
            print(f"Source: {result.get('source')}")
            print(f"Success: {result.get('success')}")
            print(f"{'='*70}")
            time.sleep(1)  # Rate limiting
        except Exception as e:
            print(f"\n❌ Error testing '{q}': {e}")
    
    print("\n✅ Testing completed!")