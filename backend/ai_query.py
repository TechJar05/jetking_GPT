import re
from langchain_openai import ChatOpenAI
from langchain_community.tools import QuerySQLDatabaseTool
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from backend.db import db

# ------------------------------------------------------------------------------
# Step 1: Discover the view
# ------------------------------------------------------------------------------
available_tables = db.get_usable_table_names()
FLATTENED_VIEW = next(
    (t for t in available_tables if t.upper() == "FLATTENED_STUDENTS"), None
)
if not FLATTENED_VIEW:
    raise Exception("❌ Could not find FLATTENED_STUDENTS view in Snowflake!")

# ------------------------------------------------------------------------------
# Step 2: Get column info
# ------------------------------------------------------------------------------
try:
    columns_query = f"SELECT DISTINCT COLUMN_NAME FROM {FLATTENED_VIEW} ORDER BY COLUMN_NAME LIMIT 50"
    columns_result = db.run(columns_query)
except Exception:
    columns_result = "Unknown"

# ------------------------------------------------------------------------------
# Step 3: Initialize LLM + SQL executor
# ------------------------------------------------------------------------------
llm = ChatOpenAI(temperature=0, model_name="gpt-4o-mini")

execute_query = QuerySQLDatabaseTool(db=db)

# ------------------------------------------------------------------------------
# Step 4: SQL-only prompt (strict)
# ------------------------------------------------------------------------------
sql_prompt = PromptTemplate.from_template("""
You are a Snowflake SQL expert.
Generate a **valid SQL query only** for the following question. 

### Rules:
- Output ONLY the SQL (starting with SELECT / WITH / INSERT / UPDATE / DELETE / CREATE)
- Do NOT include any explanations, markdown (```), or text outside SQL.
- Always use MAX(CASE WHEN ...) pivot pattern for EAV data.
- Use TRY_CAST(VALUE AS NUMBER) for numeric comparisons.
- Always GROUP BY ID.
- Match names using UPPER(VALUE) LIKE '%NAME%'.

### Context
Database Schema:
{table_info}

Available Columns:
{available_columns}

View Name:
{view_name}

### Question
{input}
""")

# ------------------------------------------------------------------------------
# Step 5: Answer prompt for final natural language reply
# ------------------------------------------------------------------------------
answer_prompt = PromptTemplate.from_template("""
Question: {question}
SQL Query: {query}
Results: {result}

Now provide a clear, conversational answer. 
If data is empty, explain that politely.
""")

answer_chain = answer_prompt | llm | StrOutputParser()

# ------------------------------------------------------------------------------
# Step 6: Utility to clean LLM output into valid SQL
# ------------------------------------------------------------------------------
def clean_sql_output(output: str) -> str:
    """Strip markdown, explanations, or non-SQL text — keep pure SQL."""
    if not output:
        return ""

    # Extract SQL if inside markdown fences
    if "```" in output:
        match = re.search(r"```(?:sql)?(.*?)```", output, re.DOTALL | re.IGNORECASE)
        if match:
            output = match.group(1)

    # Remove any leading/trailing non-SQL text
    sql_keywords = ("SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "CREATE")
    first_sql = re.search(r"(SELECT|WITH|INSERT|UPDATE|DELETE|CREATE)", output, re.IGNORECASE)
    if first_sql:
        output = output[first_sql.start():]

    # Cleanup whitespace and remove markdown remnants
    cleaned = re.sub(r"[`#*]", "", output).strip()
    return cleaned


def fix_common_sql_issues(sql: str) -> str:
    """Automatically fix common LLM-generated SQL mistakes."""
    if not sql:
        return sql

    # ✅ Fix COUNT() with missing argument
    sql = re.sub(r"COUNT\s*\(\s*\)", "COUNT(*)", sql, flags=re.IGNORECASE)

    # ✅ Fix double %% (from escaped LIKE clauses)
    sql = sql.replace("%%", "%")

    # ✅ Optional: remove unnecessary GROUP BY id when not needed
    if "COUNT(" in sql and "GROUP BY id" in sql:
        sql = sql.replace("GROUP BY id", "")

    # ✅ Ensure semicolon at end (optional, for Snowflake)
    sql = sql.strip().rstrip(";") + ";"

    return sql


# ------------------------------------------------------------------------------
# Step 7: SQL generation
# ------------------------------------------------------------------------------
def generate_sql(question: str) -> str:
    # Get concise schema info
    table_info = db.get_table_info()
    if len(table_info) > 4000:  # limit to ~4000 characters
        table_info = table_info[:4000] + "\n-- [truncated schema info]"

    # Limit columns shown to LLM
    available_cols = columns_result
    if isinstance(available_cols, list) and len(available_cols) > 50:
        available_cols = available_cols[:50]

    raw_output = (sql_prompt | llm | StrOutputParser()).invoke({
        "input": question,
        "table_info": table_info,
        "view_name": FLATTENED_VIEW,
        "available_columns": available_cols
    })

    cleaned_sql = clean_sql_output(raw_output)
    fixed_sql = fix_common_sql_issues(cleaned_sql)

    print("\n🧹 Cleaned SQL Query:\n", fixed_sql)
    return fixed_sql



# ------------------------------------------------------------------------------
# Step 8: Full question-to-answer pipeline
# ------------------------------------------------------------------------------
def ask_question(question: str):
    """Generate SQL, execute it, and produce a conversational answer."""
    try:
        sql_query = generate_sql(question)
        result = execute_query.invoke(sql_query)

        # If result is empty or None
        if not result:
            polite_answer = (
                "I couldn’t find any matching data for your question. "
                "You might want to try rephrasing or asking in a different way."
            )
            return {
                "question": question,
                "sql_query": sql_query,
                "result": result,
                "answer": polite_answer
            }

        # Normal success flow
        answer = answer_chain.invoke({
            "question": question,
            "query": sql_query,
            "result": result
        })
        return {
            "question": question,
            "sql_query": sql_query,
            "result": result,
            "answer": answer
        }

    except Exception as e:
        # Log error internally if needed
        error_message = str(e).lower()

        # If it's an SQL or technical error, suppress it
        if any(word in error_message for word in ["sql", "snowflake", "traceback", "invalid", "error", "exception"]):
            polite_response = (
                "Sorry, something went wrong while processing your request. "
                "Please try again or ask your question in a different way."
            )
        else:
            polite_response = (
                "I'm unable to fetch that information right now. "
                "Please try again in a moment."
            )

        return {
            "question": question,
            "sql_query": None,
            "result": None,
            "answer": polite_response
        }
