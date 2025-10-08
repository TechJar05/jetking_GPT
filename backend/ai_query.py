

from langchain_openai import ChatOpenAI
from langchain_community.tools import QuerySQLDatabaseTool
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from db import db

# Identify the flattened view
available_tables = db.get_usable_table_names()
FLATTENED_VIEW = next(
    (t for t in available_tables if t.upper() == "FLATTENED_STUDENTS"), None
)
if not FLATTENED_VIEW:
    raise Exception("❌ Could not find FLATTENED_STUDENTS view in Snowflake!")

# Get column info
try:
    columns_query = f"SELECT DISTINCT COLUMN_NAME FROM {FLATTENED_VIEW} ORDER BY COLUMN_NAME LIMIT 50"
    columns_result = db.run(columns_query)
except Exception as e:
    columns_result = "Unknown"

llm = ChatOpenAI(temperature=0, model_name="gpt-4")
execute_query = QuerySQLDatabaseTool(db=db)

sql_prompt = PromptTemplate.from_template("""
You are a SQL expert working with Snowflake and an EAV data model.
Database Schema:
{table_info}
Available Excel Column Names:
{available_columns}

DATA MODEL:
The table {view_name} has columns: ID, FILE_NAME, UPLOADED_AT, COLUMN_NAME, VALUE

Rules:
- Use MAX(CASE WHEN ...) pivot logic.
- Always GROUP BY ID.
- Use TRY_CAST(VALUE AS NUMBER) for numeric.
- Match names with UPPER(VALUE) LIKE '%NAME%'.

Question: {input}
Generate only SQL.
""")

answer_prompt = PromptTemplate.from_template("""
Question: {question}
SQL Query: {query}
Results: {result}

Provide a clear, natural language answer.
""")

answer_chain = answer_prompt | llm | StrOutputParser()

def generate_sql(question: str) -> str:
    return (sql_prompt | llm | StrOutputParser()).invoke({
        "input": question,
        "table_info": db.get_table_info(),
        "view_name": FLATTENED_VIEW,
        "available_columns": columns_result
    })

def ask_question(question: str):
    """End-to-end processing of natural language question"""
    sql_query = generate_sql(question)
    result = execute_query.invoke(sql_query)
    answer = answer_chain.invoke({
        "question": question,
        "query": sql_query,
        "result": result
    })
    return {"question": question, "sql_query": sql_query, "result": result, "answer": answer}
