import os
import time
import pandas as pd
from sqlalchemy import create_engine
from langchain_community.chat_models import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit

# ---------------------------
# PostgreSQL Configuration
# ---------------------------
PG_USER = "jetbotgpt"

PG_PASSWORD = "rracIlAQr4iNtn0vbl9Sp58nGlwTNmHm"

PG_HOST = "dpg-d3njt7s9c44c73eb42sg-a.oregon-postgres.render.com"

PG_PORT = "5432"

PG_DB = "jetbotgpt"

# Load OpenAI API key from environment (.env)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY

# ---------------------------
# Create PostgreSQL engine and database
# ---------------------------
engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)
db = SQLDatabase(engine, include_tables=["students"])

# ---------------------------
# Initialize GPT with retry logic
# ---------------------------
llm = ChatOpenAI(
    temperature=0,
    model_name="gpt-3.5-turbo",
    request_timeout=60,
    max_retries=3,
)

# ---------------------------
# Create SQL Agent
# ---------------------------
toolkit = SQLDatabaseToolkit(db=db, llm=llm)
agent_executor = create_sql_agent(
    llm=llm,
    toolkit=toolkit,
    verbose=False,
    agent_type="openai-tools",
    handle_parsing_errors=True,
    max_iterations=5,
    max_execution_time=60,
    return_intermediate_steps=False,
)

# ---------------------------
# Query with retry
# ---------------------------
def query_with_retry(query, max_attempts=3, delay=2):
    for attempt in range(max_attempts):
        try:
            result = agent_executor.invoke({"input": query})
            return result['output']
        except Exception as e:
            error_msg = str(e)
            if "500" in error_msg or "server_error" in error_msg:
                if attempt < max_attempts - 1:
                    time.sleep(delay * (2 ** attempt))
                    continue
                else:
                    return "❌ OpenAI API issue. Try again later."
            elif "rate_limit" in error_msg.lower():
                if attempt < max_attempts - 1:
                    time.sleep(60)
                    continue
                else:
                    return "❌ Rate limit exceeded. Wait a minute."
            else:
                return f"❌ Error: {error_msg}"
    return "❌ Max retry attempts reached."

# ---------------------------
# Direct SQL fallback
# ---------------------------
def execute_direct_sql(query_description):
    query_map = {
        "total centres": "SELECT COUNT(DISTINCT center) as total_centers FROM students",
        "list centres": "SELECT DISTINCT center FROM students ORDER BY center",
        "all centres": "SELECT DISTINCT center FROM students ORDER BY center",
        "count students": "SELECT COUNT(*) as total_students FROM students",
        "total students": "SELECT COUNT(*) as total_students FROM students",
        "centers with outstanding": """
            SELECT center, COUNT(*) as student_count, SUM(outstanding) as total_outstanding 
            FROM students 
            WHERE outstanding > 0 
            GROUP BY center 
            ORDER BY total_outstanding DESC
        """,
    }
    query_lower = query_description.lower()
    for key, sql in query_map.items():
        if key in query_lower:
            try:
                df = pd.read_sql(sql, engine)
                return df.to_dict(orient="records")
            except Exception as e:
                return {"error": str(e)}
    return None

# ---------------------------
# Main query function
# ---------------------------
def ask_question(query):
    # Try direct SQL fallback first
    direct_result = execute_direct_sql(query)
    if direct_result:
        return {"source": "sql", "result": direct_result}

    # Else use GPT agent
    gpt_result = query_with_retry(query)
    return {"source": "gpt", "result": gpt_result}
