from backend.db import get_snowflake_connection

def get_top_students(limit=5):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    query = f"""
        SELECT 
            data:"Student Name"::string AS student_name,
            TRY_CAST(data:"Net Fee"::string AS float) AS net_fee,
            TRY_CAST(data:"Paid Amount"::string AS float) AS paid_amount
        FROM RAW_EXCEL_DATA
        ORDER BY net_fee DESC
        LIMIT {limit}
    """
    cur.execute(query)
    results = cur.fetchall()
    cur.close()
    conn.close()
    return [
        {"student_name": r[0], "net_fee": r[1], "paid_amount": r[2]}
        for r in results
    ]


def get_student_by_name(name):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    query = f"""
        SELECT data
        FROM RAW_EXCEL_DATA
        WHERE LOWER(data:"Student Name"::string) = LOWER('{name}')
        LIMIT 1
    """
    cur.execute(query)
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row[0]) if row else None
