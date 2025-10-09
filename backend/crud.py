# from backend.db import get_snowflake_connection

# def get_top_students(limit=5):
#     conn = get_snowflake_connection()
#     cur = conn.cursor()
#     query = f"""
#         SELECT 
#             data:"Student Name"::string AS student_name,
#             TRY_CAST(data:"Net Fee"::string AS float) AS net_fee,
#             TRY_CAST(data:"Paid Amount"::string AS float) AS paid_amount
#         FROM RAW_EXCEL_DATA
#         ORDER BY net_fee DESC
#         LIMIT {limit}
#     """
#     cur.execute(query)
#     results = cur.fetchall()
#     cur.close()
#     conn.close()
#     return [
#         {"student_name": r[0], "net_fee": r[1], "paid_amount": r[2]}
#         for r in results
#     ]


# def get_student_by_name(name):
#     conn = get_snowflake_connection()
#     cur = conn.cursor()
#     query = f"""
#         SELECT data
#         FROM RAW_EXCEL_DATA
#         WHERE LOWER(data:"Student Name"::string) = LOWER('{name}')
#         LIMIT 1
#     """
#     cur.execute(query)
#     row = cur.fetchone()
#     cur.close()
#     conn.close()
#     return dict(row[0]) if row else None




from backend.db import db

def get_top_students(limit: int = 5):
    query = f"""
    WITH student_fees AS (
        SELECT 
            ID,
            MAX(CASE WHEN COLUMN_NAME = 'Name' THEN VALUE END) AS name,
            TRY_CAST(MAX(CASE WHEN COLUMN_NAME = 'Net Fee' THEN VALUE END) AS NUMBER) AS net_fee
        FROM FLATTENED_STUDENTS
        GROUP BY ID
    )
    SELECT name, net_fee
    FROM student_fees
    WHERE net_fee IS NOT NULL
    ORDER BY net_fee DESC
    LIMIT {limit}
    """
    return db.run(query)

def get_student_by_name(name: str):
    query = f"""
    SELECT 
        ID,
        MAX(CASE WHEN COLUMN_NAME = 'Name' THEN VALUE END) AS name,
        MAX(CASE WHEN COLUMN_NAME = 'Net Fee' THEN VALUE END) AS net_fee,
        MAX(CASE WHEN COLUMN_NAME = 'Email ID' THEN VALUE END) AS email
    FROM FLATTENED_STUDENTS
    WHERE ID IN (
        SELECT ID FROM FLATTENED_STUDENTS
        WHERE COLUMN_NAME = 'Name' AND UPPER(VALUE) LIKE '%{name.upper()}%'
    )
    GROUP BY ID
    """
    result = db.run(query)
    return result if result else None
