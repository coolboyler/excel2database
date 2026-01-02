from database import DatabaseManager
from sqlalchemy import text

db = DatabaseManager()

table = 'power_data_20241002'
print(f"Checking table: {table}")

with db.engine.connect() as conn:
    # Check for Load
    query_load = text(f"SELECT DISTINCT type, channel_name FROM {table} WHERE channel_name LIKE '%负荷%' OR type LIKE '%负荷%'")
    result_load = conn.execute(query_load).fetchall()
    if result_load:
        print(f"Found LOAD data: {result_load}")
    else:
        print("No LOAD data found.")

    # List all types to see what's there
    query_all = text(f"SELECT DISTINCT type FROM {table}")
    result_all = conn.execute(query_all).fetchall()
    print(f"All types: {result_all}")
