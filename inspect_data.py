from database import DatabaseManager
from sqlalchemy import text

db = DatabaseManager()
tables = db.get_tables()

print(f"Tables: {tables}")

# Check power_actual
if 'power_actual' in tables:
    print("\nFound power_actual table.")
    with db.engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM power_actual LIMIT 5")).fetchall()
        print(f"Sample data from power_actual: {result}")
        
        # Check date range
        min_date = conn.execute(text("SELECT MIN(data_date) FROM power_actual")).scalar()
        max_date = conn.execute(text("SELECT MAX(data_date) FROM power_actual")).scalar()
        print(f"Date range: {min_date} to {max_date}")
else:
    print("\npower_actual table NOT found.")

# Check power_forecast
if 'power_forecast' in tables:
    print("\nFound power_forecast table.")
    with db.engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM power_forecast LIMIT 5")).fetchall()
        print(f"Sample data from power_forecast: {result}")
else:
    print("\npower_forecast table NOT found.")
