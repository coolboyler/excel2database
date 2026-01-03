import sys
import datetime
import json
import time
from sqlalchemy import text
from database import DatabaseManager
import weather

# 尝试导入 chinese_calendar，如果失败则尝试添加路径
try:
    from chinese_calendar import is_workday, is_holiday, get_holiday_detail
except ImportError:
    sys.path.append("/Users/cayron/Library/Python/3.9/lib/python/site-packages")
    try:
        from chinese_calendar import is_workday, is_holiday, get_holiday_detail
    except ImportError:
        print("❌ chinese_calendar module not found. Please run: pip install chinesecalendar")
        # 定义简单的后备逻辑或退出
        def is_workday(d): return d.weekday() < 5
        def is_holiday(d): return d.weekday() >= 5
        def get_holiday_detail(d): return (is_holiday(d), None)

def init_db():
    db = DatabaseManager()
    with db.engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS calendar_weather (
                date DATE PRIMARY KEY,
                day_type VARCHAR(20) NOT NULL,
                day_type_cn VARCHAR(20),
                holiday_name VARCHAR(50),
                max_temp FLOAT,
                min_temp FLOAT,
                weather_summary VARCHAR(50),
                weather_json JSON,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
    return db

from api import update_price_cache_for_date

def update_calendar(start_date, end_date):
    db = init_db()
    current_date = start_date
    delta = datetime.timedelta(days=1)
    
    print(f"🚀 Starting Calendar Initialization from {start_date} to {end_date}")
    total_days = (end_date - start_date).days + 1
    processed = 0
    
    with db.engine.connect() as conn:
        while current_date <= end_date:
            # 1. 确定日期类型 (工作日/节假日)
            try:
                is_hol = is_holiday(current_date)
                hol_detail = get_holiday_detail(current_date)
            except NotImplementedError:
                is_weekend = current_date.weekday() >= 5
                is_hol = is_weekend
                hol_detail = (is_hol, None)
            
            day_type = "workday"
            day_type_cn = "工作日"
            holiday_name = None
            if is_hol:
                if hol_detail and hol_detail[1]:
                    day_type = "holiday"
                    day_type_cn = "节假日"
                    holiday_name = hol_detail[1]
                else:
                    day_type = "weekend"
                    day_type_cn = "周末"

            # 2. 检查是否需要获取天气
            row = conn.execute(text("SELECT weather_json FROM calendar_weather WHERE date = :d"), {"d": current_date}).fetchone()
            
            weather_data = None
            should_fetch = True
            
            if row and row[0]: 
                try:
                    existing_json = json.loads(row[0])
                    # 检查新字段是否存在
                    if "apparent_temps" in existing_json and "wind_speeds" in existing_json:
                        should_fetch = False
                except:
                    should_fetch = True
            
            # 不获取太远的未来数据
            today = datetime.date.today()
            if current_date > today + datetime.timedelta(days=20):
                should_fetch = False

            if should_fetch:
                weather_data = weather.fetch_weather_for_date(current_date)
                if current_date < today: time.sleep(0.05)
            
            # 3. 插入/更新数据库
            sql = text("""
                INSERT INTO calendar_weather (date, day_type, day_type_cn, holiday_name, max_temp, min_temp, weather_summary, weather_json)
                VALUES (:date, :day_type, :day_type_cn, :holiday_name, :max_temp, :min_temp, :weather_summary, :weather_json)
                ON DUPLICATE KEY UPDATE
                    day_type = VALUES(day_type),
                    day_type_cn = VALUES(day_type_cn),
                    holiday_name = VALUES(holiday_name),
                    max_temp = IF(VALUES(max_temp) IS NOT NULL, VALUES(max_temp), max_temp),
                    min_temp = IF(VALUES(min_temp) IS NOT NULL, VALUES(min_temp), min_temp),
                    weather_summary = IF(VALUES(weather_summary) IS NOT NULL, VALUES(weather_summary), weather_summary),
                    weather_json = IF(VALUES(weather_json) IS NOT NULL, VALUES(weather_json), weather_json)
            """)
            
            params = {
                "date": current_date,
                "day_type": day_type,
                "day_type_cn": day_type_cn,
                "holiday_name": holiday_name,
                "max_temp": weather_data['max_temp'] if weather_data else None,
                "min_temp": weather_data['min_temp'] if weather_data else None,
                "weather_summary": weather_data['weather_type'] if weather_data else None,
                "weather_json": json.dumps(weather_data) if weather_data else None
            }
            
            conn.execute(sql, params)
            conn.commit()
            
            # [新增逻辑] 同步更新缓存表 cache_daily_hourly
            # 因为天气数据可能会更新，或者节假日类型会更新，这些都在 sql_config 里被用到了
            try:
                date_str = current_date.strftime("%Y-%m-%d")
                # 仅更新天气字段，避免覆盖已有的电力数据或触发耗时的电力查询
                update_price_cache_for_date(date_str, only_weather=True)
            except Exception as e:
                print(f"⚠️ 缓存同步失败 ({current_date}): {e}")
            
            processed += 1
            if processed % 50 == 0:
                status = "Existing"
                if weather_data:
                    status = weather_data.get('source', 'Fetched')
                print(f"Progress: {processed}/{total_days} ({current_date}) - {status}")
            
            current_date += delta

    print("✅ Calendar Initialization Complete!")
