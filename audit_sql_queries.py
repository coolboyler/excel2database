
import pandas as pd
from database import DatabaseManager
from sqlalchemy import text
from sql_config import SQL_RULES

def audit_queries(date_str="2025-12-23"):
    db = DatabaseManager()
    table_name = f"power_data_{date_str.replace('-', '')}"
    
    print(f"=== 正在审计日期 {date_str} (表: {table_name}) 的数据查询逻辑 ===\n")
    print(f"配置文件: sql_config.py\n")
    
    # 检查表是否存在
    tables = db.get_tables()
    if table_name not in tables:
        print(f"❌ 表 {table_name} 不存在")
        return

    with db.engine.connect() as conn:
        for key, rule in SQL_RULES.items():
            name = rule["name"]
            where_clause = rule["where"]
            
            # 构造完整 SQL
            full_sql = f"SELECT record_time, value FROM {table_name} WHERE {where_clause}"
            
            print(f"🔍 [指标]: {name} ({key})")
            print(f"💻 [条件]: {where_clause}")
            
            # 执行查询 (取前5条展示)
            try:
                # 为了展示方便，我们按时间排序取前5条
                preview_sql = f"{full_sql} ORDER BY record_time ASC LIMIT 5"
                result = conn.execute(text(preview_sql)).fetchall()
                
                if result:
                    print(f"📊 [结果示例 (前5条)]:")
                    print(f"   {'时间':<15} | {'数值':<15}")
                    print("   " + "-"*30)
                    for row in result:
                        time_str = str(row[0]) # record_time
                        val = row[1]
                        print(f"   {time_str:<15} | {val:<15}")
                    
                    # 验证数据量
                    count_sql = f"SELECT COUNT(*) FROM ({full_sql}) as tmp"
                    count = conn.execute(text(count_sql)).scalar()
                    print(f"✅ [统计]: 共找到 {count} 条记录")
                else:
                    print("⚠️ [结果]: 未查询到数据")
            
            except Exception as e:
                print(f"❌ [错误]: {e}")
            
            print("\n" + "="*80 + "\n")

if __name__ == "__main__":
    audit_queries()
