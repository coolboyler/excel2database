
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
            name = rule.get("name", key)
            source = rule.get("source")

            # Weather rules don't have "where" clauses; they come from calendar_weather.
            if source != "power_data":
                print(f"🔍 [指标]: {name} ({key})")
                print(f"🧩 [来源]: {source}")
                print("ℹ️  [说明]: 非 power_data 查询规则（不涉及 sheet_name 匹配）。")
                print("\n" + "="*80 + "\n")
                continue

            where_clause = rule.get("where")
            if not where_clause:
                continue

            print(f"🔍 [指标]: {name} ({key})")
            print(f"💻 [条件]: {where_clause}")

            try:
                # 统计：总行数 + 匹配到的 sheet 数量
                stat = conn.execute(
                    text(
                        f"""
                        SELECT
                          COUNT(*) AS n,
                          COUNT(DISTINCT sheet_name) AS sheet_cnt
                        FROM {table_name}
                        WHERE {where_clause}
                        """
                    )
                ).fetchone()
                n = int(stat[0] or 0)
                sheet_cnt = int(stat[1] or 0)

                if n <= 0:
                    print("⚠️ [结果]: 未查询到数据")
                    print("\n" + "="*80 + "\n")
                    continue

                print(f"✅ [统计]: 共找到 {n} 条记录 | 匹配 sheet 数: {sheet_cnt}")

                # 展示 top sheets
                sheets = conn.execute(
                    text(
                        f"""
                        SELECT sheet_name, COUNT(*) AS c
                        FROM {table_name}
                        WHERE {where_clause}
                        GROUP BY sheet_name
                        ORDER BY c DESC
                        LIMIT 8
                        """
                    )
                ).fetchall()
                print("📄 [Top Sheets]:", [(r[0], int(r[1] or 0)) for r in sheets])
                if sheet_cnt > 1:
                    print("⚠️ [警告]: 该规则可能匹配多个 sheet（会导致 cache_daily_hourly 混合均值）。建议补充 sheet_name 过滤条件。")

                # 示例：取前 5 条
                preview = conn.execute(
                    text(
                        f"""
                        SELECT record_time, value, sheet_name
                        FROM {table_name}
                        WHERE {where_clause}
                        ORDER BY record_time ASC
                        LIMIT 5
                        """
                    )
                ).fetchall()
                print("📊 [示例(前5条)]:")
                print(f"   {'时间':<10} | {'数值':<15} | {'sheet':<30}")
                print("   " + "-" * 65)
                for rt, v, sn in preview:
                    print(f"   {str(rt):<10} | {str(v):<15} | {str(sn):<30}")
            except Exception as e:
                print(f"❌ [错误]: {e}")

            print("\n" + "="*80 + "\n")

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Audit sql_config.py rules against a specific power_data_YYYYMMDD table.")
    ap.add_argument("--date", default="2025-12-23", help="YYYY-MM-DD")
    args = ap.parse_args()
    audit_queries(args.date)
