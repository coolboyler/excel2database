#!/usr/bin/env python3
import argparse
import datetime
import os
import re
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from pred_reader import PowerDataImporter
from database import DatabaseManager


def parse_date_list(args, db_manager):
    if args.dates:
        return [d.strip() for d in args.dates.split(",") if d.strip()]

    if args.start and args.end:
        start = datetime.datetime.strptime(args.start, "%Y-%m-%d").date()
        end = datetime.datetime.strptime(args.end, "%Y-%m-%d").date()
        if start > end:
            start, end = end, start
        dates = []
        cur = start
        while cur <= end:
            dates.append(cur.strftime("%Y-%m-%d"))
            cur += datetime.timedelta(days=1)
        return dates

    # fallback: all power_data tables
    tables = db_manager.get_tables()
    dates = []
    for t in tables:
        if not t.startswith("power_data_"):
            continue
        m = re.match(r"power_data_(\d{8})", t)
        if not m:
            continue
        ds = m.group(1)
        dates.append(f"{ds[:4]}-{ds[4:6]}-{ds[6:]}")
    dates.sort()
    return dates


def main():
    parser = argparse.ArgumentParser(description="回填节点电价城市均价记录")
    parser.add_argument("--dates", help="逗号分隔日期列表，如 2025-06-28,2025-06-29")
    parser.add_argument("--start", help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end", help="结束日期 YYYY-MM-DD")
    parser.add_argument("--city", help="只回填指定城市（可选）")
    parser.add_argument("--types", default="实时,日前", help="类型关键字，默认: 实时,日前")
    args = parser.parse_args()

    db_manager = DatabaseManager()
    importer = PowerDataImporter()

    date_list = parse_date_list(args, db_manager)
    if not date_list:
        print("未找到可处理日期")
        return

    type_keywords = [t.strip() for t in args.types.split(",") if t.strip()]
    print(f"准备处理 {len(date_list)} 天, 类型: {type_keywords}, 城市: {args.city or '全部'}")

    total_records = 0
    for d in date_list:
        for t in type_keywords:
            keyword = t if "节点电价" in t else f"{t}节点电价"
            records = importer.ensure_city_means_for_date(d, keyword, city=args.city, insert=True)
            total_records += len(records)
        print(f"✅ {d} 完成")

    print(f"完成回填, 共写入 {total_records} 条城市均价记录")


if __name__ == "__main__":
    main()
