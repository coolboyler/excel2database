#!/usr/bin/env python3
"""
Backfill Guangdong per-city real-time node price daily means into `gd_city_rt_price_daily`.

Why:
- The UI endpoint /api/gd-city-price computes city aggregates from raw `power_data_YYYYMMDD` tables
  and caches them per day. For historical dates this can be slow the first time.
- This script bulk-fills the cache table from existing DB data so the page is fast immediately.

Notes:
- We derive city purely by the node name prefix (e.g. '广州xxx' -> '广州'). This matches the most
  common naming convention for Guangdong node exports and is fast to compute in SQL.
- Daily mean is computed as AVG(per-hour AVG(node prices)), so each hour is equally weighted.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time
from typing import Dict, List, Tuple

from sqlalchemy import create_engine, text

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BASE_DIR not in sys.path:
    sys.path.insert(0, _BASE_DIR)

from config import DB_CONFIG
from pred_reader import PowerDataImporter


def _engine():
    conn_str = "mysql+pymysql://%s:%s@%s:%s/%s" % (
        DB_CONFIG.get("user"),
        DB_CONFIG.get("password"),
        DB_CONFIG.get("host"),
        DB_CONFIG.get("port"),
        DB_CONFIG.get("database"),
    )
    return create_engine(conn_str, pool_pre_ping=True, pool_recycle=1800)


def _parse_ymd_from_table(name: str) -> dt.date | None:
    if not name.startswith("power_data_"):
        return None
    s = name.replace("power_data_", "").strip()
    if len(s) != 8 or not s.isdigit():
        return None
    try:
        return dt.date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
    except Exception:
        return None


def _ensure_cache_table(conn, table: str):
    conn.execute(
        text(
            f"""
            CREATE TABLE IF NOT EXISTS `{table}` (
                `record_date` DATE NOT NULL,
                `city` VARCHAR(50) NOT NULL,
                `rt_daily_mean` FLOAT NULL,
                `hours_with_data` TINYINT NULL,
                `raw_rows` INT NULL,
                `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`record_date`, `city`),
                KEY `idx_city` (`city`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
    )


def _existing_nonzero_city_rows(conn, cache_table: str, date_str: str) -> int:
    try:
        n = conn.execute(
            text(
                f"""
                SELECT COUNT(*) AS n
                FROM `{cache_table}`
                WHERE record_date = :d
                  AND (raw_rows IS NOT NULL AND raw_rows > 0)
                """
            ),
            {"d": date_str},
        ).scalar()
        return int(n or 0)
    except Exception:
        return 0


def _table_row_estimate(conn, table: str) -> int:
    # InnoDB "Rows" is an estimate, but good enough to skip tiny tables quickly.
    try:
        row = (
            conn.execute(text("SHOW TABLE STATUS LIKE :t"), {"t": table})
            .mappings()
            .fetchone()
        )
        if not row:
            return 0
        return int(row.get("Rows") or 0)
    except Exception:
        return 0


def _has_rt_node_price(conn, power_table: str) -> bool:
    try:
        one = conn.execute(
            text(
                f"""
                SELECT 1
                FROM {power_table}
                WHERE (type LIKE '%节点电价%' AND type LIKE '%实时%')
                LIMIT 1
                """
            )
        ).fetchone()
        return bool(one)
    except Exception:
        return False


def _build_case_city_expr(cities: List[str]) -> str:
    parts = ["CASE"]
    for c in cities:
        # city names are fixed literals from code; safe to inline.
        parts.append(f"WHEN channel_name LIKE '{c}%' THEN '{c}'")
    parts.append("ELSE NULL END")
    return "\n".join(parts)


def _compute_city_daily(conn, power_table: str, cities: List[str]) -> Dict[str, Tuple[float | None, int, int]]:
    """
    Returns:
      city -> (rt_daily_mean, hours_with_data, raw_rows)
    """
    case_city = _build_case_city_expr(cities)

    # Hourly avg per city/hour, then daily mean = AVG(hour_avg).
    # We also keep `cnt` to calculate raw rows.
    sql = text(
        f"""
        SELECT city,
               AVG(hour_avg) AS rt_daily_mean,
               COUNT(*) AS hours_with_data,
               SUM(cnt) AS raw_rows
        FROM (
            SELECT
                {case_city} AS city,
                HOUR(record_time) AS hour,
                AVG(value) AS hour_avg,
                COUNT(*) AS cnt
            FROM {power_table}
            WHERE (type LIKE '%节点电价%' AND type LIKE '%实时%')
              AND channel_name NOT LIKE '%均值%'
              AND channel_name NOT LIKE '%节点均价%'
              AND channel_name <> '节点电价'
              AND record_time IS NOT NULL
            GROUP BY city, hour
        ) t
        WHERE city IS NOT NULL
        GROUP BY city
        """
    )

    out: Dict[str, Tuple[float | None, int, int]] = {}
    rows = conn.execute(sql).mappings().fetchall()
    for r in rows:
        c = r.get("city")
        if not c:
            continue
        mean = float(r["rt_daily_mean"]) if r.get("rt_daily_mean") is not None else None
        hours = int(r.get("hours_with_data") or 0)
        raw = int(r.get("raw_rows") or 0)
        out[str(c)] = (mean, hours, raw)
    return out


def _upsert_city_daily(conn, cache_table: str, date_str: str, cities: List[str], computed: Dict[str, Tuple[float | None, int, int]]):
    rows = []
    for c in cities:
        mean, hours, raw = computed.get(c, (None, 0, 0))
        rows.append(
            {
                "record_date": date_str,
                "city": c,
                "rt_daily_mean": mean,
                "hours_with_data": int(hours),
                "raw_rows": int(raw),
            }
        )

    conn.execute(
        text(
            f"""
            INSERT INTO `{cache_table}`
              (`record_date`, `city`, `rt_daily_mean`, `hours_with_data`, `raw_rows`)
            VALUES
              (:record_date, :city, :rt_daily_mean, :hours_with_data, :raw_rows)
            ON DUPLICATE KEY UPDATE
              `rt_daily_mean`=VALUES(`rt_daily_mean`),
              `hours_with_data`=VALUES(`hours_with_data`),
              `raw_rows`=VALUES(`raw_rows`)
            """
        ),
        rows,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache-table", default="gd_city_rt_price_daily")
    ap.add_argument("--min-rows-est", type=int, default=5000, help="Skip power_data tables with estimated rows below this threshold.")
    ap.add_argument("--start", default=None, help="Start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", default=None, help="End date YYYY-MM-DD (inclusive)")
    ap.add_argument("--limit", type=int, default=0, help="Limit number of days to process (0 = no limit)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    imp = PowerDataImporter()
    cities = list(getattr(imp, "_CITY_LIST_GD", []) or [])
    if not cities:
        raise SystemExit("No Guangdong city list found in PowerDataImporter._CITY_LIST_GD")

    start_d = dt.date.min
    end_d = dt.date.max
    if args.start:
        start_d = dt.date.fromisoformat(args.start)
    if args.end:
        end_d = dt.date.fromisoformat(args.end)

    eng = _engine()
    t0 = time.time()
    processed = 0
    filled = 0
    skipped = 0

    with eng.begin() as conn:
        _ensure_cache_table(conn, args.cache_table)

        tables = [r[0] for r in conn.execute(text("SHOW TABLES")).fetchall()]
        power_tables = []
        for t in tables:
            d = _parse_ymd_from_table(t)
            if not d:
                continue
            if d < start_d or d > end_d:
                continue
            power_tables.append((d, t))
        power_tables.sort(key=lambda x: x[0])

        print("Candidates:", len(power_tables), "| range:", (power_tables[0][0] if power_tables else None), "->", (power_tables[-1][0] if power_tables else None))

        for d, t in power_tables:
            if args.limit and processed >= args.limit:
                break
            processed += 1
            date_str = d.strftime("%Y-%m-%d")

            # Already filled?
            if _existing_nonzero_city_rows(conn, args.cache_table, date_str) >= max(1, int(len(cities) * 0.8)):
                skipped += 1
                continue

            rows_est = _table_row_estimate(conn, t)
            if rows_est and rows_est < int(args.min_rows_est):
                skipped += 1
                continue

            if not _has_rt_node_price(conn, t):
                skipped += 1
                continue

            day_t0 = time.time()
            computed = _compute_city_daily(conn, t, cities)
            if not computed:
                skipped += 1
                continue

            if args.dry_run:
                filled += 1
                print(f"[dry] {date_str} <- {t} | cities={len(computed)} | rows_est={rows_est} | {time.time() - day_t0:.2f}s")
                continue

            _upsert_city_daily(conn, args.cache_table, date_str, cities, computed)
            filled += 1

            if filled % 10 == 0:
                elapsed = time.time() - t0
                print(f"filled={filled} processed={processed} skipped={skipped} elapsed={elapsed:.1f}s (last {date_str} in {time.time() - day_t0:.2f}s)")

    elapsed = time.time() - t0
    print(f"Done. filled={filled} processed={processed} skipped={skipped} elapsed={elapsed:.1f}s cache_table={args.cache_table}")


if __name__ == "__main__":
    main()
