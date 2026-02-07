#!/usr/bin/env python3
"""
Backfill Guangdong hourly DA/RT node price curve cache into `gd_price_hourly_cache`.

This makes /gd_city_price date switching fast because the page can read 24 rows/day instead
of scanning large `power_data_YYYYMMDD` tables on first access.
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

from config import DB_CONFIG  # noqa: E402


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


def _ensure_curve_table(conn, table: str):
    conn.execute(
        text(
            f"""
            CREATE TABLE IF NOT EXISTS `{table}` (
                `record_date` DATE NOT NULL,
                `hour` TINYINT NOT NULL,
                `price_da` FLOAT NULL,
                `price_rt` FLOAT NULL,
                `price_diff` FLOAT NULL,
                `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`record_date`, `hour`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
    )


def _already_cached(conn, table: str, date_str: str) -> bool:
    try:
        n = conn.execute(
            text(f"SELECT COUNT(*) FROM `{table}` WHERE record_date=:d"),
            {"d": date_str},
        ).scalar()
        return int(n or 0) >= 24
    except Exception:
        return False


def _has_gd_node_price(conn, power_table: str) -> bool:
    try:
        one = conn.execute(
            text(
                f"""
                SELECT 1
                FROM {power_table}
                WHERE type LIKE '%广东%' AND type LIKE '%节点电价%'
                  AND (type LIKE '%日前%' OR type LIKE '%实时%')
                LIMIT 1
                """
            )
        ).fetchone()
        return bool(one)
    except Exception:
        return False


def _compute_hourly_avg(conn, power_table: str, kind: str) -> Dict[int, float]:
    # kind: "da" or "rt"
    like = "%日前%" if kind == "da" else "%实时%"
    rows = conn.execute(
        text(
            f"""
            SELECT HOUR(record_time) AS hour, AVG(value) AS v
            FROM {power_table}
            WHERE type LIKE '%广东%' AND type LIKE '%节点电价%' AND type LIKE :like
              AND channel_name NOT LIKE '%均值%'
              AND channel_name NOT LIKE '%节点均价%'
              AND channel_name <> '节点电价'
              AND record_time IS NOT NULL
            GROUP BY HOUR(record_time)
            """
        ),
        {"like": like},
    ).fetchall()
    out: Dict[int, float] = {}
    for h, v in rows:
        try:
            hh = int(h)
        except Exception:
            continue
        if 0 <= hh <= 23 and v is not None:
            out[hh] = float(v)
    return out


def _upsert(conn, table: str, date_str: str, da: Dict[int, float], rt: Dict[int, float]):
    payload = []
    for h in range(24):
        v_da = da.get(h)
        v_rt = rt.get(h)
        diff = (v_da - v_rt) if (v_da is not None and v_rt is not None) else None
        payload.append(
            {"record_date": date_str, "hour": h, "price_da": v_da, "price_rt": v_rt, "price_diff": diff}
        )

    conn.execute(
        text(
            f"""
            INSERT INTO `{table}`
              (`record_date`, `hour`, `price_da`, `price_rt`, `price_diff`)
            VALUES
              (:record_date, :hour, :price_da, :price_rt, :price_diff)
            ON DUPLICATE KEY UPDATE
              `price_da`=VALUES(`price_da`),
              `price_rt`=VALUES(`price_rt`),
              `price_diff`=VALUES(`price_diff`)
            """
        ),
        payload,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache-table", default="gd_price_hourly_cache")
    ap.add_argument("--start", default=None, help="Start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", default=None, help="End date YYYY-MM-DD (inclusive)")
    ap.add_argument("--limit", type=int, default=0, help="Limit days processed (0 = no limit)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

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
        _ensure_curve_table(conn, args.cache_table)

        tables = [r[0] for r in conn.execute(text("SHOW TABLES")).fetchall()]
        power_tables: List[Tuple[dt.date, str]] = []
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

            if _already_cached(conn, args.cache_table, date_str):
                skipped += 1
                continue

            if not _has_gd_node_price(conn, t):
                skipped += 1
                continue

            day_t0 = time.time()
            if args.dry_run:
                filled += 1
                print(f"[dry] {date_str} <- {t} | {time.time() - day_t0:.2f}s")
                continue

            da = _compute_hourly_avg(conn, t, "da")
            rt = _compute_hourly_avg(conn, t, "rt")
            _upsert(conn, args.cache_table, date_str, da, rt)
            filled += 1

            if filled % 20 == 0:
                elapsed = time.time() - t0
                print(f"filled={filled} processed={processed} skipped={skipped} elapsed={elapsed:.1f}s (last {date_str} in {time.time() - day_t0:.2f}s)")

    elapsed = time.time() - t0
    print(f"Done. filled={filled} processed={processed} skipped={skipped} elapsed={elapsed:.1f}s cache_table={args.cache_table}")


if __name__ == "__main__":
    main()

