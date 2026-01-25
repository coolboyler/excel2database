#!/usr/bin/env python3
"""
Copy "strategy_*" tables (used by 报价申报 / 策略复盘) from the local DB (config.py)
to the remote DB (config_remote.py).

Default behavior is to make the remote tables match local:
  - create missing tables
  - if schema differs, drop+recreate remote table
  - otherwise truncate remote table
  - copy all rows from local into remote
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import pymysql

# Ensure repo root is importable when running as a script from ./scripts.
REPO_ROOT = str(Path(__file__).resolve().parents[1])
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

try:
    from config import DB_CONFIG as LOCAL_DB_CONFIG
except Exception as e:  # pragma: no cover
    raise SystemExit(f"Failed to import local DB config from config.py: {e}")

try:
    from config_remote import DB_CONFIG as REMOTE_DB_CONFIG
except Exception as e:  # pragma: no cover
    raise SystemExit(f"Failed to import remote DB config from config_remote.py: {e}")


def _connect(cfg: Dict) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=cfg["host"],
        port=int(cfg.get("port", 3306)),
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        charset=cfg.get("charset", "utf8mb4"),
        autocommit=False,
    )


def _list_tables(conn: pymysql.connections.Connection, pattern: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES")
        rows = cur.fetchall()
    tables = [r[0] for r in rows]
    # MySQL LIKE pattern
    like = pattern.replace("%", ".*").replace("_", ".")
    rx = re.compile(rf"^{like}$")
    return sorted([t for t in tables if rx.match(t)])


def _show_create(conn: pymysql.connections.Connection, table: str) -> str | None:
    with conn.cursor() as cur:
        try:
            cur.execute(f"SHOW CREATE TABLE `{table}`")
        except Exception:
            return None
        row = cur.fetchone()
        if not row:
            return None
        return row[1]


def _truncate(conn: pymysql.connections.Connection, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE `{table}`")


def _drop(conn: pymysql.connections.Connection, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")


def _create(conn: pymysql.connections.Connection, create_sql: str) -> None:
    with conn.cursor() as cur:
        cur.execute(create_sql)


def _copy_rows(
    src: pymysql.connections.Connection,
    dst: pymysql.connections.Connection,
    table: str,
    chunk_size: int,
) -> int:
    # Unbuffered cursor to avoid loading big tables into memory.
    src_cur = src.cursor(pymysql.cursors.SSCursor)
    dst_cur = dst.cursor()
    try:
        src_cur.execute(f"SELECT * FROM `{table}`")
        cols = [d[0] for d in (src_cur.description or [])]
        if not cols:
            return 0
        placeholders = ", ".join(["%s"] * len(cols))
        col_list = ", ".join([f"`{c}`" for c in cols])
        insert_sql = f"INSERT INTO `{table}` ({col_list}) VALUES ({placeholders})"

        total = 0
        while True:
            batch = src_cur.fetchmany(chunk_size)
            if not batch:
                break
            dst_cur.executemany(insert_sql, batch)
            dst.commit()
            total += len(batch)
        return total
    finally:
        try:
            src_cur.close()
        finally:
            dst_cur.close()


def main(argv: Sequence[str]) -> int:
    ap = argparse.ArgumentParser(description="Copy strategy_* tables from local DB to remote DB.")
    ap.add_argument("--pattern", default="strategy_%", help="MySQL table name pattern (default: strategy_%%)")
    ap.add_argument("--chunk-size", type=int, default=2000, help="Insert batch size (default: 2000)")
    args = ap.parse_args(argv)

    src = _connect(LOCAL_DB_CONFIG)
    dst = _connect(REMOTE_DB_CONFIG)
    try:
        tables = _list_tables(src, args.pattern)
        if not tables:
            print(f"No tables matched pattern {args.pattern!r} in local DB.")
            return 1

        # Speed/safety knobs: we don't use foreign keys in these tables, but disable checks anyway.
        for conn in (src, dst):
            with conn.cursor() as cur:
                cur.execute("SET FOREIGN_KEY_CHECKS=0")
            conn.commit()

        print(f"Copying {len(tables)} tables: {', '.join(tables)}")
        for t in tables:
            src_create = _show_create(src, t)
            if not src_create:
                print(f"[skip] {t}: cannot read SHOW CREATE TABLE from local")
                continue

            dst_create = _show_create(dst, t)
            if dst_create is None:
                print(f"[create] {t}")
                _create(dst, src_create)
                dst.commit()
            elif dst_create.strip() != src_create.strip():
                print(f"[recreate] {t} (schema differs)")
                _drop(dst, t)
                _create(dst, src_create)
                dst.commit()
            else:
                print(f"[truncate] {t}")
                _truncate(dst, t)
                dst.commit()

            copied = _copy_rows(src, dst, t, chunk_size=args.chunk_size)
            print(f"[copied] {t}: {copied} rows")

        print("Done.")
        return 0
    finally:
        try:
            src.close()
        finally:
            dst.close()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
