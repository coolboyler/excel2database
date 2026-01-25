#!/usr/bin/env python3
"""
Upsert strategy actual hourly energy into MySQL.

Default: applies the Dec-2025 fixes from the latest chat (12/03, 12/05, 12/09).
You can also provide a JSON file via --input.

JSON format:
[
  {"date": "2025-12-03", "source": "both", "hourly": [24 floats]},
  ...
]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, List

# Reuse the existing DB logic (table creation + metric refresh) from the API module.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from api import _parse_iso_date, _upsert_actual_hourly, _upsert_settlement_actual_hourly  # noqa: E402


DEFAULT_PAYLOAD: List[Dict[str, Any]] = [
    {
        "date": "2025-12-03",
        "source": "both",
        "hourly": [
            1.6391,
            1.66307,
            1.65322,
            1.70349,
            1.66121,
            1.75384,
            1.6611,
            2.09351,
            4.0232,
            4.19339,
            3.60673,
            3.58261,
            2.91108,
            3.26399,
            3.87847,
            3.69217,
            4.02322,
            3.69832,
            3.28261,
            3.48528,
            3.10678,
            2.51995,
            2.16188,
            1.74862,
        ],
    },
    {
        "date": "2025-12-05",
        "source": "both",
        "hourly": [
            1.65806,
            1.56499,
            1.55165,
            1.5242,
            1.53312,
            1.51813,
            1.45021,
            1.89835,
            3.78701,
            3.97938,
            3.29429,
            3.2182,
            2.72274,
            3.20122,
            3.70795,
            3.64728,
            3.71306,
            3.71186,
            3.35102,
            3.42924,
            3.11611,
            2.59556,
            2.00454,
            1.74809,
        ],
    },
    {
        "date": "2025-12-09",
        "source": "both",
        "hourly": [
            1.59225,
            1.65606,
            1.59058,
            1.60096,
            1.61951,
            1.65279,
            1.55464,
            1.95728,
            3.84904,
            4.08901,
            3.41101,
            3.15164,
            2.72426,
            3.20227,
            3.708,
            3.60096,
            3.69719,
            3.53613,
            3.25101,
            3.3566,
            3.01253,
            2.49268,
            1.99291,
            1.669,
        ],
    },
]


def _load_payload(path: str | None) -> List[Dict[str, Any]]:
    if not path:
        return DEFAULT_PAYLOAD
    if path == "-":
        raw = sys.stdin.read()
    else:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()
    obj = json.loads(raw)
    if not isinstance(obj, list):
        raise SystemExit("Input JSON must be a list")
    return obj  # type: ignore[return-value]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input",
        default=None,
        help="JSON file path (or '-' for stdin). If omitted, applies the built-in Dec-2025 fixes.",
    )
    args = ap.parse_args()

    payload = _load_payload(args.input)
    total_actual = 0
    total_settlement = 0

    for item in payload:
        if not isinstance(item, dict):
            raise SystemExit("Each payload item must be an object")
        date_s = str(item.get("date") or "").strip()
        if not date_s:
            raise SystemExit("Missing 'date'")
        source = str(item.get("source") or "both").strip()
        hourly = item.get("hourly")
        if not isinstance(hourly, list) or len(hourly) != 24:
            raise SystemExit(f"{date_s}: 'hourly' must be a 24-length list")

        d = _parse_iso_date(date_s)
        rows = []
        for h, v in enumerate(hourly):
            try:
                fv = float(v)
            except Exception:
                continue
            rows.append({"record_date": d, "hour": int(h), "actual_energy": float(fv)})

        if source in ("actual", "both"):
            total_actual += _upsert_actual_hourly(rows)
        if source in ("settlement", "both"):
            total_settlement += _upsert_settlement_actual_hourly(rows)

        print(f"{date_s}: upserted actual={len(rows) if source in ('actual','both') else 0}, settlement={len(rows) if source in ('settlement','both') else 0}")

    print(f"done: inserted actual={total_actual}, settlement={total_settlement}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
