#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Daily COS watcher + Excel2sql importer.

Goal:
- Poll Tencent COS during a daily window (default 11:10-12:00, every 60s).
- For each data type (4 prefixes), pick the newest Excel matching the target date
  (date offsets are configurable, with fallbacks).
- Download -> import into DB -> delete local file.
- After DB import, update cache table (cache_daily_hourly).
- Persist a small state file to avoid duplicate downloads/imports.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gc
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Dict, Iterable, List, Optional, Tuple

from qcloud_cos import CosConfig, CosS3Client


# Ensure local imports work when running as a script.
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

from pred_reader import PowerDataImporter  # noqa: E402


LOG = logging.getLogger("cos_daily_auto_import")


@dataclass(frozen=True)
class CosObj:
    key: str
    size: int
    etag: str
    last_modified: dt.datetime  # timezone-aware UTC


_FILENAME_DATE_RE = re.compile(
    r"(?:[（(]\s*)?"
    r"("
    r"\d{4}-\d{1,2}-\d{1,2}"
    r"|\d{4}[._]\d{1,2}[._]\d{1,2}"
    r"|\d{4}年\d{1,2}月\d{1,2}日"
    r"|\d{8}"
    r")"
    r"(?:\s*[)）])?"
)


def _load_dotenv(path: Path) -> Dict[str, str]:
    # Minimal .env parser (no external deps).
    if not path.exists():
        return {}
    out: Dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip()
        if not k:
            continue
        if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
            v = v[1:-1]
        out[k] = v
    return out


def _parse_last_modified(s: str) -> dt.datetime:
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return dt.datetime.strptime(s, fmt).replace(tzinfo=dt.timezone.utc)
        except ValueError:
            pass
    raise ValueError(f"Unrecognized LastModified format: {s!r}")


def _extract_filename_date_ymd(name: str) -> Optional[str]:
    """
    Return YYYY-MM-DD parsed from a COS key/filename, or None.
    Uses the last date-like token to handle names containing multiple dates.
    """
    s = PurePosixPath(name).as_posix()
    m = None
    for m in _FILENAME_DATE_RE.finditer(s):
        pass
    if not m:
        return None
    token = m.group(1)
    if len(token) == 8 and token.isdigit():
        token = f"{token[0:4]}-{token[4:6]}-{token[6:8]}"
    if "年" in token and "月" in token and "日" in token:
        token = token.replace("年", "-").replace("月", "-").replace("日", "")
    token = token.replace("_", "-").replace(".", "-")
    try:
        d = dt.datetime.strptime(token, "%Y-%m-%d").date()
        return d.strftime("%Y-%m-%d")
    except ValueError:
        return None


def _parse_hhmm(s: str) -> Tuple[int, int]:
    s = (s or "").strip()
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", s)
    if not m:
        raise ValueError(f"Invalid HH:MM: {s!r}")
    hh = int(m.group(1))
    mm = int(m.group(2))
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        raise ValueError(f"Invalid HH:MM: {s!r}")
    return hh, mm


def _load_json(path: Path, default):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _dump_json_atomic(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    tmp.replace(path)


def _get_cos_client(region: str, secret_id: str, secret_key: str) -> CosS3Client:
    cfg = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key)
    return CosS3Client(cfg)


def _iter_cos_objects(client: CosS3Client, bucket: str, prefix: str) -> Iterable[CosObj]:
    marker = ""
    # Normalize prefix: COS keys are posix-like; allow both with/without trailing "/".
    norm_prefix = prefix
    if norm_prefix and not norm_prefix.endswith("/"):
        norm_prefix += "/"

    while True:
        resp = client.list_objects(Bucket=bucket, Prefix=norm_prefix, Marker=marker, MaxKeys=1000)
        for obj in (resp.get("Contents") or []):
            key = str(obj.get("Key") or "")
            if not key or key.endswith("/"):
                continue
            yield CosObj(
                key=key,
                size=int(obj.get("Size", 0) or 0),
                etag=str(obj.get("ETag", "")).strip('"'),
                last_modified=_parse_last_modified(str(obj["LastModified"])),
            )
        if str(resp.get("IsTruncated", "")).lower() == "true":
            marker = str(resp.get("NextMarker") or "")
        else:
            break


def _pick_candidate_for_dates(objs: Iterable[CosObj], ymd_set: set[str]) -> Optional[CosObj]:
    best: Optional[CosObj] = None
    for o in objs:
        name = PurePosixPath(o.key).name
        if not name.lower().endswith((".xlsx", ".xls")):
            continue
        ymd = _extract_filename_date_ymd(name)
        if ymd not in ymd_set:
            continue
        if best is None or (o.last_modified, o.key) > (best.last_modified, best.key):
            best = o
    return best


def _target_dates(base_date: dt.date, offsets_priority: List[int]) -> List[str]:
    out: List[str] = []
    for off in offsets_priority:
        out.append((base_date + dt.timedelta(days=int(off))).strftime("%Y-%m-%d"))
    return out


def _import_excel_and_update_cache(importer: PowerDataImporter, filename: str, filepath: Path) -> None:
    filename = os.path.basename(filename)
    LOG.info("Importing: %s", filename)

    dated_rt = r"\d{4}-\d{2}-\d{2}实时节点电价查询"
    dated_da = r"\d{4}-\d{2}-\d{2}日前节点电价查询"

    if "负荷实际信息" in filename or "负荷预测信息" in filename:
        method = importer.import_power_data
    elif "信息披露(区域)查询实际信息" in filename or "信息披露查询实际信息" in filename:
        method = importer.import_imformation_true
    elif "信息披露(区域)查询预测信息" in filename or "信息披露查询预测信息" in filename:
        method = importer.import_imformation_pred
    elif re.search(dated_rt, filename) or re.search(dated_da, filename):
        method = importer.import_point_data_new
    elif "实时节点电价查询" in filename or "日前节点电价查询" in filename:
        method = importer.import_point_data
    else:
        raise RuntimeError(f"Unrecognized import rule for filename: {filename}")

    result = method(str(filepath))

    def _safe_int(x) -> int:
        try:
            return int(x)
        except Exception:
            return 0

    if method == importer.import_custom_excel:
        (s1, t1, c1, _), (s2, t2, c2, _), (s3, t3, c3, _) = result
        success = bool(s1)
        if success and not (s2 and s3):
            LOG.warning("Partial success (actual): %s=%s, %s=%s", t2, s2, t3, s3)
        table = f"{t1}, {t2}, {t3}"
        count = _safe_int(c1) + _safe_int(c2) + _safe_int(c3)
    elif method == importer.import_custom_excel_pred:
        (s1, t1, c1, _), (s2, t2, c2, _), (s4, t4, c4, _), (s5, t5, c5, _) = result
        success = bool(s1)
        if success and not (s2 and s4 and s5):
            LOG.warning("Partial success (forecast): %s=%s, %s=%s, %s=%s", t2, s2, t4, s4, t5, s5)
        table = f"{t1}, {t2}, {t4}, {t5}"
        count = _safe_int(c1) + _safe_int(c2) + _safe_int(c4) + _safe_int(c5)
    elif method in (importer.import_imformation_pred, importer.import_imformation_true):
        # May return a single (success, table, count, preview) or a tuple of such tuples.
        if isinstance(result, tuple) and len(result) == 4 and not isinstance(result[0], tuple):
            success, table, count, _ = result
            count = _safe_int(count)
        elif isinstance(result, tuple) and len(result) > 0 and isinstance(result[0], tuple):
            success = all(r[0] for r in result)
            table = ", ".join([str(r[1]) for r in result])
            count = sum(_safe_int(r[2]) for r in result)
        else:
            raise RuntimeError(f"Import returned unexpected format: {result}")
    else:
        success, table, count, _ = result
        count = _safe_int(count)

    if not success:
        raise RuntimeError(f"Import failed: {filename}")

    LOG.info("Import OK | table=%s | records=%s", table, count)

    # Cache table update is part of the contract for these daily files.
    date_str = _extract_filename_date_ymd(filename)
    if not date_str:
        raise RuntimeError(f"Cannot extract date from filename for cache update: {filename}")

    # Lazy import to avoid pulling FastAPI on dry runs / listing.
    from api import update_price_cache_for_date  # noqa: E402

    updated = update_price_cache_for_date(date_str)
    LOG.info("Cache updated | date=%s | rows=%s", date_str, updated)


def _ensure_safe_key(key: str) -> str:
    p = PurePosixPath(key)
    if p.is_absolute() or ".." in p.parts:
        raise ValueError(f"Unsafe COS key path: {key!r}")
    return p.as_posix()


def _download_to(client: CosS3Client, bucket: str, key: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    client.download_file(Bucket=bucket, Key=key, DestFilePath=str(dest))


def _day_state(state: dict, day_key: str) -> dict:
    days = state.setdefault("days", {})
    return days.setdefault(day_key, {"targets": {}, "total_attempts": 0})


def _should_attempt(day_state: dict, target_name: str, max_per_type: int) -> bool:
    t = day_state.get("targets", {}).get(target_name)
    if not t:
        return True
    return int(t.get("attempts", 0) or 0) < max_per_type and str(t.get("status")) != "done"


def _all_targets_done(state: dict, day_key: str, targets: dict) -> bool:
    if not targets:
        return False
    day_state = (state.get("days") or {}).get(day_key) or {}
    tmap = day_state.get("targets") or {}
    for name in targets.keys():
        if str((tmap.get(name) or {}).get("status")) != "done":
            return False
    return True


def run_once(config: dict, base_date: dt.date, dry_run: bool = False) -> int:
    """
    Returns: number of imports completed in this run (0..4).
    """
    tencent_cfg = config.get("tencent_cos") or {}
    env = dict(os.environ)
    dotenv_value = tencent_cfg.get("dotenv_path") or env.get("TENCENT_COS_DOTENV") or ""
    dotenv_path = Path(dotenv_value).expanduser() if dotenv_value else None
    if dotenv_path and dotenv_path.is_file():
        env.update(_load_dotenv(dotenv_path))

    region = (tencent_cfg.get("region") or env.get("TENCENT_COS_REGION") or env.get("COS_REGION") or "").strip()
    bucket = (tencent_cfg.get("bucket") or env.get("TENCENT_COS_BUCKET") or env.get("COS_BUCKET") or "").strip()

    secret_id = env.get("TENCENT_SECRET_ID") or env.get("SECRET_ID") or ""
    secret_key = env.get("TENCENT_SECRET_KEY") or env.get("SECRET_KEY") or ""
    if not region or not bucket:
        raise RuntimeError("Missing COS bucket/region (TENCENT_COS_BUCKET / TENCENT_COS_REGION).")
    if not secret_id or not secret_key:
        raise RuntimeError("Missing COS credentials (TENCENT_SECRET_ID / TENCENT_SECRET_KEY).")

    state_file = (_HERE / config["local"]["state_file"]).resolve()
    download_dir = (_HERE / config["local"]["download_dir"]).resolve()

    limits = config.get("limits") or {}
    max_day = int(limits.get("max_download_import_per_day", 4))
    max_per_type = int(limits.get("max_per_type_per_day", 1))

    client = _get_cos_client(region, secret_id, secret_key)
    completed_this_run = 0

    # Dry-run should never consume daily quotas / attempts: only report hits.
    if dry_run:
        targets: dict = config.get("targets") or {}
        for target_name, target_cfg in targets.items():
            prefix = str(target_cfg["prefix"])
            offsets = list(target_cfg.get("date_offsets_days_priority") or [])
            if not offsets:
                continue
            wanted_dates = _target_dates(base_date, offsets)
            wanted_set = set(wanted_dates)

            objs = list(_iter_cos_objects(client, bucket, prefix))
            candidate = _pick_candidate_for_dates(objs, wanted_set)
            if not candidate:
                LOG.info("No candidate for %s | prefix=%s | wanted=%s", target_name, prefix, wanted_dates)
                continue
            key = _ensure_safe_key(candidate.key)
            LOG.info(
                "Hit %s | key=%s | etag=%s | last_modified=%s",
                target_name,
                key,
                candidate.etag,
                candidate.last_modified.isoformat(),
            )
        return 0

    state = _load_json(state_file, default={"days": {}})
    day_key = base_date.strftime("%Y-%m-%d")
    ds = _day_state(state, day_key)

    # Stop early if the day cap has been hit.
    if int(ds.get("total_attempts", 0) or 0) >= max_day:
        LOG.info("Daily cap hit (%s/%s). Nothing to do.", ds.get("total_attempts"), max_day)
        return 0

    targets: dict = config.get("targets") or {}
    for target_name, target_cfg in targets.items():
        if int(ds.get("total_attempts", 0) or 0) >= max_day:
            break

        if not _should_attempt(ds, target_name, max_per_type):
            continue

        prefix = str(target_cfg["prefix"])
        offsets = list(target_cfg.get("date_offsets_days_priority") or [])
        if not offsets:
            LOG.warning("Target %s has empty date_offsets_days_priority; skipping.", target_name)
            continue

        wanted_dates = _target_dates(base_date, offsets)
        wanted_set = set(wanted_dates)

        candidate = _pick_candidate_for_dates(_iter_cos_objects(client, bucket, prefix), wanted_set)
        if not candidate:
            LOG.info("No candidate for %s | prefix=%s | wanted=%s", target_name, prefix, wanted_dates)
            continue

        key = _ensure_safe_key(candidate.key)
        filename = PurePosixPath(key).name
        prev = (ds.get("targets") or {}).get(target_name) or {}

        if prev and prev.get("etag") == candidate.etag and int(prev.get("attempts", 0) or 0) >= 1:
            # Avoid repeated downloads for the same object within the same day window.
            continue

        LOG.info(
            "Hit %s | key=%s | etag=%s | last_modified=%s",
            target_name,
            key,
            candidate.etag,
            candidate.last_modified.isoformat(),
        )

        ds.setdefault("targets", {}).setdefault(target_name, {})
        ds["targets"][target_name].update(
            {
                "key": key,
                "etag": candidate.etag,
                "last_modified": candidate.last_modified.isoformat(),
                "wanted_dates": wanted_dates,
                "attempted_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "attempts": int(prev.get("attempts", 0) or 0) + 1,
                "status": "attempting",
            }
        )
        ds["total_attempts"] = int(ds.get("total_attempts", 0) or 0) + 1
        _dump_json_atomic(state_file, state)

        # Download -> import -> delete local file -> update cache (part of import step).
        local_path = download_dir / f"{target_name}__{filename}"
        importer = PowerDataImporter()
        try:
            _download_to(client, bucket, key, local_path)
            _import_excel_and_update_cache(importer, filename, local_path)
            ds["targets"][target_name]["status"] = "done"
            completed_this_run += 1
        except Exception as e:
            ds["targets"][target_name]["status"] = "failed"
            ds["targets"][target_name]["error"] = str(e)
            LOG.exception("Failed %s | key=%s", target_name, key)
        finally:
            try:
                if local_path.exists():
                    local_path.unlink()
            except Exception:
                LOG.warning("Failed to delete local file: %s", str(local_path))
            try:
                importer.db_manager.engine.dispose()
            except Exception:
                pass
            del importer
            gc.collect()

        _dump_json_atomic(state_file, state)

    return completed_this_run


def run_window(config: dict, base_date: dt.date, dry_run: bool = False) -> None:
    poll = config.get("polling") or {}
    start_hhmm = poll.get("start_hhmm", "11:10")
    end_hhmm = poll.get("end_hhmm", "12:00")
    interval_seconds = int(poll.get("interval_seconds", 60))

    sh, sm = _parse_hhmm(str(start_hhmm))
    eh, em = _parse_hhmm(str(end_hhmm))
    start_dt = dt.datetime.combine(base_date, dt.time(sh, sm))
    end_dt = dt.datetime.combine(base_date, dt.time(eh, em))

    now = dt.datetime.now()
    if now < start_dt:
        sleep_s = (start_dt - now).total_seconds()
        LOG.info("Waiting for window start: %s (sleep %.1fs)", start_dt.isoformat(), sleep_s)
        time.sleep(max(0.0, sleep_s))

    day_key = base_date.strftime("%Y-%m-%d")
    targets: dict = config.get("targets") or {}

    while True:
        now = dt.datetime.now()
        if now >= end_dt:
            LOG.info("Window ended: %s", end_dt.isoformat())
            return

        t0 = time.time()
        try:
            done = run_once(config, base_date=base_date, dry_run=dry_run)
            LOG.info("Tick done | imports=%s", done)
        except Exception:
            LOG.exception("Tick failed")

        # If all targets are done, stop early.
        if not dry_run:
            state_file = (_HERE / config["local"]["state_file"]).resolve()
            state = _load_json(state_file, default={"days": {}})
            if _all_targets_done(state, day_key, targets):
                LOG.info("All targets done for %s; exiting early.", day_key)
                return

        # Sleep to next tick.
        elapsed = time.time() - t0
        sleep_s = max(1.0, float(interval_seconds) - elapsed)
        time.sleep(sleep_s)


def _setup_logging(log_level: str) -> None:
    level = getattr(logging, (log_level or "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[logging.StreamHandler()],
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="COS daily watcher + Excel import + cache update")
    ap.add_argument(
        "--config",
        default=str(_HERE / "cos_daily_import.config.json"),
        help="Path to config JSON",
    )
    ap.add_argument("--date", default="", help="Base date (YYYY-MM-DD), default: today")
    ap.add_argument("--once", action="store_true", help="Run once (ignore time window)")
    ap.add_argument("--dry-run", action="store_true", help="List hits and write state, but do not download/import")
    ap.add_argument("--log-level", default="INFO", help="DEBUG|INFO|WARNING|ERROR")
    args = ap.parse_args()

    _setup_logging(args.log_level)

    cfg_path = Path(args.config).expanduser().resolve()
    cfg = _load_json(cfg_path, default=None)
    if not isinstance(cfg, dict):
        raise SystemExit(f"Invalid config file: {str(cfg_path)}")

    if args.date.strip():
        base_date = dt.datetime.strptime(args.date.strip(), "%Y-%m-%d").date()
    else:
        base_date = dt.date.today()

    LOG.info("Base date: %s", base_date.strftime("%Y-%m-%d"))
    if args.once:
        done = run_once(cfg, base_date=base_date, dry_run=args.dry_run)
        LOG.info("Completed once | imports=%s", done)
        return 0

    run_window(cfg, base_date=base_date, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
