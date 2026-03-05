#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Chicago 311 extractor for Socrata SODA3 CSV endpoint (BigQuery/GCS friendly)

Modes:
- full: windowed by created_date (start/end + window_days)
- incremental: pulls changes by last_modified_date > watermark (with optional overlap)

Common behavior:
- Pages via $limit=1000 and $offset increments
- Stable ordering:
    * full: created_date, sr_number
    * incremental: last_modified_date, sr_number
- Writes to GCS with _manifest.json + _SUCCESS markers
- Optional Hive-style daily partitioning producing exactly one data.csv(.gz) per day
"""

import argparse
import csv
import io
import json
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import List, Optional, Tuple

import requests
from google.cloud import storage


DEFAULT_DATASET_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.csv"
DEFAULT_START_DATE = "2018-12-18"
DEFAULT_END_DATE = "2026-02-10"


@dataclass
class Window:
    start: datetime  # inclusive (UTC)
    end: datetime    # exclusive (UTC)

    @property
    def start_ymd(self) -> str:
        return self.start.date().isoformat()

    @property
    def end_ymd(self) -> str:
        return self.end.date().isoformat()


def parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def parse_watermark(s: str) -> datetime:
    """
    Parse watermark into a timezone-aware UTC datetime.
    Accepts:
      - ISO8601 with 'Z' (e.g. 2026-02-10T16:00:00.000Z)
      - ISO8601 without timezone (treated as UTC)
      - YYYY-MM-DD (treated as midnight UTC)
    """
    s = s.strip()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        d = parse_ymd(s)
        return datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc)

    # Normalize trailing Z to +00:00 for fromisoformat
    if s.endswith("Z"):
        s2 = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s2)
            return dt.astimezone(timezone.utc)
        except ValueError:
            pass

    # Try fromisoformat directly
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def utc_midnight(ymd: str) -> datetime:
    d = parse_ymd(ymd)
    return datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc)


def iso_utc(dt: datetime) -> str:
    """Socrata where-clause: safest to use ISO 8601 in UTC."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

def soql_floating_ts(dt: datetime) -> str:
    """
    Socrata 'floating_timestamp' literal:
    - no timezone suffix (no 'Z', no offset)
    - ISO format with milliseconds
    """
    if dt.tzinfo is not None:
        # Convert to UTC then drop tzinfo; floating timestamp is "naive"
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    # Ensure milliseconds (Socrata likes .SSS)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

def build_windows(start_ymd: str, end_ymd: str, window_days: int) -> List[Window]:
    start_d = parse_ymd(start_ymd)
    end_d = parse_ymd(end_ymd)
    if end_d <= start_d:
        raise ValueError("end-date must be after start-date")

    windows: List[Window] = []
    cur = datetime.combine(start_d, datetime.min.time(), tzinfo=timezone.utc)
    end_dt = datetime.combine(end_d, datetime.min.time(), tzinfo=timezone.utc)

    step = timedelta(days=window_days)
    while cur < end_dt:
        nxt = min(cur + step, end_dt)
        windows.append(Window(start=cur, end=nxt))
        cur = nxt
    return windows


def count_csv_rows(csv_bytes: bytes) -> int:
    """Counts data rows in CSV bytes (excluding header)."""
    text = csv_bytes.decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(text))
    n = 0
    header_seen = False
    for _ in reader:
        if not header_seen:
            header_seen = True
            continue
        n += 1
    return n



def build_query_params(
    *,
    mode: str,
    window: Window,
    limit: int,
    offset: int,
    watermark: Optional[datetime] = None,
    overlap_hours: int = 24,
) -> dict:
    """
    Build Socrata query params for full or incremental mode.

    Notes:
    - created_date/last_modified_date are Socrata floating_timestamp fields in this dataset,
      so we use floating timestamp literals (no timezone suffix).
    - Incremental uses last_modified_date > (watermark - overlap).
    """
    if mode == "full":
        start = soql_floating_ts(window.start)
        end = soql_floating_ts(window.end)
        where = f"created_date >= '{start}' AND created_date < '{end}'"
        order = "created_date, sr_number"
    else:
        if watermark is None:
            raise ValueError("incremental mode requires watermark")
        start_dt = watermark - timedelta(hours=overlap_hours)
        start = soql_floating_ts(start_dt)
        end = soql_floating_ts(window.end)  # use window.end as an upper bound (run-time bound)
        # Use strict '>' on watermark boundary; overlap handles safety.
        where = f"last_modified_date >= '{start}' AND last_modified_date < '{end}'"
        order = "last_modified_date, sr_number"

    return {
        "$where": where,
        "$order": order,
        "$limit": str(limit),
        "$offset": str(offset),
    }


def request_with_retry(
    session: requests.Session,
    url: str,
    headers: dict,
    params: dict,
    timeout: Tuple[int, int],
    max_retries: int,
    base_backoff: float,
) -> bytes:
    retryable = {429, 500, 502, 503, 504}
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = session.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp.content

            if resp.status_code in retryable and attempt <= max_retries:
                # Exponential backoff with jitter
                sleep_s = base_backoff * (2 ** (attempt - 1))
                sleep_s = sleep_s * (0.7 + random.random() * 0.6)
                print(f"[warn] HTTP {resp.status_code}. retry {attempt}/{max_retries} in {sleep_s:.2f}s")
                time.sleep(sleep_s)
                continue

            # Non-retryable or out of retries
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:500]}")
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt <= max_retries:
                sleep_s = base_backoff * (2 ** (attempt - 1))
                sleep_s = sleep_s * (0.7 + random.random() * 0.6)
                print(f"[warn] network error: {e}. retry {attempt}/{max_retries} in {sleep_s:.2f}s")
                time.sleep(sleep_s)
                continue
            raise


def gcs_path_prefix(base_prefix: str, mode: str, window: Window, daily_hive: bool) -> str:
    # Base path
    base = f"{base_prefix}/mode={mode}"

    # Optional Hive-style daily partitioning. This is the simplest way to make BigQuery/Spark
    # external tables work with a single wildcard (uris=['gs://.../mode=full/*']).
    if daily_hive:
        # For full we partition by created_date day; for incremental we partition by run date (window.start_ymd).
        partition_key = "created_date" if mode == "full" else "run_date"
        return f"{base}/{partition_key}={window.start_ymd}"

    return base


def upload_bytes_to_gcs(client: storage.Client, bucket: str, blob_path: str, data: bytes, content_type: str) -> None:
    b = client.bucket(bucket)
    blob = b.blob(blob_path)
    blob.upload_from_string(data, content_type=content_type)


def blob_exists(client: storage.Client, bucket: str, blob_path: str) -> bool:
    b = client.bucket(bucket)
    return b.blob(blob_path).exists(client)


def delete_prefix(client: storage.Client, bucket: str, prefix: str) -> None:
    b = client.bucket(bucket)
    blobs = list(client.list_blobs(bucket, prefix=prefix))
    for bl in blobs:
        bl.delete()


def run_window(
    *,
    session: requests.Session,
    gcs: storage.Client,
    dataset_url: str,
    headers: dict,
    bucket: str,
    base_prefix: str,
    mode: str,
    daily_hive: bool,
    watermark: Optional[datetime],
    overlap_hours: int,
    window: Window,
    limit: int,
    max_pages_per_file: int,
    gzip_output: bool,
    overwrite_window: bool,
    timeout: Tuple[int, int],
    max_retries: int,
    base_backoff: float,
    polite_sleep: float,
) -> dict:
    prefix = gcs_path_prefix(base_prefix, mode, window, daily_hive)
    success_blob = f"{prefix}/_SUCCESS"
    manifest_blob = f"{prefix}/_manifest.json"

    if daily_hive and window.start.date() + timedelta(days=1) != window.end.date():
        raise ValueError("--daily-hive requires --window-days 1 so each window maps to exactly one day.")

    if blob_exists(gcs, bucket, success_blob) and not overwrite_window:
        print(f"[skip] window {window.start_ymd}..{window.end_ymd} already successful.")
        return {"skipped": True, "window_start": window.start_ymd, "window_end": window.end_ymd}

    # If partial data exists without _SUCCESS, either overwrite or clean
    if overwrite_window:
        print(f"[info] overwrite enabled -> cleaning prefix: {prefix}/")
        delete_prefix(gcs, bucket, prefix=f"{prefix}/")
    else:
        # If manifest exists but no _SUCCESS, treat as partial and clean for safety
        if blob_exists(gcs, bucket, manifest_blob):
            print(f"[warn] partial window detected (manifest exists, no _SUCCESS). cleaning prefix: {prefix}/")
            delete_prefix(gcs, bucket, prefix=f"{prefix}/")

    part_files: List[str] = []
    total_rows = 0
    total_pages = 0

    offset = 0
    part_index = 1
    pages_in_current_file = 0
    buffer = bytearray()
    header_line: Optional[bytes] = None

    def flush_part() -> None:
        nonlocal buffer, part_index, pages_in_current_file
        if not buffer:
            return
        if daily_hive:
            filename = f"data.csv" + (".gz" if gzip_output else "")
        else:
            filename = f"part-{part_index:06d}.csv" + (".gz" if gzip_output else "")
        blob_path = f"{prefix}/{filename}"
        data_bytes = bytes(buffer)

        if gzip_output:
            import gzip
            data_bytes = gzip.compress(data_bytes)

        upload_bytes_to_gcs(
            gcs,
            bucket,
            blob_path,
            data_bytes,
            content_type="application/gzip" if gzip_output else "text/csv",
        )
        part_files.append(blob_path)
        print(f"[gcs] uploaded {blob_path} ({len(data_bytes)/1024/1024:.2f} MB)")
        if not daily_hive:
            part_index += 1
        pages_in_current_file = 0
        buffer = bytearray()

    while True:
        params = build_query_params(
            mode=mode,
            window=window,
            limit=limit,
            offset=offset,
            watermark=watermark,
            overlap_hours=overlap_hours,
        )
        csv_bytes = request_with_retry(
            session=session,
            url=dataset_url,
            headers=headers,
            params=params,
            timeout=timeout,
            max_retries=max_retries,
            base_backoff=base_backoff,
        )

        # Socrata returns a header row always; count data rows
        rows = count_csv_rows(csv_bytes)
        total_pages += 1
        total_rows += rows

        if rows == 0:
            # Window finished (or empty)
            print(f"[info] window {window.start_ymd}..{window.end_ymd}: no more rows at offset={offset}.")
            break

        # Merge pages to reduce tiny files:
        # Keep only the first page's header; strip headers from subsequent pages.
        if header_line is None:
            # first page: store full bytes
            header_line = csv_bytes.splitlines(keepends=True)[0]
            buffer.extend(csv_bytes)
        else:
            # subsequent pages: drop the header line
            lines = csv_bytes.splitlines(keepends=True)
            if lines and lines[0] == header_line:
                buffer.extend(b"".join(lines[1:]))
            else:
                # fallback: if header mismatch, just append all (rare; safer than losing)
                buffer.extend(csv_bytes)

        pages_in_current_file += 1
        # In daily_hive mode we must not flush mid-run (it would overwrite data.csv). Flush only once at the end.
        if (not daily_hive) and pages_in_current_file >= max_pages_per_file:
            flush_part()

        if total_pages % 25 == 0:
            print(f"[progress] window {window.start_ymd}..{window.end_ymd} pages={total_pages} rows~={total_rows} offset={offset}")

        # end condition for window
        if rows < limit:
            print(f"[info] window {window.start_ymd}..{window.end_ymd}: last page rows={rows} (<{limit}).")
            break

        offset += limit
        if polite_sleep > 0:
            time.sleep(polite_sleep)

    # flush remaining buffer
    flush_part()

    manifest = {
        "dataset_url": dataset_url,
        "mode": mode,
        "window_start": iso_utc(window.start),
        "window_end": iso_utc(window.end),
        "limit": limit,
        "max_pages_per_file": max_pages_per_file,
        "gzip": gzip_output,
        "total_rows_estimated": total_rows,
        "total_pages": total_pages,
        "gcs_bucket": bucket,
        "gcs_prefix": prefix,
        "parts": part_files,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    upload_bytes_to_gcs(
        gcs,
        bucket,
        manifest_blob,
        json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        content_type="application/json",
    )
    upload_bytes_to_gcs(gcs, bucket, success_blob, b"", content_type="text/plain")
    print(f"[ok] window {window.start_ymd}..{window.end_ymd} done. parts={len(part_files)} rows~={total_rows}")

    return manifest


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--dataset-url", default=os.getenv("SODA_DATASET_URL", DEFAULT_DATASET_URL))
    p.add_argument("--app-token", default=os.getenv("SOCRATA_APP_TOKEN"))
    p.add_argument("--gcp-project", default=os.getenv("GCP_PROJECT_ID"))
    p.add_argument("--gcs-bucket", default=os.getenv("GCS_BUCKET_NAME"))
    p.add_argument("--gcs-prefix", default=os.getenv("GCS_PREFIX", "chicago_311/raw"))
    p.add_argument(
        "--daily-hive",
        action="store_true",
        help="Write one file per day using Hive-style partitioning: .../created_date=YYYY-MM-DD/ (requires --window-days 1).",
    )

    p.add_argument("--mode", default="full", choices=["full", "incremental"])
    p.add_argument("--watermark", default=None, help="Incremental lower bound on last_modified_date (ISO8601 or YYYY-MM-DD).")
    p.add_argument("--overlap-hours", type=int, default=24, help="Safety overlap applied to watermark (default 24h).")
    p.add_argument("--run-date", default=None, help="Run date (YYYY-MM-DD) used for incremental output partitioning. Default: today UTC.")

    p.add_argument("--start-date", default=DEFAULT_START_DATE)
    p.add_argument("--end-date", default=DEFAULT_END_DATE)
    p.add_argument("--window-days", type=int, default=1)

    p.add_argument("--limit", type=int, default=1000)
    p.add_argument("--max-pages-per-file", type=int, default=50)
    p.add_argument("--gzip", action="store_true")

    p.add_argument("--overwrite-window", action="store_true")
    p.add_argument("--timeout-connect", type=int, default=10)
    p.add_argument("--timeout-read", type=int, default=60)
    p.add_argument("--max-retries", type=int, default=6)
    p.add_argument("--base-backoff", type=float, default=1.0)
    p.add_argument("--polite-sleep", type=float, default=0.15)

    return p.parse_args()


def main() -> int:
    args = parse_args()

    if not args.app_token:
        print("[error] missing app token. Provide --app-token or SOCRATA_APP_TOKEN env var.", file=sys.stderr)
        return 2
    if not args.gcp_project:
        print("[error] missing GCP project. Provide --gcp-project or GCP_PROJECT_ID env var.", file=sys.stderr)
        return 2
    if not args.gcs_bucket:
        print("[error] missing GCS bucket. Provide --gcs-bucket or GCS_BUCKET_NAME env var.", file=sys.stderr)
        return 2

    # Watermark handling
    wm: Optional[datetime] = None
    if args.mode == "incremental":
        if not args.watermark:
            print("[error] incremental mode requires --watermark (last_modified_date lower bound).", file=sys.stderr)
            return 2
        wm = parse_watermark(args.watermark)

        # Use an exact 1-day window for daily-hive partitioning semantics.
        # The query still uses watermark/overlap as the true lower bound.
        if args.run_date:
            run_start = utc_midnight(args.run_date)
        else:
            today_utc = datetime.now(timezone.utc).date().isoformat()
            run_start = utc_midnight(today_utc)
        run_end = run_start + timedelta(days=1)

        windows = [Window(start=run_start, end=run_end)]
    else:
        windows = build_windows(args.start_date, args.end_date, args.window_days)

    if args.mode == "full":
        print(f"[info] windows={len(windows)} start={args.start_date} end={args.end_date} window_days={args.window_days}")
    else:
        print(f"[info] incremental run_date={windows[0].start_ymd} watermark={wm.isoformat()} overlap_hours={args.overlap_hours}")

    headers = {
        "X-App-Token": args.app_token,
        "Accept": "text/csv",
        "User-Agent": "chicago-311-extractor/0.1",
    }

    timeout = (args.timeout_connect, args.timeout_read)

    # HTTP session reuse
    session = requests.Session()

    # GCS client (uses GOOGLE_APPLICATION_CREDENTIALS inside container/runtime)
    gcs = storage.Client(project=args.gcp_project)

    # Run each window
    for idx, w in enumerate(windows, start=1):
        print(f"[run] ({idx}/{len(windows)}) window {w.start_ymd}..{w.end_ymd}")
        run_window(
            session=session,
            gcs=gcs,
            dataset_url=args.dataset_url,
            headers=headers,
            bucket=args.gcs_bucket,
            base_prefix=args.gcs_prefix,
            mode=args.mode,
            daily_hive=bool(args.daily_hive),
            watermark=wm,
            overlap_hours=int(args.overlap_hours),
            window=w,
            limit=args.limit,
            max_pages_per_file=max(1, args.max_pages_per_file),
            gzip_output=bool(args.gzip),
            overwrite_window=bool(args.overwrite_window),
            timeout=timeout,
            max_retries=args.max_retries,
            base_backoff=args.base_backoff,
            polite_sleep=args.polite_sleep,
        )

    print(f"[done] {args.mode} run completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())