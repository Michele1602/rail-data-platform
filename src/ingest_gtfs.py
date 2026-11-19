
from __future__ import annotations

import io
import os
import re
import zipfile
import csv
import hashlib
import requests
import logging
import argparse
#import pandas as pd

from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
from datetime import date
"""Load a GTFS feed (zip) as-is into PostgreSQL, schema `raw`.

Design choices for the raw layer:
- every column is stored as text; typing happens later in dbt (staging layer)
- every row carries `_feed_id` (hash of the zip file) and `_loaded_at`
- a feed that was already loaded is skipped (idempotent); a new feed version is
  appended, so the history of feed versions is kept (useful for a Data Vault
  layer later)
- the whole feed is loaded in one transaction: either everything or nothing
  (if a table already exists with different columns, COPY fails and rolls back)

Usage (from the repository root):
    python src/ingest_gtfs.py --zip data/raw/my_feed.zip
    python src/ingest_gtfs.py            # downloads GTFS_URL from .env
"""
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = Path("data/raw")

GTFS_URL = os.getenv("GTFS_URL")

GTFS_FILES = [
    "agency.txt",
    "routes.txt",
    "stops.txt",
    "trips.txt",
    "stop_times.txt",
    "calendar_dates.txt",
    "feed_info.txt",
]
CHUNK_ROWS = 100_000

def connect():
    """opens the PostgreSQL connection using environment variables (POSTGRES_HOST, etc.), with default values."""

    return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )

def download_feed(url: str) -> Path:
    """Download the GTFS stream ZIP file and save the history in DATA_DIR"""

    logger.info(f"Downloading GTFS data from {url}")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    target = DATA_DIR / f"gtfs_{date.today():%Y%m%d}.zip"
    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        logger.info(f"Download complete. Size: {len(resp.content) / 1024 / 1024:.1f} MB")

        with open(target, "wb") as fh:
            for block in resp.iter_content(chunk_size=1024 * 1024):
                fh.write(block)
    return target

def file_hash(path: Path) -> str:
    """feed_id with 16 hexadecimal characters: same file, same feed_id"""

    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()[:16]

def clean_name(name: str) -> str:
    """Normalize column names: 
    lowercase, all non-alphanumeric characters replaced with _, 
    leading and trailing _ removed. 
    For example: Stop ID becomes stop_id."""
    return re.sub(r"\W+", "_", name.strip().lower()).strip("_")

def ensure_base_objects(cur) -> None:
    """Create the raw schema and the raw.feed_log table if needed. 
    This table is the log of feeds that have already been loaded."""
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.feed_log (
            feed_id   text PRIMARY KEY,
            source    text NOT NULL,
            loaded_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )

def load_table(cur, zf: zipfile.ZipFile, member: str, feed_id: str) -> int:
    table_name = "gtfs_" + Path(member).stem.lower()
    table = sql.Identifier("raw", table_name)

    with zf.open(member) as raw_file:
        reader = csv.reader(io.TextIOWrapper(raw_file, encoding="utf-8-sig", newline=""))
        header = [clean_name(col) for col in next(reader)]

        columns_ddl = sql.SQL(", ").join(
            sql.SQL("{} text").format(sql.Identifier(col)) for col in header
        )
        cur.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} ({}, _feed_id text, "
                "_loaded_at timestamptz DEFAULT now())"
            ).format(table, columns_ddl)
        )

        copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv)").format(
            table, sql.SQL(", ").join(map(sql.Identifier, header + ["_feed_id"]))
        ).as_string(cur)

        total = 0
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        pending = 0

        def flush() -> None:
            nonlocal pending, total
            if pending == 0:
                return
            buffer.seek(0)
            cur.copy_expert(copy_sql, buffer)
            total += pending
            buffer.seek(0)
            buffer.truncate()
            pending = 0

        for row in reader:
            if not row:
                continue
            row = (row + [""] * len(header))[: len(header)]  # pad / truncate
            writer.writerow(row + [feed_id])
            pending += 1
            if pending >= CHUNK_ROWS:
                flush()
        flush()
    return total

def main() -> None:
    parser = argparse.ArgumentParser(description="Load a GTFS zip into PostgreSQL (raw layer).")
    parser.add_argument("--zip", help="path to a local GTFS zip (otherwise GTFS_URL is downloaded)")
    args = parser.parse_args()

    if args.zip:
        zip_path, source = Path(args.zip), args.zip
    else:
        url = os.getenv("GTFS_URL")
        if not url:
            raise SystemExit("Set GTFS_URL in .env or pass --zip path/to/feed.zip")
        zip_path, source = download_feed(url), url

    feed_id = file_hash(zip_path)
    conn = connect()
    try:
        with conn.cursor() as cur:
            ensure_base_objects(cur)
            cur.execute("SELECT 1 FROM raw.feed_log WHERE feed_id = %s", (feed_id,))
            if cur.fetchone():
                print(f"Feed {feed_id} already loaded, nothing to do.")
                return
            with zipfile.ZipFile(zip_path) as zf:
                for member in sorted(zf.namelist()):
                    if member.lower().endswith(".txt"):
                        rows = load_table(cur, zf, member, feed_id)
                        print(f"{member}: {rows:,} rows")
            cur.execute(
                "INSERT INTO raw.feed_log (feed_id, source) VALUES (%s, %s)", (feed_id, source)
            )
        conn.commit()
        print(f"Feed {feed_id} loaded.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()