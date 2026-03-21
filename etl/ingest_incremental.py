"""
etl/ingest_incremental.py — Incremental load pipeline.
Only processes records newer than the last successful load watermark.
This is how production pipelines work — no full reloads.

Run: python etl/ingest_incremental.py
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

RAW_DIR = Path("data/raw")


# ── DB connection ─────────────────────────────────────────────────────────────
def get_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(url)


# ── Watermark helpers ─────────────────────────────────────────────────────────
def get_watermark(engine, table: str) -> datetime:
    """Get the last loaded timestamp for a table."""
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        cur.execute(
            "SELECT last_loaded_at FROM pipeline_watermarks WHERE table_name = %s",
            (table,)
        )
        row = cur.fetchone()
        cur.close()
    finally:
        raw_conn.close()
    return row[0] if row else datetime(2000, 1, 1, tzinfo=timezone.utc)


def update_watermark(engine, table: str, new_ts: datetime, rows: int) -> None:
    """Update the watermark after a successful load."""
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        cur.execute("""
            INSERT INTO pipeline_watermarks
                (table_name, last_loaded_at, rows_loaded, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (table_name) DO UPDATE SET
                last_loaded_at = EXCLUDED.last_loaded_at,
                rows_loaded    = EXCLUDED.rows_loaded,
                updated_at     = NOW()
        """, (table, new_ts, rows))
        raw_conn.commit()
        cur.close()
    finally:
        raw_conn.close()


# ── Incremental insert ────────────────────────────────────────────────────────
def insert_new_records(
    df: pd.DataFrame,
    table: str,
    engine,
    id_col: str,
) -> int:
    """Insert only records not already in the table. Returns count inserted."""
    if df.empty:
        return 0

    # Get existing IDs to avoid duplicates
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        cur.execute(f'SELECT "{id_col}" FROM "{table}"')
        existing_ids = {row[0] for row in cur.fetchall()}
        cur.close()
    finally:
        raw_conn.close()

    # Filter to only new records
    new_df = df[~df[id_col].isin(existing_ids)].copy()

    if new_df.empty:
        return 0

    records      = new_df.where(pd.notnull(new_df), None).to_dict(orient="records")
    cols         = list(records[0].keys())
    col_str      = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))

    raw_conn = engine.raw_connection()
    try:
        cur  = raw_conn.cursor()
        rows = [tuple(r[c] for c in cols) for r in records]
        cur.executemany(
            f'INSERT INTO "{table}" ({col_str}) VALUES ({placeholders})',
            rows,
        )
        raw_conn.commit()
        cur.close()
    finally:
        raw_conn.close()

    return len(new_df)


# ── Dead letter queue ─────────────────────────────────────────────────────────
def send_to_dlq(records: list[dict], table: str, reason: str, engine) -> None:
    """Bad records go here instead of crashing the pipeline."""
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dead_letter_queue (
                id          SERIAL PRIMARY KEY,
                source_table TEXT,
                record_data  TEXT,
                reason       TEXT,
                failed_at    TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        for record in records:
            cur.execute("""
                INSERT INTO dead_letter_queue
                    (source_table, record_data, reason)
                VALUES (%s, %s, %s)
            """, (table, str(record), reason))
        raw_conn.commit()
        cur.close()
    finally:
        raw_conn.close()
    print(f"  ⚠️   {len(records)} records sent to dead_letter_queue — {reason}")


# ── Incremental loaders ───────────────────────────────────────────────────────
def load_fault_codes_incremental(engine) -> None:
    table     = "raw_fault_codes"
    watermark = get_watermark(engine, table)

    df = pd.read_csv(RAW_DIR / "fault_codes.csv")
    df["occurred_at"] = pd.to_datetime(df["occurred_at"], utc=True)
    df["resolved"]    = df["resolved"].astype(bool)

    new_df = df[df["occurred_at"] > watermark].copy()

    valid_severities = {"critical", "high", "medium", "low"}
    bad  = new_df[~new_df["severity"].isin(valid_severities)]
    good = new_df[new_df["severity"].isin(valid_severities)]

    if not bad.empty:
        send_to_dlq(bad.to_dict("records"), table, "invalid severity value", engine)

    inserted = insert_new_records(good, table, engine, "fault_id")

    # Always update watermark to max timestamp in full dataset
    max_ts = df["occurred_at"].max()
    update_watermark(engine, table, max_ts, inserted)

    print(f"  ✓  {table:<25} — {inserted:>5} new rows  "
          f"(watermark → {max_ts.strftime('%Y-%m-%d')})")


def load_repair_logs_incremental(engine) -> None:
    table     = "raw_repair_logs"
    watermark = get_watermark(engine, table)

    with open(RAW_DIR / "repair_logs.json", encoding="utf-8") as f:
        records = json.load(f)

    df = pd.DataFrame(records)
    df["repair_start"]   = pd.to_datetime(df["repair_start"], utc=True)
    df["repair_end"]     = pd.to_datetime(df["repair_end"],   utc=True)
    df["parts_replaced"] = df["parts_replaced"].astype(bool)
    df["warranty_claim"] = df["warranty_claim"].astype(bool)

    new_df   = df[df["repair_start"] > watermark].copy()
    inserted = insert_new_records(new_df, table, engine, "repair_id")

    max_ts = df["repair_start"].max()
    update_watermark(engine, table, max_ts, inserted)

    print(f"  ✓  {table:<25} — {inserted:>5} new rows  "
          f"(watermark → {max_ts.strftime('%Y-%m-%d')})")


def load_telemetry_incremental(engine) -> None:
    table     = "raw_telemetry"
    watermark = get_watermark(engine, table)

    df = pd.read_csv(RAW_DIR / "vehicle_telemetry.csv")
    df["recorded_at"]        = pd.to_datetime(df["recorded_at"], utc=True)
    df["ota_update_pending"] = df["ota_update_pending"].astype(bool)

    new_df   = df[df["recorded_at"] > watermark].copy()
    inserted = insert_new_records(new_df, table, engine, "telemetry_id")

    max_ts = df["recorded_at"].max()
    update_watermark(engine, table, max_ts, inserted)

    print(f"  ✓  {table:<25} — {inserted:>5} new rows  "
          f"(watermark → {max_ts.strftime('%Y-%m-%d')})")


def load_vehicles_incremental(engine) -> None:
    """Vehicles are a slowly changing dimension — insert new ones only."""
    table = "raw_vehicles"

    df = pd.read_csv(RAW_DIR / "vehicles.csv")
    df["manufactured_date"] = pd.to_datetime(df["manufactured_date"]).dt.date

    inserted = insert_new_records(df, table, engine, "vehicle_id")
    update_watermark(engine, table, datetime.now(tz=timezone.utc), inserted)

    print(f"  ✓  {table:<25} — {inserted:>5} new rows")


def show_watermarks(engine) -> None:
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        cur.execute("""
            SELECT table_name, last_loaded_at, rows_loaded, updated_at
            FROM pipeline_watermarks
            ORDER BY table_name
        """)
        rows = cur.fetchall()
        cur.close()
    finally:
        raw_conn.close()

    print("\n  Current watermarks:")
    for row in rows:
        print(f"    {row[0]:<25} last: {str(row[1])[:19]}  rows: {row[2]:>6,}")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n📥  Running incremental ingest...\n")
    engine = get_engine()

    load_vehicles_incremental(engine)
    load_fault_codes_incremental(engine)
    load_repair_logs_incremental(engine)
    load_telemetry_incremental(engine)

    show_watermarks(engine)
    print("\n✅  Incremental ingest complete.\n")