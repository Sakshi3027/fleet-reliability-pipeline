"""
etl/ingest.py — Load raw CSV/JSON files into PostgreSQL raw tables.
Run: python etl/ingest.py
"""

import json
import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()


# ── DB connection ─────────────────────────────────────────────────────────────
def get_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(url)


RAW_DIR = Path("data/raw")


# ── Core loader — compatible with SQLAlchemy 1.4 + pandas 3.x ────────────────
def load_df(df: pd.DataFrame, table: str, engine) -> None:
    """Truncate then bulk insert using raw psycopg2 — SQLAlchemy 1.4 compatible."""
    records = df.where(pd.notnull(df), None).to_dict(orient="records")
    if not records:
        return
    cols        = list(records[0].keys())
    col_str     = ", ".join(cols)
    placeholder = ", ".join(["%s"] * len(cols))

    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
        rows = [tuple(r[c] for c in cols) for r in records]
        cur.executemany(
            f"INSERT INTO {table} ({col_str}) VALUES ({placeholder})",
            rows,
        )
        raw_conn.commit()
        cur.close()
    finally:
        raw_conn.close()


# ── Loaders ───────────────────────────────────────────────────────────────────
def load_vehicles(engine) -> None:
    df = pd.read_csv(RAW_DIR / "vehicles.csv")
    df["manufactured_date"] = pd.to_datetime(df["manufactured_date"]).dt.date
    load_df(df, "raw_vehicles", engine)
    print(f"  ✓  raw_vehicles        — {len(df):>6,} rows")


def load_fault_codes(engine) -> None:
    df = pd.read_csv(RAW_DIR / "fault_codes.csv")
    df["occurred_at"] = pd.to_datetime(df["occurred_at"])
    df["resolved"]    = df["resolved"].astype(bool)
    load_df(df, "raw_fault_codes", engine)
    print(f"  ✓  raw_fault_codes     — {len(df):>6,} rows")


def load_repair_logs(engine) -> None:
    with open(RAW_DIR / "repair_logs.json", encoding="utf-8") as f:
        records = json.load(f)
    df = pd.DataFrame(records)
    df["repair_start"]   = pd.to_datetime(df["repair_start"])
    df["repair_end"]     = pd.to_datetime(df["repair_end"])
    df["parts_replaced"] = df["parts_replaced"].astype(bool)
    df["warranty_claim"] = df["warranty_claim"].astype(bool)
    load_df(df, "raw_repair_logs", engine)
    print(f"  ✓  raw_repair_logs     — {len(df):>6,} rows")


def load_telemetry(engine) -> None:
    df = pd.read_csv(RAW_DIR / "vehicle_telemetry.csv")
    df["recorded_at"]        = pd.to_datetime(df["recorded_at"])
    df["ota_update_pending"] = df["ota_update_pending"].astype(bool)
    load_df(df, "raw_telemetry", engine)
    print(f"  ✓  raw_telemetry       — {len(df):>6,} rows")


def verify_counts(engine) -> None:
    tables = ["raw_vehicles", "raw_fault_codes", "raw_repair_logs", "raw_telemetry"]
    print("\n  DB row counts:")
    with engine.connect() as conn:
        for table in tables:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            print(f"    {table:<25} {count:>7,}")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n📥  Ingesting raw data into PostgreSQL...\n")
    engine = get_engine()

    load_vehicles(engine)
    load_fault_codes(engine)
    load_repair_logs(engine)
    load_telemetry(engine)
    verify_counts(engine)

    print("\n✅  Ingest complete.\n")