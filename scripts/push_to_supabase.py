"""
scripts/push_to_supabase.py
Pushes all local tables to Supabase for cloud dashboard deployment.
Run: python scripts/push_to_supabase.py
"""

import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

LOCAL_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
SUPABASE_URL = "postgresql+psycopg2://postgres:Sakshichavan27@db.gqnrwfjrgwhaoozvtwng.supabase.co:5432/postgres"

TABLES = [
    ("public",          "raw_vehicles"),
    ("public",          "raw_fault_codes"),
    ("public",          "raw_repair_logs"),
    ("public",          "raw_telemetry"),
    ("public",          "mart_failure_forecast"),
    ("analytics_marts", "mart_mttr"),
    ("analytics_marts", "mart_failure_rates"),
    ("analytics_marts", "mart_vehicle_health"),
]


def read_table(engine, schema: str, table: str) -> pd.DataFrame:
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        cur.execute(f'SELECT * FROM "{schema}"."{table}"')
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        cur.close()
    finally:
        raw_conn.close()
    return pd.DataFrame(rows, columns=cols)


def push_table(df: pd.DataFrame, table: str, engine) -> None:
    if df.empty:
        print(f"  ⚠️   {table} is empty — skipped")
        return

    records = df.where(pd.notnull(df), None).to_dict(orient="records")
    cols    = list(records[0].keys())
    col_str = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))

    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()

        # Drop and recreate table
        cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')

        # Build CREATE TABLE from dataframe dtypes
        type_map = {
            "int64":   "BIGINT",
            "float64": "NUMERIC",
            "bool":    "BOOLEAN",
            "object":  "TEXT",
            "datetime64[ns]": "TIMESTAMPTZ",
            "datetime64[ns, UTC]": "TIMESTAMPTZ",
        }
        col_defs = []
        for col in cols:
            dtype = str(df[col].dtype)
            pg_type = type_map.get(dtype, "TEXT")
            col_defs.append(f'"{col}" {pg_type}')

        cur.execute(f'CREATE TABLE "{table}" ({", ".join(col_defs)})')

        # Bulk insert
        rows = [tuple(r[c] for c in cols) for r in records]
        cur.executemany(
            f'INSERT INTO "{table}" ({col_str}) VALUES ({placeholders})',
            rows,
        )
        raw_conn.commit()
        cur.close()
    finally:
        raw_conn.close()

    print(f"  ✓  {table:<35} — {len(df):>7,} rows")


if __name__ == "__main__":
    print("\n☁️   Pushing data to Supabase...\n")
    local_engine    = create_engine(LOCAL_URL)
    supabase_engine = create_engine(SUPABASE_URL)

    for schema, table in TABLES:
        print(f"  Reading {schema}.{table}...")
        df = read_table(local_engine, schema, table)
        push_table(df, table, supabase_engine)

    print("\n✅  All tables pushed to Supabase.\n")