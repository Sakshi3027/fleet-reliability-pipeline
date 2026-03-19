"""
etl/clean.py — Data quality checks and cleaning on raw tables.
Run: python etl/clean.py
"""

import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()


def get_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(url)


def check_nulls(engine) -> None:
    checks = {
        "raw_fault_codes":  ["fault_id", "vehicle_id", "severity", "occurred_at"],
        "raw_repair_logs":  ["repair_id", "vehicle_id", "mttr_hours", "repair_start"],
        "raw_telemetry":    ["telemetry_id", "vehicle_id", "battery_soh_pct"],
        "raw_vehicles":     ["vehicle_id", "model"],
    }
    print("  Null checks:")
    all_passed = True
    with engine.connect() as conn:
        for table, cols in checks.items():
            for col in cols:
                n = conn.execute(
                    text(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
                ).scalar()
                status = "✓" if n == 0 else "✗ FAIL"
                if n > 0:
                    all_passed = False
                print(f"    {status}  {table}.{col:<30} nulls: {n}")
    return all_passed


def check_severity_values(engine) -> bool:
    valid = ("'critical'", "'high'", "'medium'", "'low'")
    sql = f"""
        SELECT COUNT(*) FROM raw_fault_codes
        WHERE severity NOT IN ({','.join(valid)})
    """
    with engine.connect() as conn:
        n = conn.execute(text(sql)).scalar()
    status = "✓" if n == 0 else f"✗ FAIL — {n} invalid values"
    print(f"\n  Severity values:  {status}")
    return n == 0


def check_mttr_positive(engine) -> bool:
    sql = "SELECT COUNT(*) FROM raw_repair_logs WHERE mttr_hours <= 0"
    with engine.connect() as conn:
        n = conn.execute(text(sql)).scalar()
    status = "✓" if n == 0 else f"✗ FAIL — {n} non-positive MTTR values"
    print(f"  MTTR > 0:         {status}")
    return n == 0


def check_battery_soh_range(engine) -> bool:
    sql = """
        SELECT COUNT(*) FROM raw_telemetry
        WHERE battery_soh_pct < 0 OR battery_soh_pct > 100
    """
    with engine.connect() as conn:
        n = conn.execute(text(sql)).scalar()
    status = "✓" if n == 0 else f"✗ FAIL — {n} out-of-range SOH values"
    print(f"  Battery SOH range: {status}")
    return n == 0


def check_repair_dates(engine) -> bool:
    sql = """
        SELECT COUNT(*) FROM raw_repair_logs
        WHERE repair_end <= repair_start
    """
    with engine.connect() as conn:
        n = conn.execute(text(sql)).scalar()
    status = "✓" if n == 0 else f"✗ FAIL — {n} invalid date ranges"
    print(f"  Repair end > start:{status}")
    return n == 0


def row_count_summary(engine) -> None:
    tables = ["raw_vehicles", "raw_fault_codes", "raw_repair_logs", "raw_telemetry"]
    print("\n  Row counts:")
    with engine.connect() as conn:
        for table in tables:
            n = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            print(f"    {table:<25} {n:>7,}")


if __name__ == "__main__":
    print("\n🔍  Running data quality checks...\n")
    engine = get_engine()

    results = [
        check_nulls(engine),
        check_severity_values(engine),
        check_mttr_positive(engine),
        check_battery_soh_range(engine),
        check_repair_dates(engine),
    ]
    row_count_summary(engine)

    if all(results):
        print("\n✅  All quality checks passed. Safe to transform.\n")
    else:
        print("\n⚠️   Some checks failed — review above before transforming.\n")