"""
etl/transform.py — SQL transformations that build the mart tables.
Reads from raw_* tables, writes to mart_* tables.
Run: python etl/transform.py
"""

import os
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


# ── SQL transforms ────────────────────────────────────────────────────────────

SQL_MTTR_BY_COMPONENT = """
DELETE FROM mart_mttr_by_component;

INSERT INTO mart_mttr_by_component
    (component, severity, avg_mttr_hours, median_mttr_hours, total_repairs, period_month)
SELECT
    component,
    severity,
    ROUND(AVG(mttr_hours)::numeric,    2) AS avg_mttr_hours,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mttr_hours)::numeric, 2)
                                          AS median_mttr_hours,
    COUNT(*)                              AS total_repairs,
    DATE_TRUNC('month', repair_start)::date AS period_month
FROM raw_repair_logs
GROUP BY component, severity, DATE_TRUNC('month', repair_start)
ORDER BY period_month, component, severity;
"""

SQL_FAILURE_RATES = """
DELETE FROM mart_failure_rates;

INSERT INTO mart_failure_rates
    (vehicle_id, component, period_month,
     fault_count, critical_count, resolved_count, failure_rate_pct)
SELECT
    vehicle_id,
    component,
    DATE_TRUNC('month', occurred_at)::date AS period_month,
    COUNT(*)                                AS fault_count,
    SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical_count,
    SUM(CASE WHEN resolved = TRUE       THEN 1 ELSE 0 END) AS resolved_count,
    ROUND(
        100.0 * SUM(CASE WHEN resolved = FALSE THEN 1 ELSE 0 END) / COUNT(*),
        2
    )                                       AS failure_rate_pct
FROM raw_fault_codes
GROUP BY vehicle_id, component, DATE_TRUNC('month', occurred_at)
ORDER BY period_month, vehicle_id, component;
"""

SQL_VEHICLE_HEALTH = """
DELETE FROM mart_vehicle_health;

INSERT INTO mart_vehicle_health
    (vehicle_id, period_month,
     avg_battery_soh_pct, avg_battery_temp_c, avg_motor_temp_c,
     total_fault_count, total_repair_cost)
SELECT
    t.vehicle_id,
    DATE_TRUNC('month', t.recorded_at)::date        AS period_month,
    ROUND(AVG(t.battery_soh_pct)::numeric,  2)      AS avg_battery_soh_pct,
    ROUND(AVG(t.battery_temp_c)::numeric,   1)      AS avg_battery_temp_c,
    ROUND(AVG(t.motor_temp_c)::numeric,     1)      AS avg_motor_temp_c,
    COUNT(DISTINCT f.fault_id)                      AS total_fault_count,
    ROUND(COALESCE(SUM(r.parts_cost_usd + r.labor_cost_usd), 0)::numeric, 2)
                                                    AS total_repair_cost
FROM raw_telemetry t
LEFT JOIN raw_fault_codes f
       ON f.vehicle_id = t.vehicle_id
      AND DATE_TRUNC('month', f.occurred_at) = DATE_TRUNC('month', t.recorded_at)
LEFT JOIN raw_repair_logs r
       ON r.fault_id = f.fault_id
GROUP BY t.vehicle_id, DATE_TRUNC('month', t.recorded_at)
ORDER BY period_month, t.vehicle_id;
"""


# ── Runner ────────────────────────────────────────────────────────────────────
def run_transform(name: str, sql: str, engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(sql))
    # count rows written
    table = "mart_" + name
    with engine.connect() as conn:
        n = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
    print(f"  ✓  {table:<30} — {n:>7,} rows")


def verify_marts(engine) -> None:
    marts = ["mart_mttr_by_component", "mart_failure_rates", "mart_vehicle_health"]
    print("\n  Mart row counts:")
    with engine.connect() as conn:
        for mart in marts:
            n = conn.execute(text(f"SELECT COUNT(*) FROM {mart}")).scalar()
            print(f"    {mart:<30} {n:>7,}")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n⚙️   Running SQL transforms...\n")
    engine = get_engine()

    run_transform("mttr_by_component", SQL_MTTR_BY_COMPONENT, engine)
    run_transform("failure_rates",     SQL_FAILURE_RATES,     engine)
    run_transform("vehicle_health",    SQL_VEHICLE_HEALTH,    engine)

    verify_marts(engine)
    print("\n✅  Transform complete.\n")