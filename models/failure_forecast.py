"""
models/failure_forecast.py — Predicts 30-day failure likelihood per component.
Uses Prophet for time-series forecasting on monthly fault counts.
Run: python models/failure_forecast.py
"""

import os
import warnings
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore")
load_dotenv()


# ── DB connection ─────────────────────────────────────────────────────────────
def get_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(url)


# ── Load training data ────────────────────────────────────────────────────────
def load_fault_series(engine) -> pd.DataFrame:
    """Monthly fault counts per component — this is what Prophet trains on."""
    sql = """
        SELECT
            DATE_TRUNC('month', occurred_at)::date AS ds,
            component,
            COUNT(*) AS y
        FROM raw_fault_codes
        GROUP BY DATE_TRUNC('month', occurred_at), component
        ORDER BY ds, component
    """
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        cur.close()
    finally:
        raw_conn.close()

    df = pd.DataFrame(rows, columns=cols)
    df["ds"] = pd.to_datetime(df["ds"])
    df["y"]  = df["y"].astype(float)
    return df


# ── Prophet forecast per component ───────────────────────────────────────────
def forecast_component(series: pd.DataFrame, component: str) -> pd.DataFrame:
    """Fit Prophet on one component's monthly fault series, forecast 3 months."""
    from prophet import Prophet

    df = series[series["component"] == component][["ds", "y"]].copy()

    if len(df) < 6:
        return pd.DataFrame()   # not enough data to forecast

    m = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False,
        changepoint_prior_scale=0.1,
        interval_width=0.80,
    )
    m.fit(df)

    future   = m.make_future_dataframe(periods=3, freq="MS")
    forecast = m.predict(future)

    forecast["component"]   = component
    forecast["actual_y"]    = df.set_index("ds").reindex(forecast["ds"])["y"].values

    return forecast[["ds", "component", "yhat", "yhat_lower",
                      "yhat_upper", "actual_y"]].tail(3)


# ── Risk scoring ──────────────────────────────────────────────────────────────
def score_risk(yhat: float, historical_mean: float) -> str:
    """Simple risk tier based on forecast vs historical average."""
    if historical_mean == 0:
        return "unknown"
    ratio = yhat / historical_mean
    if ratio >= 1.4:
        return "high"
    elif ratio >= 1.1:
        return "medium"
    else:
        return "low"


# ── Save forecasts to DB ──────────────────────────────────────────────────────
def save_forecasts(forecasts: pd.DataFrame, engine) -> None:
    """Write forecast results to mart_failure_forecast table."""
    create_sql = """
        CREATE TABLE IF NOT EXISTS mart_failure_forecast (
            component       TEXT,
            forecast_month  DATE,
            predicted_faults NUMERIC(8,2),
            lower_bound     NUMERIC(8,2),
            upper_bound     NUMERIC(8,2),
            risk_tier       TEXT,
            generated_at    TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (component, forecast_month)
        )
    """
    with engine.connect() as conn:
        conn.execute(text(create_sql))

    records = forecasts.where(pd.notnull(forecasts), None).to_dict(orient="records")
    if not records:
        return

    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        cur.execute("TRUNCATE TABLE mart_failure_forecast")
        for r in records:
            cur.execute("""
                INSERT INTO mart_failure_forecast
                    (component, forecast_month, predicted_faults,
                     lower_bound, upper_bound, risk_tier)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (component, forecast_month) DO UPDATE SET
                    predicted_faults = EXCLUDED.predicted_faults,
                    lower_bound      = EXCLUDED.lower_bound,
                    upper_bound      = EXCLUDED.upper_bound,
                    risk_tier        = EXCLUDED.risk_tier,
                    generated_at     = NOW()
            """, (
                r["component"],
                r["forecast_month"].date() if hasattr(r["forecast_month"], "date") else r["forecast_month"],
                round(float(r["predicted_faults"]), 2),
                round(float(r["lower_bound"]), 2),
                round(float(r["upper_bound"]), 2),
                r["risk_tier"],
            ))
        raw_conn.commit()
        cur.close()
    finally:
        raw_conn.close()


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n🔮  Running failure forecasts...\n")
    engine     = get_engine()
    series     = load_fault_series(engine)
    components = series["component"].unique()

    all_forecasts = []

    for component in sorted(components):
        try:
            fc = forecast_component(series, component)
            if fc.empty:
                print(f"  ⚠️   {component:<25} — not enough data, skipped")
                continue

            hist_mean = series[series["component"] == component]["y"].mean()
            fc["risk_tier"]       = fc["yhat"].apply(lambda v: score_risk(v, hist_mean))
            fc["predicted_faults"] = fc["yhat"].round(2)
            fc["lower_bound"]      = fc["yhat_lower"].round(2)
            fc["upper_bound"]      = fc["yhat_upper"].round(2)
            fc["forecast_month"]   = fc["ds"]

            all_forecasts.append(fc)

            # Print summary
            for _, row in fc.iterrows():
                print(
                    f"  {component:<25}  "
                    f"{str(row['ds'])[:7]}  "
                    f"predicted: {row['predicted_faults']:>5.1f} faults  "
                    f"risk: {row['risk_tier']}"
                )
        except Exception as e:
            print(f"  ✗  {component:<25} — error: {e}")

    if all_forecasts:
        combined = pd.concat(all_forecasts, ignore_index=True)
        save_forecasts(combined, engine)
        print(f"\n  💾  Saved {len(combined)} forecast rows to mart_failure_forecast")

    print("\n✅  Forecast complete.\n")