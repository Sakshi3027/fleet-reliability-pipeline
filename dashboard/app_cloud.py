import os
import warnings
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
load_dotenv()

st.set_page_config(
    page_title="Fleet Reliability Dashboard",
    page_icon="🚗",
    layout="wide",
)


# ── DB connection ─────────────────────────────────────────────────────────────
def get_conn_string():
    try:
        return st.secrets["SUPABASE_URL"]
    except Exception:
        load_dotenv()
        return os.getenv("SUPABASE_URL")


def load_query(sql: str) -> pd.DataFrame:
    import psycopg2
    url = get_conn_string()
    url = url.replace("postgresql+psycopg2://", "").replace("postgresql://", "")
    user_pass, rest = url.split("@", 1)
    user, password  = user_pass.split(":", 1)
    host_port, dbname = rest.split("/", 1)
    host, port      = host_port.split(":", 1)
    conn = psycopg2.connect(
        host=host, port=int(port), dbname=dbname,
        user=user, password=password,
        sslmode="require", connect_timeout=10,
    )
    cur  = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=cols)


# ── Data loaders ──────────────────────────────────────────────────────────────
def faults():
    return load_query("SELECT * FROM raw_fault_codes")

def repairs():
    return load_query("SELECT * FROM raw_repair_logs")

def mttr():
    return load_query("SELECT * FROM mart_mttr ORDER BY period_month")

def failure_rates():
    return load_query("SELECT * FROM mart_failure_rates ORDER BY period_month")

def vehicle_health():
    return load_query("SELECT * FROM mart_vehicle_health ORDER BY period_month")

def forecasts():
    return load_query("SELECT * FROM mart_failure_forecast ORDER BY forecast_month")


# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.image("https://img.icons8.com/fluency/96/electric-vehicle.png", width=60)
st.sidebar.title("Fleet Reliability")
st.sidebar.markdown("---")

df_faults   = faults()
df_repairs  = repairs()
df_mttr     = mttr()
df_fr       = failure_rates()
df_health   = vehicle_health()
df_forecast = forecasts()

df_faults["occurred_at"]   = pd.to_datetime(df_faults["occurred_at"], utc=True)
df_repairs["repair_start"] = pd.to_datetime(df_repairs["repair_start"], utc=True)
df_mttr["period_month"]    = pd.to_datetime(df_mttr["period_month"])
df_fr["period_month"]      = pd.to_datetime(df_fr["period_month"])
df_health["period_month"]  = pd.to_datetime(df_health["period_month"], utc=True)
df_mttr["period_month"]    = pd.to_datetime(df_mttr["period_month"], utc=True)
df_fr["period_month"]      = pd.to_datetime(df_fr["period_month"], utc=True)

all_components = sorted(df_faults["component"].unique().tolist())
sel_components = st.sidebar.multiselect(
    "Components", all_components, default=all_components
)
sel_severities = st.sidebar.multiselect(
    "Severity", ["critical", "high", "medium", "low"],
    default=["critical", "high", "medium", "low"]
)
date_min = df_faults["occurred_at"].min().date()
date_max = df_faults["occurred_at"].max().date()
sel_dates = st.sidebar.date_input(
    "Date range", value=(date_min, date_max),
    min_value=date_min, max_value=date_max
)
st.sidebar.markdown("---")
st.sidebar.caption("Data refreshes every 5 min")

start = pd.Timestamp(sel_dates[0]).tz_localize("UTC")
end   = pd.Timestamp(sel_dates[1]).tz_localize("UTC")
df_f = df_faults[
    df_faults["component"].isin(sel_components) &
    df_faults["severity"].isin(sel_severities) &
    df_faults["occurred_at"].between(start, end)
]
df_r = df_repairs[df_repairs["component"].isin(sel_components)]


# ── Header ────────────────────────────────────────────────────────────────────
st.title("🚗 Fleet Reliability Dashboard")
st.caption(f"Showing {len(df_f):,} fault events across {df_f['vehicle_id'].nunique()} vehicles")
st.markdown("---")

# ── KPIs ─────────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)
total_faults    = len(df_f)
critical_faults = len(df_f[df_f["severity"] == "critical"])
avg_mttr        = df_r["mttr_hours"].mean() if len(df_r) else 0
resolution_rate = (df_f["resolved"].sum() / len(df_f) * 100) if len(df_f) else 0
total_cost      = df_r["parts_cost_usd"].fillna(0).sum() + df_r["labor_cost_usd"].fillna(0).sum()

k1.metric("Total Faults",      f"{total_faults:,}")
k2.metric("Critical Faults",   f"{critical_faults:,}",
          delta=f"{critical_faults/total_faults*100:.1f}% of total" if total_faults else None,
          delta_color="inverse")
k3.metric("Avg MTTR",          f"{avg_mttr:.1f} hrs")
k4.metric("Resolution Rate",   f"{resolution_rate:.1f}%")
k5.metric("Total Repair Cost", f"${total_cost:,.0f}")
st.markdown("---")

# ── Row 1: Fault trend + Severity ─────────────────────────────────────────────
c1, c2 = st.columns([2, 1])
with c1:
    st.subheader("Monthly Fault Trend")
    monthly = (
        df_f.set_index("occurred_at")
        .resample("ME")["fault_id"].count()
        .reset_index()
        .rename(columns={"occurred_at": "month", "fault_id": "faults"})
    )
    fig = px.line(monthly, x="month", y="faults",
                  markers=True, color_discrete_sequence=["#E8593C"])
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), xaxis_title="", yaxis_title="Faults")
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("By Severity")
    sev_counts = df_f["severity"].value_counts().reset_index()
    sev_counts.columns = ["severity", "count"]
    color_map = {"critical": "#E24B4A", "high": "#EF9F27", "medium": "#378ADD", "low": "#1D9E75"}
    fig2 = px.pie(sev_counts, names="severity", values="count",
                  color="severity", color_discrete_map=color_map, hole=0.4)
    fig2.update_layout(margin=dict(l=0, r=0, t=0, b=0), legend=dict(orientation="h", y=-0.1))
    st.plotly_chart(fig2, use_container_width=True)

# ── Row 2: MTTR + Fault rate ──────────────────────────────────────────────────
c3, c4 = st.columns(2)
with c3:
    st.subheader("Avg MTTR by Component")
    mttr_comp = df_r.groupby("component")["mttr_hours"].mean().reset_index()
    mttr_comp = mttr_comp.sort_values("mttr_hours", ascending=True)
    fig3 = px.bar(mttr_comp, x="mttr_hours", y="component", orientation="h",
                  color="mttr_hours", color_continuous_scale="Reds",
                  labels={"mttr_hours": "Avg MTTR (hrs)", "component": ""})
    fig3.update_layout(margin=dict(l=0, r=0, t=0, b=0), coloraxis_showscale=False)
    st.plotly_chart(fig3, use_container_width=True)

with c4:
    st.subheader("Fault Rate by Component")
    fault_comp = df_f["component"].value_counts().reset_index()
    fault_comp.columns = ["component", "faults"]
    fig4 = px.bar(fault_comp, x="component", y="faults",
                  color="faults", color_continuous_scale="Blues")
    fig4.update_layout(margin=dict(l=0, r=0, t=0, b=0),
                       xaxis_tickangle=-30, coloraxis_showscale=False,
                       xaxis_title="", yaxis_title="Fault Count")
    st.plotly_chart(fig4, use_container_width=True)

# ── Row 3: Battery SOH + Top vehicles ────────────────────────────────────────
c5, c6 = st.columns(2)
with c5:
    st.subheader("Fleet Avg Battery SOH Over Time")
    df_health["avg_battery_soh_pct"] = pd.to_numeric(df_health["avg_battery_soh_pct"], errors="coerce")
    soh = df_health.groupby("period_month")["avg_battery_soh_pct"].mean().reset_index()
    fig5 = px.area(soh, x="period_month", y="avg_battery_soh_pct",
                   color_discrete_sequence=["#1D9E75"],
                   labels={"avg_battery_soh_pct": "SOH %", "period_month": ""})
    fig5.update_layout(margin=dict(l=0, r=0, t=0, b=0))
    fig5.add_hline(y=80, line_dash="dash", line_color="red", annotation_text="80% threshold")
    st.plotly_chart(fig5, use_container_width=True)

with c6:
    st.subheader("Top 10 Vehicles by Fault Count")
    top_v = (
        df_f.groupby("vehicle_id")["fault_id"].count()
        .nlargest(10).reset_index()
        .rename(columns={"fault_id": "faults"})
    )
    fig6 = px.bar(top_v, x="vehicle_id", y="faults",
                  color_discrete_sequence=["#534AB7"])
    fig6.update_layout(margin=dict(l=0, r=0, t=0, b=0),
                       xaxis_title="", yaxis_title="Faults", xaxis_tickangle=-45)
    st.plotly_chart(fig6, use_container_width=True)

# ── Row 4: Forecast ───────────────────────────────────────────────────────────
st.markdown("---")
st.subheader("🔮 30-Day Failure Forecast (Prophet)")
df_forecast["forecast_month"]   = pd.to_datetime(df_forecast["forecast_month"])
df_forecast["predicted_faults"] = pd.to_numeric(df_forecast["predicted_faults"])
risk_colors = {"high": "#E24B4A", "medium": "#EF9F27", "low": "#1D9E75"}

fc1, fc2 = st.columns([2, 1])
with fc1:
    fig7 = px.bar(df_forecast, x="component", y="predicted_faults",
                  color="risk_tier", barmode="group",
                  color_discrete_map=risk_colors,
                  facet_col="forecast_month", facet_col_wrap=3,
                  labels={"predicted_faults": "Predicted Faults", "component": ""})
    fig7.update_layout(margin=dict(l=0, r=0, t=40, b=0),
                       xaxis_tickangle=-45, legend_title="Risk")
    fig7.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1][:7]))
    st.plotly_chart(fig7, use_container_width=True)

with fc2:
    st.markdown("**Risk Summary**")
    risk_summary = df_forecast.groupby("risk_tier")["component"].count().reset_index()
    risk_summary.columns = ["Risk", "Count"]
    for _, row in risk_summary.iterrows():
        color = risk_colors.get(row["Risk"], "#888")
        st.markdown(
            f'<span style="color:{color}; font-size:18px">●</span> '
            f'**{row["Risk"].upper()}** — {row["Count"]} forecasts',
            unsafe_allow_html=True,
        )
    st.markdown("---")
    st.caption("Forecast generated by Prophet. Based on 24 months of historical fault data.")

# ── Row 5: Recent critical faults ────────────────────────────────────────────
st.markdown("---")
st.subheader("🚨 Recent Critical & High Faults")
recent = (
    df_faults[df_faults["severity"].isin(["critical", "high"])]
    .sort_values("occurred_at", ascending=False)
    .head(20)[["occurred_at", "vehicle_id", "component",
               "fault_code", "description", "severity", "resolved"]]
)
recent["occurred_at"] = recent["occurred_at"].dt.strftime("%Y-%m-%d %H:%M")

def highlight_severity(row):
    if row["severity"] == "critical":
        return ["background-color: #fde8e8"] * len(row)
    elif row["severity"] == "high":
        return ["background-color: #fef3e2"] * len(row)
    return [""] * len(row)

st.dataframe(
    recent.style.apply(highlight_severity, axis=1),
    use_container_width=True,
    hide_index=True,
)