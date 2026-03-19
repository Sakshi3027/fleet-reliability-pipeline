"""
dags/fleet_pipeline_dag.py
Daily DAG: ingest → clean → transform
Schedule: runs every day at 6am UTC
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Project root — works regardless of where Airflow is launched from
PROJECT_ROOT = Path(__file__).parent.parent
PYTHON       = sys.executable   # uses the same venv Airflow was started in


# ── Task functions ────────────────────────────────────────────────────────────

def run_ingest(**context):
    result = subprocess.run(
        [PYTHON, str(PROJECT_ROOT / "etl" / "ingest.py")],
        capture_output=True, text=True, cwd=str(PROJECT_ROOT)
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"Ingest failed:\n{result.stderr}")


def run_clean(**context):
    result = subprocess.run(
        [PYTHON, str(PROJECT_ROOT / "etl" / "clean.py")],
        capture_output=True, text=True, cwd=str(PROJECT_ROOT)
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"Clean checks failed:\n{result.stderr}")


def run_transform(**context):
    result = subprocess.run(
        [PYTHON, str(PROJECT_ROOT / "etl" / "transform.py")],
        capture_output=True, text=True, cwd=str(PROJECT_ROOT)
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"Transform failed:\n{result.stderr}")


# ── DAG definition ────────────────────────────────────────────────────────────

default_args = {
    "owner":            "fleet-team",
    "retries":          1,
    "email_on_failure": False,
}

with DAG(
    dag_id="fleet_reliability_pipeline",
    description="Daily EV fleet ETL: ingest → quality check → transform",
    schedule_interval="0 6 * * *",   # every day at 6am UTC
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["fleet", "etl", "reliability"],
) as dag:

    t_ingest = PythonOperator(
        task_id="ingest_raw_data",
        python_callable=run_ingest,
    )

    t_clean = PythonOperator(
        task_id="quality_checks",
        python_callable=run_clean,
    )

    t_transform = PythonOperator(
        task_id="run_transforms",
        python_callable=run_transform,
    )

    # Pipeline order: ingest → clean → transform
    t_ingest >> t_clean >> t_transform