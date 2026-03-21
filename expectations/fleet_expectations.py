"""
expectations/fleet_expectations.py
Data quality contracts using Great Expectations.
Generates an HTML validation report.

Run: python expectations/fleet_expectations.py
"""

import os
import json
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine
import great_expectations as gx
from great_expectations.core import ExpectationSuite, ExpectationConfiguration

load_dotenv()


def get_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(url)


def load_table(engine, table: str) -> pd.DataFrame:
    raw_conn = engine.raw_connection()
    try:
        cur  = raw_conn.cursor()
        cur.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        cur.close()
    finally:
        raw_conn.close()
    return pd.DataFrame(rows, columns=cols)


def validate_table(
    context,
    df: pd.DataFrame,
    suite_name: str,
    expectations: list[dict],
) -> dict:
    """Run expectations against a dataframe and return results."""

    # Build suite
    suite = ExpectationSuite(expectation_suite_name=suite_name)
    for exp in expectations:
        suite.add_expectation(ExpectationConfiguration(**exp))

    # Run validation
    validator = context.get_validator(
        batch_request=gx.core.batch.RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name=suite_name,
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": "default_identifier"},
        ),
        expectation_suite=suite,
    )

    results = validator.validate()
    return results


def run_expectations():
    print("\n🔍  Running Great Expectations validation...\n")
    engine  = get_engine()
    context = gx.get_context()

    # Add pandas datasource
    context.add_datasource(
        name="pandas_datasource",
        class_name="Datasource",
        module_name="great_expectations.datasource",
        execution_engine={
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        data_connectors={
            "runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "module_name": "great_expectations.datasource.data_connector",
                "batch_identifiers": ["default_identifier_name"],
            }
        },
    )

    all_passed = True

    # ── Fault codes ────────────────────────────────────────────────────────────
    df = load_table(engine, "raw_fault_codes")
    results = validate_table(context, df, "fault_codes_suite", [
        {"expectation_type": "expect_column_values_to_not_be_null",
         "kwargs": {"column": "fault_id"}},
        {"expectation_type": "expect_column_values_to_be_unique",
         "kwargs": {"column": "fault_id"}},
        {"expectation_type": "expect_column_values_to_not_be_null",
         "kwargs": {"column": "vehicle_id"}},
        {"expectation_type": "expect_column_values_to_be_in_set",
         "kwargs": {"column": "severity",
                    "value_set": ["critical", "high", "medium", "low"]}},
        {"expectation_type": "expect_table_row_count_to_be_between",
         "kwargs": {"min_value": 1000, "max_value": 20000}},
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "odometer_at_fault_km", "min_value": 0}},
    ])
    passed = results["statistics"]["successful_expectations"]
    total  = results["statistics"]["evaluated_expectations"]
    status = "✅" if results["success"] else "❌"
    print(f"  {status}  fault_codes      — {passed}/{total} expectations passed")
    if not results["success"]:
        all_passed = False

    # ── Repair logs ────────────────────────────────────────────────────────────
    df = load_table(engine, "raw_repair_logs")
    results = validate_table(context, df, "repair_logs_suite", [
        {"expectation_type": "expect_column_values_to_not_be_null",
         "kwargs": {"column": "repair_id"}},
        {"expectation_type": "expect_column_values_to_be_unique",
         "kwargs": {"column": "repair_id"}},
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "mttr_hours",
                    "min_value": 0.01, "max_value": 1000}},
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "parts_cost_usd", "min_value": 0}},
        {"expectation_type": "expect_column_values_to_be_in_set",
         "kwargs": {"column": "root_cause",
                    "value_set": ["manufacturing_defect", "wear_and_tear",
                                  "software_bug", "user_error",
                                  "environmental", "unknown"]}},
    ])
    passed = results["statistics"]["successful_expectations"]
    total  = results["statistics"]["evaluated_expectations"]
    status = "✅" if results["success"] else "❌"
    print(f"  {status}  repair_logs      — {passed}/{total} expectations passed")
    if not results["success"]:
        all_passed = False

    # ── Telemetry ──────────────────────────────────────────────────────────────
    df = load_table(engine, "raw_telemetry")
    results = validate_table(context, df, "telemetry_suite", [
        {"expectation_type": "expect_column_values_to_not_be_null",
         "kwargs": {"column": "telemetry_id"}},
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "battery_soh_pct",
                    "min_value": 0, "max_value": 100}},
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "battery_temp_c",
                    "min_value": -20, "max_value": 80}},
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "avg_regen_efficiency_pct",
                    "min_value": 0, "max_value": 100}},
    ])
    passed = results["statistics"]["successful_expectations"]
    total  = results["statistics"]["evaluated_expectations"]
    status = "✅" if results["success"] else "❌"
    print(f"  {status}  telemetry        — {passed}/{total} expectations passed")
    if not results["success"]:
        all_passed = False

    # ── Summary ────────────────────────────────────────────────────────────────
    print()
    if all_passed:
        print("✅  All data quality contracts passed.\n")
    else:
        print("❌  Some contracts failed — check above.\n")

    # Build data docs (HTML report)
    context.build_data_docs()
    print("📊  HTML report generated at:")
    print("    great_expectations/uncommitted/data_docs/local_site/index.html\n")


if __name__ == "__main__":
    run_expectations()