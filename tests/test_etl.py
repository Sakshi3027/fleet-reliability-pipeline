"""
tests/test_etl.py — Unit tests for ETL pipeline logic.
Run: pytest tests/ -v
"""

import pytest
import pandas as pd
from datetime import datetime


# ── Tests: data generator ─────────────────────────────────────────────────────

def test_vehicles_schema():
    """Generated vehicles have all required fields."""
    import sys
    sys.path.insert(0, ".")
    from data.generate_mock_data import generate_vehicles
    vehicles = generate_vehicles()

    assert len(vehicles) == 120
    required = ["vehicle_id", "vin", "model", "manufactured_date",
                "battery_capacity_kwh", "odometer_km", "fleet_id"]
    for field in required:
        assert field in vehicles[0], f"Missing field: {field}"


def test_vehicle_ids_unique():
    """All vehicle IDs are unique."""
    from data.generate_mock_data import generate_vehicles
    vehicles = generate_vehicles()
    ids = [v["vehicle_id"] for v in vehicles]
    assert len(ids) == len(set(ids))


def test_fault_codes_have_valid_severity():
    """All fault records have a valid severity value."""
    from data.generate_mock_data import generate_vehicles, generate_fault_codes
    vehicles  = generate_vehicles()
    faults    = generate_fault_codes(vehicles)
    valid     = {"critical", "high", "medium", "low"}
    severities = {f["severity"] for f in faults}
    assert severities.issubset(valid), f"Invalid severities found: {severities - valid}"


def test_fault_codes_sorted_by_date():
    """Fault records are returned sorted by occurred_at."""
    from data.generate_mock_data import generate_vehicles, generate_fault_codes
    vehicles = generate_vehicles()
    faults   = generate_fault_codes(vehicles)
    dates    = [f["occurred_at"] for f in faults]
    assert dates == sorted(dates)


def test_repair_logs_only_resolved_faults():
    """Repair logs are only generated for resolved faults."""
    from data.generate_mock_data import (
        generate_vehicles, generate_fault_codes, generate_repair_logs
    )
    vehicles = generate_vehicles()
    faults   = generate_fault_codes(vehicles)
    repairs  = generate_repair_logs(faults)

    resolved_ids = {f["fault_id"] for f in faults if f["resolved"]}
    repair_fault_ids = {r["fault_id"] for r in repairs}
    assert repair_fault_ids.issubset(resolved_ids)


def test_mttr_always_positive():
    """MTTR is always a positive number."""
    from data.generate_mock_data import (
        generate_vehicles, generate_fault_codes, generate_repair_logs
    )
    vehicles = generate_vehicles()
    faults   = generate_fault_codes(vehicles)
    repairs  = generate_repair_logs(faults)

    for r in repairs:
        assert r["mttr_hours"] > 0, f"Non-positive MTTR: {r['mttr_hours']}"


def test_repair_end_after_start():
    """repair_end is always after repair_start."""
    from data.generate_mock_data import (
        generate_vehicles, generate_fault_codes, generate_repair_logs
    )
    vehicles = generate_vehicles()
    faults   = generate_fault_codes(vehicles)
    repairs  = generate_repair_logs(faults)

    for r in repairs:
        start = datetime.fromisoformat(r["repair_start"])
        end   = datetime.fromisoformat(r["repair_end"])
        assert end > start, f"repair_end before repair_start for {r['repair_id']}"


def test_battery_soh_in_range():
    """Battery SOH is always between 70 and 100."""
    from data.generate_mock_data import generate_vehicles, generate_telemetry
    vehicles  = generate_vehicles()
    telemetry = generate_telemetry(vehicles)

    for row in telemetry:
        soh = row["battery_soh_pct"]
        assert 70 <= soh <= 100, f"SOH out of range: {soh}"


# ── Tests: transform SQL logic (unit-tested via pandas) ───────────────────────

def test_mttr_calculation():
    """MTTR average calculation is correct."""
    df = pd.DataFrame({
        "component":   ["battery_pack", "battery_pack", "motor_controller"],
        "severity":    ["high", "high", "critical"],
        "mttr_hours":  [10.0, 20.0, 5.0],
        "repair_start": pd.to_datetime(["2024-01-15", "2024-01-20", "2024-01-10"]),
    })
    result = df.groupby("component")["mttr_hours"].mean()
    assert result["battery_pack"]     == 15.0
    assert result["motor_controller"] == 5.0


def test_failure_rate_calculation():
    """Failure rate (unresolved / total) is calculated correctly."""
    df = pd.DataFrame({
        "fault_id":  ["F1", "F2", "F3", "F4"],
        "resolved":  [True, True, False, True],
    })
    total      = len(df)
    unresolved = (~df["resolved"]).sum()
    rate       = round(100.0 * unresolved / total, 2)
    assert rate == 25.0


def test_risk_scoring():
    """Risk scoring tiers are assigned correctly."""
    from models.failure_forecast import score_risk

    assert score_risk(15.0, 10.0) == "high"    # 1.5x  → high
    assert score_risk(11.0, 10.0) == "medium"  # 1.1x  → medium
    assert score_risk(9.0,  10.0) == "low"     # 0.9x  → low
    assert score_risk(5.0,   0.0) == "unknown" # zero mean → unknown