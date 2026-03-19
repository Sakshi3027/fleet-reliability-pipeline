"""
Mock data generator for Fleet Reliability Pipeline.
Generates realistic EV fault codes, repair logs, and vehicle telemetry.

Run from project root:
    python data/generate_mock_data.py
"""

import random
import json
import csv
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

# ── Configuration ─────────────────────────────────────────────────────────────
NUM_VEHICLES = 120
START_DATE   = datetime(2023, 1, 1)
END_DATE     = datetime(2024, 12, 31)
OUTPUT_DIR   = Path(__file__).parent / "raw"

# ── Domain Data ───────────────────────────────────────────────────────────────
VEHICLE_MODELS = ["EV-Sedan-S", "EV-SUV-X", "EV-Truck-T", "EV-Van-V"]

COMPONENTS = [
    "battery_pack", "motor_controller", "charging_system",
    "brake_system", "thermal_management", "ota_module",
    "suspension", "hvac",
]

FAULT_CODES = {
    "battery_pack": [
        ("BMS_001", "Cell voltage imbalance"),
        ("BMS_002", "SOH below threshold"),
        ("BMS_003", "Thermal runaway warning"),
        ("BMS_004", "Charge cycle anomaly"),
    ],
    "motor_controller": [
        ("MC_001", "Phase current fault"),
        ("MC_002", "IGBT overtemperature"),
        ("MC_003", "Encoder signal lost"),
        ("MC_004", "Torque limit exceeded"),
    ],
    "charging_system": [
        ("CHG_001", "AC input overvoltage"),
        ("CHG_002", "DC fast charge timeout"),
        ("CHG_003", "Onboard charger fault"),
        ("CHG_004", "Pilot signal error"),
    ],
    "brake_system": [
        ("BRK_001", "Regen brake calibration"),
        ("BRK_002", "ABS sensor fault"),
        ("BRK_003", "Hydraulic pressure low"),
    ],
    "thermal_management": [
        ("THM_001", "Coolant level low"),
        ("THM_002", "Pump flow rate fault"),
        ("THM_003", "Cabin temp deviation"),
    ],
    "ota_module": [
        ("OTA_001", "Update install failed"),
        ("OTA_002", "Signature mismatch"),
        ("OTA_003", "Rollback triggered"),
    ],
    "suspension": [
        ("SUS_001", "Air spring pressure"),
        ("SUS_002", "Damper response slow"),
    ],
    "hvac": [
        ("HVC_001", "Compressor fault"),
        ("HVC_002", "Refrigerant pressure low"),
    ],
}

SEVERITY_WEIGHTS = {
    "critical": 0.10,
    "high":     0.25,
    "medium":   0.40,
    "low":      0.25,
}

TECHNICIAN_IDS  = [f"TECH_{i:03d}" for i in range(1, 21)]
SERVICE_CENTERS = [
    "SC_Austin_01", "SC_Fremont_02", "SC_Chicago_03",
    "SC_Dallas_04", "SC_Seattle_05",
]
ROOT_CAUSES = [
    "manufacturing_defect", "wear_and_tear", "software_bug",
    "user_error", "environmental", "unknown",
]


# ── Helpers ───────────────────────────────────────────────────────────────────
def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


# ── Generators ────────────────────────────────────────────────────────────────
def generate_vehicles() -> list[dict]:
    vehicles = []
    for i in range(1, NUM_VEHICLES + 1):
        manufactured = random_date(datetime(2021, 1, 1), datetime(2023, 6, 1))
        vehicles.append({
            "vehicle_id":           f"VH_{i:04d}",
            "vin":                  f"5YJ{random.randint(100000, 999999)}EF{i:04d}",
            "model":                random.choice(VEHICLE_MODELS),
            "manufactured_date":    manufactured.date().isoformat(),
            "battery_capacity_kwh": random.choice([60, 75, 100, 120]),
            "odometer_km":          random.randint(5_000, 180_000),
            "fleet_id":             f"FLEET_{random.randint(1, 5):02d}",
        })
    return vehicles


def generate_fault_codes(vehicles: list[dict]) -> list[dict]:
    records = []
    for v in vehicles:
        n_faults = random.randint(2, 18)
        for _ in range(n_faults):
            component         = random.choice(COMPONENTS)
            code, description = random.choice(FAULT_CODES[component])
            severity          = random.choices(
                list(SEVERITY_WEIGHTS.keys()),
                weights=list(SEVERITY_WEIGHTS.values()),
            )[0]
            occurred_at = random_date(START_DATE, END_DATE)
            records.append({
                "fault_id":             f"FLT_{len(records) + 1:06d}",
                "vehicle_id":           v["vehicle_id"],
                "fault_code":           code,
                "component":            component,
                "description":          description,
                "severity":             severity,
                "occurred_at":          occurred_at.isoformat(),
                "odometer_at_fault_km": max(0, v["odometer_km"] - random.randint(0, 50_000)),
                "resolved":             random.choices([True, False], weights=[0.75, 0.25])[0],
            })
    return sorted(records, key=lambda r: r["occurred_at"])


def generate_repair_logs(fault_records: list[dict]) -> list[dict]:
    logs = []
    mttr_range = {
        "critical": (2,   12),
        "high":     (6,   48),
        "medium":   (12, 120),
        "low":      (24, 240),
    }
    for fault in fault_records:
        if not fault["resolved"]:
            continue
        fault_time   = datetime.fromisoformat(fault["occurred_at"])
        lo, hi       = mttr_range[fault["severity"]]
        repair_hours = random.uniform(lo, hi)
        repair_start = fault_time + timedelta(hours=random.uniform(0.5, 6))
        repair_end   = repair_start + timedelta(hours=repair_hours)
        labor_hours  = round(random.uniform(0.5, 12), 2)
        parts_used   = random.random() > 0.3

        logs.append({
            "repair_id":      f"RPR_{len(logs) + 1:06d}",
            "fault_id":       fault["fault_id"],
            "vehicle_id":     fault["vehicle_id"],
            "component":      fault["component"],
            "severity":       fault["severity"],
            "repair_start":   repair_start.isoformat(),
            "repair_end":     repair_end.isoformat(),
            "mttr_hours":     round(repair_hours, 2),
            "technician_id":  random.choice(TECHNICIAN_IDS),
            "service_center": random.choice(SERVICE_CENTERS),
            "parts_replaced": parts_used,
            "parts_cost_usd": round(random.uniform(50, 4_500), 2) if parts_used else 0.0,
            "labor_hours":    labor_hours,
            "labor_cost_usd": round(labor_hours * random.uniform(85, 150), 2),
            "root_cause":     random.choice(ROOT_CAUSES),
            "warranty_claim": random.choice([True, False]),
        })
    return logs


def generate_telemetry(vehicles: list[dict]) -> list[dict]:
    records = []
    for v in vehicles:
        current     = START_DATE
        battery_soh = random.uniform(92, 100)
        while current <= END_DATE:
            battery_soh = max(70.0, battery_soh - random.uniform(0, 0.08))
            records.append({
                "telemetry_id":              f"TEL_{len(records) + 1:07d}",
                "vehicle_id":               v["vehicle_id"],
                "recorded_at":              current.isoformat(),
                "battery_soh_pct":          round(battery_soh, 2),
                "battery_temp_c":           round(random.uniform(18, 42), 1),
                "motor_temp_c":             round(random.uniform(20, 85), 1),
                "odometer_km":              max(0, v["odometer_km"] - random.randint(0, 30_000)),
                "charge_cycles":            random.randint(50, 800),
                "avg_regen_efficiency_pct": round(random.uniform(70, 95), 1),
                "ota_version":              f"v{random.randint(4,6)}.{random.randint(0,9)}.{random.randint(0,9)}",
                "ota_update_pending":       random.choices([True, False], weights=[0.2, 0.8])[0],
                "hvac_hours":               round(random.uniform(0, 2_000), 1),
            })
            current += timedelta(weeks=1)
    return records


# ── Writers ───────────────────────────────────────────────────────────────────
def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  ✓  {len(rows):>7,} rows  →  {path.name}")


def write_json(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, default=str)
    print(f"  ✓  {len(rows):>7,} rows  →  {path.name}")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n🚗  Generating mock fleet data...\n")

    vehicles  = generate_vehicles()
    write_csv(vehicles, OUTPUT_DIR / "vehicles.csv")

    faults    = generate_fault_codes(vehicles)
    write_csv(faults, OUTPUT_DIR / "fault_codes.csv")

    repairs   = generate_repair_logs(faults)
    write_json(repairs, OUTPUT_DIR / "repair_logs.json")

    telemetry = generate_telemetry(vehicles)
    write_csv(telemetry, OUTPUT_DIR / "vehicle_telemetry.csv")

    print(
        f"\n✅  Done — {len(vehicles)} vehicles | {len(faults):,} faults | "
        f"{len(repairs):,} repairs | {len(telemetry):,} telemetry rows\n"
    )