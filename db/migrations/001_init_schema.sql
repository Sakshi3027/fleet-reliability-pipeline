-- ─────────────────────────────────────────────
-- Fleet Reliability Pipeline — Initial Schema
-- ─────────────────────────────────────────────

-- Raw tables (exactly mirrors what lands from CSV/JSON)

CREATE TABLE IF NOT EXISTS raw_vehicles (
    vehicle_id           TEXT PRIMARY KEY,
    vin                  TEXT UNIQUE NOT NULL,
    model                TEXT,
    manufactured_date    DATE,
    battery_capacity_kwh INTEGER,
    odometer_km          INTEGER,
    fleet_id             TEXT,
    loaded_at            TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_fault_codes (
    fault_id             TEXT PRIMARY KEY,
    vehicle_id           TEXT,
    fault_code           TEXT,
    component            TEXT,
    description          TEXT,
    severity             TEXT,
    occurred_at          TIMESTAMPTZ,
    odometer_at_fault_km INTEGER,
    resolved             BOOLEAN,
    loaded_at            TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_repair_logs (
    repair_id       TEXT PRIMARY KEY,
    fault_id        TEXT,
    vehicle_id      TEXT,
    component       TEXT,
    severity        TEXT,
    repair_start    TIMESTAMPTZ,
    repair_end      TIMESTAMPTZ,
    mttr_hours      NUMERIC(8,2),
    technician_id   TEXT,
    service_center  TEXT,
    parts_replaced  BOOLEAN,
    parts_cost_usd  NUMERIC(10,2),
    labor_hours     NUMERIC(6,2),
    labor_cost_usd  NUMERIC(10,2),
    root_cause      TEXT,
    warranty_claim  BOOLEAN,
    loaded_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_telemetry (
    telemetry_id              TEXT PRIMARY KEY,
    vehicle_id                TEXT,
    recorded_at               TIMESTAMPTZ,
    battery_soh_pct           NUMERIC(5,2),
    battery_temp_c            NUMERIC(5,1),
    motor_temp_c              NUMERIC(5,1),
    odometer_km               INTEGER,
    charge_cycles             INTEGER,
    avg_regen_efficiency_pct  NUMERIC(5,1),
    ota_version               TEXT,
    ota_update_pending        BOOLEAN,
    hvac_hours                NUMERIC(8,1),
    loaded_at                 TIMESTAMPTZ DEFAULT NOW()
);

-- ─────────────────────────────────────────────
-- Mart tables (transformed, used by dashboard)
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS mart_mttr_by_component (
    component          TEXT,
    severity           TEXT,
    avg_mttr_hours     NUMERIC(8,2),
    median_mttr_hours  NUMERIC(8,2),
    total_repairs      INTEGER,
    period_month       DATE,
    updated_at         TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (component, severity, period_month)
);

CREATE TABLE IF NOT EXISTS mart_failure_rates (
    vehicle_id        TEXT,
    component         TEXT,
    period_month      DATE,
    fault_count       INTEGER,
    critical_count    INTEGER,
    resolved_count    INTEGER,
    failure_rate_pct  NUMERIC(5,2),
    updated_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (vehicle_id, component, period_month)
);

CREATE TABLE IF NOT EXISTS mart_vehicle_health (
    vehicle_id          TEXT,
    period_month        DATE,
    avg_battery_soh_pct NUMERIC(5,2),
    avg_battery_temp_c  NUMERIC(5,1),
    avg_motor_temp_c    NUMERIC(5,1),
    total_fault_count   INTEGER,
    total_repair_cost   NUMERIC(10,2),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (vehicle_id, period_month)
);