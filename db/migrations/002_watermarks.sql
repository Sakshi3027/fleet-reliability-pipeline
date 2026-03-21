-- Pipeline watermark table
-- Tracks the last successful load timestamp per table
CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    table_name      TEXT PRIMARY KEY,
    last_loaded_at  TIMESTAMPTZ NOT NULL DEFAULT '2000-01-01 00:00:00+00',
    rows_loaded     INTEGER DEFAULT 0,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Seed initial watermarks for all raw tables
INSERT INTO pipeline_watermarks (table_name, last_loaded_at)
VALUES
    ('raw_fault_codes',  '2000-01-01 00:00:00+00'),
    ('raw_repair_logs',  '2000-01-01 00:00:00+00'),
    ('raw_telemetry',    '2000-01-01 00:00:00+00'),
    ('raw_vehicles',     '2000-01-01 00:00:00+00')
ON CONFLICT (table_name) DO NOTHING;