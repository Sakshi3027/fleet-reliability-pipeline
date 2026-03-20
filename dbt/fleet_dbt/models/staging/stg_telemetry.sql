with source as (
    select * from {{ source('raw', 'raw_telemetry') }}
),
renamed as (
    select
        telemetry_id,
        vehicle_id,
        recorded_at::timestamptz      as recorded_at,
        battery_soh_pct::numeric      as battery_soh_pct,
        battery_temp_c::numeric       as battery_temp_c,
        motor_temp_c::numeric         as motor_temp_c,
        odometer_km,
        charge_cycles,
        avg_regen_efficiency_pct::numeric as regen_efficiency_pct,
        ota_version,
        ota_update_pending::boolean   as ota_update_pending,
        hvac_hours::numeric           as hvac_hours
    from source
    where telemetry_id is not null
      and battery_soh_pct between 0 and 100
)
select * from renamed
