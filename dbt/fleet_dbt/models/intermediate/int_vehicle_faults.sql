-- int_vehicle_faults: joins faults with vehicle info for enriched analysis
with faults as (
    select * from {{ ref('stg_fault_codes') }}
),
vehicles as (
    select * from {{ ref('stg_vehicles') }}
),
joined as (
    select
        f.fault_id,
        f.vehicle_id,
        f.fault_code,
        f.component,
        f.fault_description,
        f.severity,
        f.occurred_at,
        f.odometer_at_fault_km,
        f.is_resolved,
        date_trunc('month', f.occurred_at)::date as fault_month,
        v.vehicle_model,
        v.fleet_id,
        v.battery_capacity_kwh,
        v.manufactured_date,
        date_part('year', age(f.occurred_at, v.manufactured_date::timestamptz))
            as vehicle_age_years
    from faults f
    left join vehicles v using (vehicle_id)
)
select * from joined
