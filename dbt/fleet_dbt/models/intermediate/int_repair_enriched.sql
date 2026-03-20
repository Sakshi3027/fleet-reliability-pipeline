with repairs as (
    select * from {{ ref('stg_repair_logs') }}
),
faults as (
    select * from {{ ref('stg_fault_codes') }}
),
vehicles as (
    select * from {{ ref('stg_vehicles') }}
),
joined as (
    select
        r.repair_id,
        r.fault_id,
        r.vehicle_id,
        r.component,
        r.severity,
        r.repair_start,
        r.repair_end,
        r.mttr_hours,
        r.technician_id,
        r.service_center,
        r.parts_replaced,
        r.parts_cost_usd,
        r.labor_hours,
        r.labor_cost_usd,
        r.parts_cost_usd + r.labor_cost_usd as total_repair_cost,
        r.root_cause,
        r.is_warranty_claim,
        date_trunc('month', r.repair_start)::date as repair_month,
        f.fault_code,
        f.occurred_at as fault_occurred_at,
        v.vehicle_model,
        v.fleet_id
    from repairs r
    left join faults   f on f.fault_id   = r.fault_id
    left join vehicles v on v.vehicle_id = r.vehicle_id
)
select * from joined
