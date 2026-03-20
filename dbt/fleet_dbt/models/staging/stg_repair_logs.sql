with source as (
    select * from {{ source('raw', 'raw_repair_logs') }}
),
renamed as (
    select
        repair_id,
        fault_id,
        vehicle_id,
        component,
        severity,
        repair_start::timestamptz as repair_start,
        repair_end::timestamptz   as repair_end,
        mttr_hours::numeric       as mttr_hours,
        technician_id,
        service_center,
        parts_replaced::boolean   as parts_replaced,
        parts_cost_usd::numeric   as parts_cost_usd,
        labor_hours::numeric      as labor_hours,
        labor_cost_usd::numeric   as labor_cost_usd,
        root_cause,
        warranty_claim::boolean   as is_warranty_claim
    from source
    where repair_id is not null
      and mttr_hours > 0
      and repair_end > repair_start
)
select * from renamed
