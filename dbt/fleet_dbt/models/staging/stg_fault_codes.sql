with source as (
    select * from {{ source('raw', 'raw_fault_codes') }}
),
renamed as (
    select
        fault_id,
        vehicle_id,
        fault_code,
        component,
        description              as fault_description,
        severity,
        occurred_at::timestamptz as occurred_at,
        odometer_at_fault_km,
        resolved::boolean        as is_resolved
    from source
    where fault_id is not null
      and vehicle_id is not null
      and severity in ('critical', 'high', 'medium', 'low')
)
select * from renamed
