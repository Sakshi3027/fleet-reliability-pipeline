with source as (
    select * from {{ source('raw', 'raw_vehicles') }}
),
renamed as (
    select
        vehicle_id,
        vin,
        model                   as vehicle_model,
        manufactured_date::date as manufactured_date,
        battery_capacity_kwh,
        odometer_km,
        fleet_id
    from source
    where vehicle_id is not null
)
select * from renamed
