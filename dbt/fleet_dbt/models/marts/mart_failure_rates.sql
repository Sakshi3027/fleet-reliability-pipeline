-- mart_failure_rates: fault counts and resolution rates by vehicle/component/month
with faults as (
    select * from {{ ref('int_vehicle_faults') }}
),
aggregated as (
    select
        vehicle_id,
        vehicle_model,
        fleet_id,
        component,
        fault_month                                        as period_month,
        count(*)                                           as total_faults,
        count(case when severity = 'critical' then 1 end) as critical_faults,
        count(case when severity = 'high'     then 1 end) as high_faults,
        count(case when severity = 'medium'   then 1 end) as medium_faults,
        count(case when severity = 'low'      then 1 end) as low_faults,
        count(case when is_resolved           then 1 end) as resolved_faults,
        count(case when not is_resolved       then 1 end) as open_faults,
        round(
            100.0 * count(case when not is_resolved then 1 end)
            / nullif(count(*), 0), 2
        )                                                  as open_rate_pct,
        round(avg(vehicle_age_years)::numeric, 1)          as avg_vehicle_age_years
    from faults
    group by vehicle_id, vehicle_model, fleet_id, component, fault_month
)
select * from aggregated
