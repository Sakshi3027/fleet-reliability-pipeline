-- mart_anomaly_alerts: flags vehicles with abnormal fault spikes
-- Uses 2-standard-deviation rule per vehicle per component
with monthly_faults as (
    select
        vehicle_id,
        vehicle_model,
        fleet_id,
        component,
        period_month,
        total_faults,
        critical_faults
    from {{ ref('mart_failure_rates') }}
),

vehicle_baselines as (
    select
        vehicle_id,
        component,
        avg(total_faults)                    as avg_faults,
        stddev(total_faults)                 as stddev_faults,
        max(total_faults)                    as max_faults,
        count(*)                             as months_observed
    from monthly_faults
    group by vehicle_id, component
    having count(*) >= 3  -- need at least 3 months of history
),

anomalies as (
    select
        f.vehicle_id,
        f.vehicle_model,
        f.fleet_id,
        f.component,
        f.period_month,
        f.total_faults,
        f.critical_faults,
        b.avg_faults,
        b.stddev_faults,
        round((f.total_faults - b.avg_faults)::numeric, 2)
            as deviation_from_mean,
        case
            when b.stddev_faults = 0 or b.stddev_faults is null then 0
            else round(
                ((f.total_faults - b.avg_faults) / b.stddev_faults)::numeric,
                2
            )
        end as z_score,
        case
            when b.stddev_faults = 0 or b.stddev_faults is null then false
            when (f.total_faults - b.avg_faults) / b.stddev_faults >= 2 then true
            else false
        end as is_anomaly,
        case
            when b.stddev_faults is null or b.stddev_faults = 0 then 'normal'
            when (f.total_faults - b.avg_faults) / b.stddev_faults >= 3 then 'critical'
            when (f.total_faults - b.avg_faults) / b.stddev_faults >= 2 then 'warning'
            else 'normal'
        end as alert_level
    from monthly_faults f
    inner join vehicle_baselines b
        on b.vehicle_id = f.vehicle_id
        and b.component = f.component
)

select * from anomalies
where is_anomaly = true
order by z_score desc, period_month desc
