-- mart_vehicle_health: monthly vehicle health scorecard
with telemetry as (
    select * from {{ ref('stg_telemetry') }}
),
faults as (
    select * from {{ ref('int_vehicle_faults') }}
),
repairs as (
    select * from {{ ref('int_repair_enriched') }}
),
telemetry_monthly as (
    select
        vehicle_id,
        date_trunc('month', recorded_at)::date as period_month,
        round(avg(battery_soh_pct)::numeric,  2) as avg_battery_soh_pct,
        round(min(battery_soh_pct)::numeric,  2) as min_battery_soh_pct,
        round(avg(battery_temp_c)::numeric,   1) as avg_battery_temp_c,
        round(avg(motor_temp_c)::numeric,     1) as avg_motor_temp_c,
        round(avg(regen_efficiency_pct)::numeric, 1) as avg_regen_efficiency_pct,
        max(charge_cycles)                         as max_charge_cycles,
        bool_or(ota_update_pending)                as has_pending_ota
    from telemetry
    group by vehicle_id, date_trunc('month', recorded_at)
),
fault_monthly as (
    select
        vehicle_id,
        fault_month as period_month,
        count(*)    as total_faults,
        count(case when severity = 'critical' then 1 end) as critical_faults
    from faults
    group by vehicle_id, fault_month
),
repair_monthly as (
    select
        vehicle_id,
        repair_month as period_month,
        round(sum(total_repair_cost)::numeric, 2) as total_repair_cost,
        round(avg(mttr_hours)::numeric, 2)        as avg_mttr_hours,
        count(*) as total_repairs
    from repairs
    group by vehicle_id, repair_month
),
joined as (
    select
        t.vehicle_id,
        t.period_month,
        t.avg_battery_soh_pct,
        t.min_battery_soh_pct,
        t.avg_battery_temp_c,
        t.avg_motor_temp_c,
        t.avg_regen_efficiency_pct,
        t.max_charge_cycles,
        t.has_pending_ota,
        coalesce(f.total_faults,    0) as total_faults,
        coalesce(f.critical_faults, 0) as critical_faults,
        coalesce(r.total_repair_cost, 0) as total_repair_cost,
        coalesce(r.avg_mttr_hours,    0) as avg_mttr_hours,
        coalesce(r.total_repairs,     0) as total_repairs
    from telemetry_monthly t
    left join fault_monthly  f using (vehicle_id, period_month)
    left join repair_monthly r using (vehicle_id, period_month)
)
select * from joined
