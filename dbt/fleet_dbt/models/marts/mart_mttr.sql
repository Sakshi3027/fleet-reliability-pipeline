-- mart_mttr: mean time to repair by component, severity, and month
with repairs as (
    select * from {{ ref('int_repair_enriched') }}
),
aggregated as (
    select
        component,
        severity,
        repair_month                                        as period_month,
        round(avg(mttr_hours)::numeric, 2)                 as avg_mttr_hours,
        round(
            percentile_cont(0.5) within group (
                order by mttr_hours
            )::numeric, 2
        )                                                   as median_mttr_hours,
        round(min(mttr_hours)::numeric, 2)                 as min_mttr_hours,
        round(max(mttr_hours)::numeric, 2)                 as max_mttr_hours,
        count(*)                                           as total_repairs,
        sum(total_repair_cost)                             as total_cost,
        round(avg(total_repair_cost)::numeric, 2)          as avg_repair_cost,
        count(case when is_warranty_claim then 1 end)      as warranty_claims
    from repairs
    group by component, severity, repair_month
)
select * from aggregated
