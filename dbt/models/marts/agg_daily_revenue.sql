-- Daily revenue aggregation: one row per calendar day.
select
    cast(pickup_datetime as date)                                   as revenue_date,
    count(*)                                                        as total_trips,
    round(sum(fare_amount), 2)                                      as total_fare,
    round(avg(fare_amount), 2)                                      as avg_fare,
    round(sum(tip_amount), 2)                                       as total_tips,
    round(100 * sum(tip_amount) / nullif(sum(fare_amount), 0), 2)   as tip_rate_pct
from {{ ref('fct_trips') }}
group by 1
order by 1
