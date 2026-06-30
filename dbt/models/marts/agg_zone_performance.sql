-- Per pickup-zone performance, with a revenue rank (window function) and a
-- high-volume flag. The flag uses an average-trips-per-month threshold so it
-- behaves correctly whether 1 month or all 12 are loaded.

with by_zone as (
    select
        f.pickup_location_id,
        z.borough,
        z.zone_name,
        count(*)                          as total_trips,
        count(distinct date_trunc('month', f.pickup_datetime)) as months_observed,
        round(avg(f.trip_distance), 2)    as avg_trip_distance,
        round(avg(f.fare_amount), 2)      as avg_fare,
        round(sum(f.total_amount), 2)     as total_revenue
    from {{ ref('fct_trips') }} f
    left join {{ ref('dim_zones') }} z
        on f.pickup_location_id = z.location_id
    group by 1, 2, 3
)

select
    pickup_location_id,
    borough,
    zone_name,
    total_trips,
    avg_trip_distance,
    avg_fare,
    total_revenue,
    rank() over (order by total_revenue desc)                       as revenue_rank,
    (total_trips / nullif(months_observed, 0)) > 10000              as is_high_volume_zone
from by_zone
order by revenue_rank
