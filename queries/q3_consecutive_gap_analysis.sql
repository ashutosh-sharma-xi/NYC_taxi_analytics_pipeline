-- Q3 · Consecutive-trip gap analysis (window functions).
-- For each pickup zone, order trips by pickup time and measure the idle gap (in
-- minutes) to the PREVIOUS trip in that zone, using LAG. Surfaces the longest
-- demand lulls per zone — useful for fleet positioning.
with ordered as (
    select
        pickup_location_id,
        pickup_datetime,
        lag(pickup_datetime) over (
            partition by pickup_location_id
            order by pickup_datetime
        ) as prev_pickup_datetime
    from analytics_marts.fct_trips           -- adjust schema prefix to your target
),

gaps as (
    select
        pickup_location_id,
        pickup_datetime,
        prev_pickup_datetime,
        datediff('minute', prev_pickup_datetime, pickup_datetime) as gap_minutes
    from ordered
    where prev_pickup_datetime is not null
)

select
    g.pickup_location_id,
    z.zone_name,
    g.prev_pickup_datetime,
    g.pickup_datetime,
    g.gap_minutes,
    rank() over (partition by g.pickup_location_id order by g.gap_minutes desc) as gap_rank
from gaps g
left join analytics_marts.dim_zones z
    on g.pickup_location_id = z.location_id
qualify gap_rank <= 5          -- top 5 longest gaps per zone
order by g.pickup_location_id, gap_rank;
