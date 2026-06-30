-- Q2 · Demand pattern by hour of day.
-- Trips, avg fare, and avg distance bucketed by pickup hour (0-23), plus each
-- hour's share of total trips.
with hourly as (
    select
        hour(pickup_datetime)            as pickup_hour,
        count(*)                         as total_trips,
        round(avg(fare_amount), 2)       as avg_fare,
        round(avg(trip_distance), 2)     as avg_distance
    from analytics_marts.fct_trips        -- adjust schema prefix to your target
    group by 1
)
select
    pickup_hour,
    total_trips,
    avg_fare,
    avg_distance,
    round(100.0 * total_trips / sum(total_trips) over (), 2) as pct_of_trips
from hourly
order by pickup_hour;
