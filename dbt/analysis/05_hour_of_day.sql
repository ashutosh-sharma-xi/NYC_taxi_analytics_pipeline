-- Demand pattern by pickup hour (0-23): trips, economics, and share of day.
with hourly as (
    select
        hour(pickup_datetime)          as pickup_hour,
        count(*)                       as trips,
        round(avg(fare_amount), 2)     as avg_fare,
        round(avg(trip_distance), 2)   as avg_miles,
        round(avg(trip_duration_minutes), 1) as avg_duration_min
    from analytics_marts.fct_trips
    group by 1
)
select
    pickup_hour,
    trips,
    round(100.0 * trips / sum(trips) over (), 2) as pct_of_trips,
    avg_fare,
    avg_miles,
    avg_duration_min
from hourly
order by pickup_hour;
