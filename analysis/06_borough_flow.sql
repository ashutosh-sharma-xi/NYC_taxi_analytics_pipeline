-- Pickup -> dropoff borough flows: where trips start and where they end.
-- Top 15 borough pairs by volume.
select
    pickup_borough,
    dropoff_borough,
    count(*)                                           as trips,
    round(100.0 * count(*) / sum(count(*)) over (), 1) as pct_of_trips,
    round(avg(trip_distance), 2)                       as avg_miles,
    round(avg(total_amount), 2)                        as avg_total
from analytics_marts.fct_trips
where pickup_borough is not null
  and dropoff_borough is not null
group by 1, 2
order by trips desc
limit 15;
