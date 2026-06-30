-- Volume & economics per month (valid trips only, from the fact table).
select
    month(pickup_datetime)                as month,
    count(*)                              as trips,
    round(avg(trip_distance), 2)          as avg_miles,
    round(avg(fare_amount), 2)            as avg_fare,
    round(avg(total_amount), 2)           as avg_total,
    round(sum(total_amount) / 1e6, 1)     as revenue_millions
from analytics_marts.fct_trips
group by 1
order by 1;
