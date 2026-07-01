-- Top 15 pickup zones by trip count, with revenue and the high-volume flag.
-- Uses the pre-aggregated mart (already joined to zone names + ranked).
select
    revenue_rank,
    borough,
    zone_name,
    total_trips,
    avg_trip_distance,
    avg_fare,
    total_revenue,
    is_high_volume_zone
from analytics_marts.agg_zone_performance
order by total_trips desc
limit 15;
