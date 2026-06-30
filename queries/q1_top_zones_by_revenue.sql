-- Q1 · Top 10 pickup zones by total revenue.
-- Reads the pre-aggregated mart, so it's a trivial, fast lookup.
select
    revenue_rank,
    borough,
    zone_name,
    total_trips,
    avg_fare,
    total_revenue,
    is_high_volume_zone
from analytics_marts.agg_zone_performance   -- adjust schema prefix to your target
order by revenue_rank
limit 10;
