-- Per pickup-zone performance AT MONTHLY GRAIN (one row per zone per month).
--
-- Grain: pickup_location_id x file_month. We use file_month (captured at ingestion
-- from the source file name) because the data is published one file per month, so
-- it's the natural, clean monthly bucket — independent of a few boundary/garbage
-- pickup timestamps.
--
-- REVENUE RANK is WITHIN each month:
--   rank() over (partition by file_month order by total_revenue desc)
-- This answers "which zones led in a given month?" and exposes seasonality /
-- consistency (is a zone reliably top, or a one-off spike?). Rolling this up to
-- a yearly or all-time leaderboard is a cheap further GROUP BY on this table.
--
-- is_high_volume_zone is now a true per-month test (>10k trips in the month),
-- since the grain is already monthly — no averaging needed.

with by_zone_month as (
    select
        f.file_month,
        f.pickup_location_id,
        z.borough,
        z.zone_name,
        count(*)                          as total_trips,
        round(avg(f.trip_distance), 2)    as avg_trip_distance,
        round(avg(f.fare_amount), 2)      as avg_fare,
        round(sum(f.total_amount), 2)     as total_revenue
    from {{ ref('fct_trips') }} f
    left join {{ ref('dim_zones') }} z
        on f.pickup_location_id = z.location_id
    group by 1, 2, 3, 4
)

select
    -- surrogate key for the zone x month grain (primary key of this model)
    md5(pickup_location_id || '|' || file_month)                       as zone_month_key,
    file_month,
    pickup_location_id,
    borough,
    zone_name,
    total_trips,
    avg_trip_distance,
    avg_fare,
    total_revenue,
    rank() over (partition by file_month order by total_revenue desc)  as revenue_rank,
    (total_trips > 10000)                                              as is_high_volume_zone
from by_zone_month
order by file_month, revenue_rank
