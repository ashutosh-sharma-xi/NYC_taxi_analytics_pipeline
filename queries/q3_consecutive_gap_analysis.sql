-- Q3 · Consecutive-trip gap analysis (window functions).
-- For each pickup zone, order trips by pickup time and measure the idle gap (in
-- minutes) to the PREVIOUS trip in that zone, using LAG. Surfaces the longest
-- demand lulls per zone — useful for fleet positioning.
--
-- ┌── Making this performant on 38M rows (Snowflake-specific) ─────────────────┐
-- • Clustering key — the LAG partitions by pickup_location_id and orders by
--   pickup_datetime. Clustering fct_trips on (pu_location_id, pickup_datetime)
--   co-locates each zone's rows in the same micro-partitions, so the sort feeding
--   the window is cheap and scans prune well:
--     ALTER TABLE fct_trips CLUSTER BY (pu_location_id, pickup_datetime);
-- • Materialisation — this is a full-scan window, run repeatedly. Precompute it as
--   an incremental mart (e.g. agg_zone_gaps) so dashboards read a small table
--   instead of re-scanning 38M rows every time. (A plain view would re-run it.)
-- • Result cache — identical re-runs return instantly from Snowflake's 24h result
--   cache (no compute) as long as the underlying data is unchanged.
-- • Warehouse sizing — window sorts are memory-heavy; bump the warehouse to M/L for
--   the run (it auto-suspends after), rather than running long on XS and spilling.
-- • Search Optimization — NOT the right tool here: it accelerates selective
--   point/range lookups, not a full-table window scan. Skip it for this query.
-- └───────────────────────────────────────────────────────────────────────────┘
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
