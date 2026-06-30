-- Intermediate: join trips to zone names for BOTH pickup and dropoff, and apply
-- the data-quality filters. Invalid records are dropped here so the marts only
-- ever see clean, enriched trips.
--
-- Materialised as an INCREMENTAL table (delete+insert on trip_id): each run only
-- processes newly-loaded rows (watermark on _loaded_at), and delete+insert removes
-- any existing rows with the same trip_id before inserting — so reloading a month
-- or a cross-file duplicate trip never double-counts.
--
-- Filters: trip_distance > 0, fare_amount > 0, passenger_count > 0, and
-- trip_duration_minutes within the parametrised [min, max] range (default 1..180).

with trips as (
    select * from {{ ref('stg_yellow_trips') }}
),

zones as (
    select * from {{ ref('stg_taxi_zones') }}
)

select
    t.trip_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.trip_duration_minutes,
    t.passenger_count,
    t.trip_distance,
    t.payment_type,
    t.fare_amount,
    t.tip_amount,
    t.total_amount,

    t.pickup_location_id,
    puz.borough   as pickup_borough,
    puz.zone_name as pickup_zone,

    t.dropoff_location_id,
    doz.borough   as dropoff_borough,
    doz.zone_name as dropoff_zone,

    t._loaded_at
from trips t
left join zones puz on t.pickup_location_id  = puz.location_id
left join zones doz on t.dropoff_location_id = doz.location_id
where t.trip_distance > 0
  and t.fare_amount   > 0
  and t.passenger_count > 0
  and t.trip_duration_minutes >= {{ var('min_trip_duration_minutes') }}
  and t.trip_duration_minutes <= {{ var('max_trip_duration_minutes') }}
{% if is_incremental() %}
  -- only process rows loaded since the last run (ingestion stamps _loaded_at)
  and t._loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}
