-- Staging: rename to snake_case, cast types, derive trip_duration_minutes,
-- and build a stable surrogate trip_id (the raw data has no natural key).
-- No filtering here — staging stays 1:1 with the source. Cleaning happens in
-- the intermediate layer.

with source as (
    select * from {{ source('raw_taxi', 'yellow_tripdata') }}
)

select
    md5(
        coalesce(to_varchar(tpep_pickup_datetime), '')  || '|' ||
        coalesce(to_varchar(tpep_dropoff_datetime), '') || '|' ||
        coalesce(to_varchar(pulocationid), '')           || '|' ||
        coalesce(to_varchar(dolocationid), '')           || '|' ||
        coalesce(to_varchar(fare_amount), '')            || '|' ||
        coalesce(to_varchar(total_amount), '')           || '|' ||
        coalesce(to_varchar(trip_distance), '')          || '|' ||
        coalesce(to_varchar(passenger_count), '')
    )                                                       as trip_id,

    tpep_pickup_datetime                                    as pickup_datetime,
    tpep_dropoff_datetime                                   as dropoff_datetime,
    cast(passenger_count as integer)                        as passenger_count,
    cast(trip_distance as float)                            as trip_distance,
    cast(pulocationid as integer)                           as pickup_location_id,
    cast(dolocationid as integer)                           as dropoff_location_id,
    cast(payment_type as integer)                           as payment_type,
    cast(fare_amount as float)                              as fare_amount,
    cast(tip_amount as float)                               as tip_amount,
    cast(total_amount as float)                             as total_amount,

    datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime)
                                                            as trip_duration_minutes,

    file_month,      -- source file's month (provenance / clean monthly grain)
    source_file,     -- source file name (lineage)
    _loaded_at
from source
