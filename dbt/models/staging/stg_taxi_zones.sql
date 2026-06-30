-- Staging: the zone lookup, loaded from the dbt seed (seeds/taxi_zone_lookup.csv).
-- 265 rows, static dimension — a seed is the idiomatic dbt home for it.

with source as (
    select * from {{ ref('taxi_zone_lookup') }}
)

select
    cast(locationid as integer) as location_id,
    borough,
    zone                        as zone_name,
    service_zone
from source
