-- Zone dimension for the marts layer. One row per taxi zone.
select
    location_id,
    borough,
    zone_name,
    service_zone
from {{ ref('stg_taxi_zones') }}
