-- Fact table: all valid, enriched trips. Grain = one trip.
--
-- Incremental so monthly/daily reruns only process new data: on an incremental
-- run we keep only rows newer than the latest pickup already loaded. The qualify
-- dedupes any repeated surrogate keys within a batch, guaranteeing trip_id unique
-- (so the unique test holds). For a full 2023 batch, run `dbt run --full-refresh`.

{{
  config(
    materialized = 'incremental',
    unique_key = 'trip_id',
    on_schema_change = 'sync_all_columns'
  )
}}

with enriched as (
    select * from {{ ref('int_trips_enriched') }}

    {% if is_incremental() %}
    where pickup_datetime > (select coalesce(max(pickup_datetime), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}
)

select *
from enriched
qualify row_number() over (partition by trip_id order by _loaded_at desc) = 1
