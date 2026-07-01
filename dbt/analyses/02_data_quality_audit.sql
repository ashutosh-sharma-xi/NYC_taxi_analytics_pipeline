-- How many RAW rows fail each data-quality rule (run before/independently of dbt).
-- Rules overlap, so these won't sum to total dropped.
with t as (
    select * from raw.raw_yellow_tripdata
)
select
    (select count(*) from t)                                                          as raw_rows,
    (select count(*) from t where fare_amount <= 0)                                   as bad_fare,
    (select count(*) from t where trip_distance <= 0)                                 as bad_distance,
    (select count(*) from t where tpep_dropoff_datetime <= tpep_pickup_datetime)      as bad_time,
    (select count(*) from t where passenger_count is null
                               or passenger_count < 1 or passenger_count > 6)         as bad_passengers,
    (select count(*) from t where year(tpep_pickup_datetime) <> 2023)                 as year_leakage;
