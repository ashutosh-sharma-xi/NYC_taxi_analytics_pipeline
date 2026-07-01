-- 02 · Raw landing table. We land the columns the pipeline needs. MATCH_BY_COLUMN_NAME
-- on COPY ignores the extra Parquet columns (VendorID, RatecodeID, tolls, etc.).
-- _loaded_at is populated by its DEFAULT on every COPY — this is what dbt source
-- freshness checks against.

CREATE TABLE IF NOT EXISTS raw_yellow_tripdata (
  tpep_pickup_datetime   TIMESTAMP_NTZ,
  tpep_dropoff_datetime  TIMESTAMP_NTZ,
  passenger_count        NUMBER,
  trip_distance          FLOAT,
  PULocationID           NUMBER,
  DOLocationID           NUMBER,
  payment_type           NUMBER,
  fare_amount            FLOAT,
  tip_amount             FLOAT,
  total_amount           FLOAT,
  -- Provenance from the source file name (captured at COPY time):
  source_file            STRING,          -- e.g. yellow_tripdata_2023-01.parquet
  file_month             DATE,            -- first of the file's month, e.g. 2023-01-01
  _loaded_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
