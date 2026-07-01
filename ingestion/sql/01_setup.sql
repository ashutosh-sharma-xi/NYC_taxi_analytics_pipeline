-- 01 · Setup — file format + internal stage for raw ingestion.
-- Idempotent. Runs inside the RAW schema set by the loader.

-- USE_LOGICAL_TYPE = TRUE is REQUIRED: NYC Parquet stores tpep_*_datetime as INT64
-- logical timestamps. Without this flag Snowflake loads the raw epoch integer
-- instead of a real timestamp (YEAR() then matches nothing and all trips get
-- filtered out downstream). CREATE OR REPLACE so the flag applies on re-runs.
CREATE OR REPLACE FILE FORMAT ff_parquet
  TYPE = PARQUET
  USE_LOGICAL_TYPE = TRUE;

CREATE STAGE IF NOT EXISTS raw_stage;
