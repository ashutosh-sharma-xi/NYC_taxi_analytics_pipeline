-- 01 · Setup — file format + internal stage for raw ingestion.
-- Idempotent. Runs inside the RAW schema set by the loader.

CREATE FILE FORMAT IF NOT EXISTS ff_parquet
  TYPE = PARQUET;

CREATE STAGE IF NOT EXISTS raw_stage;
