# NYC Taxi Analytics Pipeline

Production-grade, dbt-first analytics pipeline over the NYC TLC **Yellow Taxi**
trip records (full-year 2023, ~38M rows). Batch ingestion into Snowflake,
transformation with dbt (staging → intermediate → marts), tested, and
orchestrated with Airflow. Designed so the one-off 2023 batch and a recurring
monthly/daily load run the same code.

## Architecture

```
CloudFront Parquet ──> ingestion/ ──> Snowflake RAW ──> dbt (staging→int→marts) ──> queries / BI
   (public source)     (batch load)   raw_yellow_tripdata    transform + test
                                              ▲
                          Airflow DAG orchestrates: freshness → dbt layers → tests → notify
```

- **`ingestion/`** — standalone batch loader. Pulls Parquet from the public
  CloudFront mirror and lands `NYC_TAXI.RAW.raw_yellow_tripdata` with a
  `_loaded_at` column. No transformation. Parameterised by `MONTHS`.
- **`dbt/`** — all modeling + tests. Zone lookup is a **seed**. Sources have a
  **freshness** check on `_loaded_at`.
- **`dags/`** — `nyc_taxi_daily_pipeline` Airflow DAG (daily 02:00 UTC).
- **`analysis/`** — ad-hoc analytical SQL (Snowflake) against the marts / RAW.
- **`queries/`** — assessment query set.
- **`spark/`** — (bonus) PySpark historical processor.
- **`docs/`** — reference write-ups (HTML): `productionizing.html`, plus
  `why-duckdb.html` (a DuckDB explainer for context — not used here).

> **Engine: Snowflake only.** The pipeline runs entirely on Snowflake. See
> [`docs/productionizing.html`](docs/productionizing.html).

## dbt layers

| Layer | Models | Notes |
|---|---|---|
| staging | `stg_yellow_trips`, `stg_taxi_zones` | snake_case rename, casts, `trip_duration_minutes`, surrogate `trip_id` |
| intermediate | `int_trips_enriched` | join pickup+dropoff zone names; DQ filters (distance/fare/passengers/duration) |
| marts | `fct_trips`, `dim_zones`, `agg_daily_revenue`, `agg_zone_performance` | `fct_trips` is **incremental**; zone perf uses a window rank + high-volume flag |

### Data quality
- `not_null` / `unique` on primary keys (`trip_id`, `location_id`, …)
- `relationships` from `fct_trips.pickup_location_id` → `dim_zones.location_id`
- **Singular test** `assert_total_amount_gte_fare` — no trip has `total_amount < fare_amount`
- **Generic test** `value_within_range` — parametrised min/max on `trip_duration_minutes`
  (bounds set once in `dbt_project.yml` vars, reused by the intermediate filter)

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env           # fill in SNOWFLAKE_ACCOUNT / USER / PASSWORD
cp dbt/profiles.yml.example ~/.dbt/profiles.yml   # or set DBT_PROFILES_DIR=dbt/
```

Credentials live only in env vars / `.env` (gitignored). To point at a different
Snowflake account, change the env — never the code.

## Run it

```bash
# 1. Ingest (Jan 2023 by default; set MONTHS=2023-01,2023-02,... for more)
python ingestion/load.py

# 2. Transform + test with dbt
cd dbt
dbt seed          # load the zone lookup
dbt run           # build staging -> intermediate -> marts
dbt test          # data-quality gate
dbt source freshness

# 3. Explore
#   run queries/*.sql in Snowsight against the *_marts schema
```

Full-year load: `MONTHS=2023-01,2023-02,...,2023-12 python ingestion/load.py`
then `dbt run --full-refresh`.

## Orchestration

`dags/nyc_taxi_daily_pipeline.py` runs daily at 02:00 UTC:
`check_source_freshness → run_dbt_staging → run_dbt_intermediate → run_dbt_marts
→ run_dbt_tests → notify_success`. `retries=2`, `retry_delay=5m`,
`email_on_failure=True`, `catchup=True` for backfill. No hard-coded credentials.

## Bonus: Spark

```bash
python ingestion/download.py            # fetch Parquet locally first
python spark/process_historical.py --input data --output spark_output
```

## Notes
- TLC publishes **monthly** with a ~2-month lag — this is a batch dataset.
  The daily DAG is the orchestration pattern; the freshness check maps each run
  date to its month's file.
- `data/`, `.env`, and `dbt/target/` are gitignored (no credentials or large
  data files in the repo).
