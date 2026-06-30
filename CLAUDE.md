# NYC Taxi Analytics Pipeline — guide for contributors/agents

dbt-first analytics pipeline over NYC TLC Yellow Taxi data (2023). Batch
ingestion into Snowflake → dbt transforms → tests → Airflow orchestration.

## Architecture & where things live
- **`ingestion/`** — standalone batch loader (the ONLY writer of raw data).
  Downloads Parquet from the public CloudFront mirror → lands
  `NYC_TAXI.RAW.raw_yellow_tripdata` with a `_loaded_at` column. No transforms.
  Parameterised by `MONTHS` env var (one-off 2023 batch or recurring load).
- **`dbt/`** — all modeling + data-quality tests.
  - `models/staging` → `models/intermediate` → `models/marts` (view → view → table).
  - Zone lookup is a **seed** (`seeds/taxi_zone_lookup.csv`), not ingested.
  - `models/staging/sources.yml` defines source **freshness** on `_loaded_at`.
  - `fct_trips` is **incremental** (keyed on surrogate `trip_id`).
  - Duration bounds are `vars` in `dbt_project.yml`, reused by the intermediate
    filter AND the `value_within_range` generic test (single source of truth).
- **`dags/nyc_taxi_daily_pipeline.py`** — Airflow DAG, daily 02:00 UTC:
  `check_source_freshness → run_dbt_{staging,intermediate,marts} → run_dbt_tests
  → notify_success`. `retries=2`, `email_on_failure`, `catchup=True`.
- **`queries/`** — ad-hoc analytical SQL against the `*_marts` schema.
- **`spark/`** — (bonus) PySpark historical processor.
- **`notebooks/`** — local DuckDB exploration (free, no Snowflake needed).
- **`docs/`** — HTML decision write-ups (see Documentation conventions).

## Engine split (don't conflate)
- **DuckDB** = local/dev exploration in `notebooks/` only.
- **Snowflake** = the production warehouse dbt builds.
- Rationale: [`docs/why-duckdb.html`](docs/why-duckdb.html) (local) +
  [`docs/productionizing.html`](docs/productionizing.html) (prod).

## Conventions
- **No hard-coded credentials.** Everything reads env vars (`.env` locally,
  secrets in CI). Portable to any Snowflake account by changing env, not code.
- `.env`, `data/`, `dbt/target/` are gitignored.
- dbt: snake_case columns, staging stays 1:1 with source, cleaning happens in
  intermediate, marts are the consumable layer.
- HTML docs in `docs/`: self-contained, inline CSS, dark theme, brief & visual.
- Lead with the decision, then the rationale (why this, not the alternative).
