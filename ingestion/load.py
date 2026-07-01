"""Ingestion: download Yellow Taxi Parquet -> land in NYC_TAXI.RAW.raw_yellow_tripdata.

    python ingestion/load.py            # loads MONTHS from env (default 2023-01)
    MONTHS=2023-01,2023-02 python ingestion/load.py

This is the ONLY job that writes raw data. All modeling happens in dbt, which
reads this table as a source.

Each month is processed independently, ONE AT A TIME: stream-download -> PUT ->
COPY -> free the local file. This keeps disk/stage footprint to a single ~50 MB
file regardless of how many months you load (the full year never sits on disk
or in memory at once), and a failure mid-run still leaves earlier months loaded.

Idempotent: Snowflake COPY tracks loaded files, so re-running a month is a no-op;
a new month appends. dbt's incremental fct_trips then processes only new rows —
the same code serves a one-off 2023 batch and a recurring monthly/daily load.

Set KEEP_LOCAL=1 to retain the downloaded Parquet under data/ (e.g. for the EDA
notebook); by default each file is deleted after it loads.
"""
import logging
import os
import time

import snowflake.connector

from config import get_config, connect_kwargs
from download import download

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ingest")

SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")


def run_sql_file(cur, filename):
    """Run every statement in a .sql file, in order.

    - Removes comment lines first, so a ';' inside a comment can't split a statement.
    - Splits on ';' and executes each statement on the open cursor.
    """
    with open(os.path.join(SQL_DIR, filename), encoding="utf-8") as f:
        raw = f.read()
    # Drop full-line comments FIRST, so a ';' inside a comment can't split a statement.
    sql = "\n".join(ln for ln in raw.splitlines() if not ln.strip().startswith("--"))
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        cur.execute(stmt)
    log.info("ran sql/%s", filename)


def load_month(cur, month, keep_local):
    """Load one month of trips into the raw table.

    - Downloads that month's file and uploads it to the Snowflake stage (PUT).
    - COPYs just that file into raw_yellow_tripdata (already-loaded files are skipped).
    - Deletes the local file afterwards unless KEEP_LOCAL is set.
    """
    fname = f"yellow_tripdata_{month}.parquet"
    t0 = time.perf_counter()
    log.info("[%s] starting", month)

    path = download((month,))[0]                       # stream just this month to disk

    uri = "file://" + os.path.abspath(path).replace("\\", "/")
    # OVERWRITE=FALSE: re-uploading would reset the staged file's metadata and make
    # COPY reload it (duplicates). Skipping keeps re-runs idempotent; COPY's own
    # load history then skips files it has already ingested.
    cur.execute(f"PUT '{uri}' @raw_stage AUTO_COMPRESS=FALSE OVERWRITE=FALSE")
    log.info("[%s] PUT to stage complete", month)

    # COPY only THIS file (FILES=...). Snowflake's load history skips it on re-run.
    # We use a transform SELECT (not MATCH_BY_COLUMN_NAME) so we can also capture
    # METADATA$FILENAME as provenance and derive file_month from the file name.
    # Parquet fields are read from the $1 VARIANT; keys are quoted to keep their case.
    cur.execute(f"""
        COPY INTO raw_yellow_tripdata
          (tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance,
           PULocationID, DOLocationID, payment_type, fare_amount, tip_amount, total_amount,
           source_file, file_month)
        FROM (
          SELECT
            $1:"tpep_pickup_datetime"::timestamp_ntz,
            $1:"tpep_dropoff_datetime"::timestamp_ntz,
            $1:"passenger_count"::number,
            $1:"trip_distance"::float,
            $1:"PULocationID"::number,
            $1:"DOLocationID"::number,
            $1:"payment_type"::number,
            $1:"fare_amount"::float,
            $1:"tip_amount"::float,
            $1:"total_amount"::float,
            METADATA$FILENAME,
            TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, '[0-9]{{4}}-[0-9]{{2}}') || '-01', 'YYYY-MM-DD')
          FROM @raw_stage
        )
          FILES = ('{fname}')
          FILE_FORMAT = (FORMAT_NAME = ff_parquet)
    """)
    loaded = cur.execute(
        "SELECT COUNT(*) FROM raw_yellow_tripdata WHERE source_file = %s", (fname,)
    ).fetchone()[0]
    log.info("[%s] COPY complete — %s rows from %s in raw table", month, f"{loaded:,}", fname)

    if not keep_local:
        os.remove(path)
        log.info("[%s] freed local file %s", month, fname)
    log.info("[%s] done in %.1fs", month, time.perf_counter() - t0)


def main():
    """Run the full ingestion.

    - Connects to Snowflake (key-pair or password).
    - Makes sure the warehouse, database, schema, stage and raw table exist.
    - Loads each month in MONTHS one at a time, then logs the total row count.
    """
    months = [m.strip() for m in os.environ.get("MONTHS", "2023-01").split(",")]
    keep_local = os.environ.get("KEEP_LOCAL", "") not in ("", "0", "false", "False")
    cfg = get_config()
    run_start = time.perf_counter()

    auth = "key-pair" if cfg.get("private_key_path") else "password"
    log.info("connecting to Snowflake (account=%s, user=%s, auth=%s)", cfg["account"], cfg["user"], auth)
    con = snowflake.connector.connect(**connect_kwargs(cfg))
    cur = con.cursor()
    try:
        log.info("ensuring %s.%s and warehouse %s", cfg["database"], cfg["raw_schema"], cfg["warehouse"])
        cur.execute(f"USE WAREHOUSE {cfg['warehouse']}")
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {cfg['database']}")
        cur.execute(f"USE DATABASE {cfg['database']}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {cfg['raw_schema']}")
        cur.execute(f"USE SCHEMA {cfg['raw_schema']}")

        log.info("applying setup + raw table DDL")
        run_sql_file(cur, "01_setup.sql")
        run_sql_file(cur, "02_raw_table.sql")

        log.info("loading %d month(s): %s", len(months), ", ".join(months))
        for month in months:
            load_month(cur, month, keep_local)

        total = cur.execute("SELECT COUNT(*) FROM raw_yellow_tripdata").fetchone()[0]
        log.info("INGESTION COMPLETE — raw_yellow_tripdata holds %s rows (%.1fs total)",
                 f"{total:,}", time.perf_counter() - run_start)
        log.info("next: cd dbt && dbt seed && dbt build")
    except Exception:
        log.exception("ingestion FAILED")
        raise
    finally:
        cur.close()
        con.close()


if __name__ == "__main__":
    main()
