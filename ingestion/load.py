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
import os

import snowflake.connector

from config import get_config
from download import download

SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")


def run_sql_file(cur, filename):
    with open(os.path.join(SQL_DIR, filename), encoding="utf-8") as f:
        for stmt in [s.strip() for s in f.read().split(";") if s.strip()]:
            cur.execute(stmt)


def load_month(cur, month, keep_local):
    """Download one month, PUT it, COPY it, then optionally remove the local file."""
    fname = f"yellow_tripdata_{month}.parquet"
    print(f"\n--- {month} ---")
    path = download((month,))[0]                      # stream just this month to disk

    uri = "file://" + os.path.abspath(path).replace("\\", "/")
    cur.execute(f"PUT '{uri}' @raw_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
    print(f"   PUT {fname}")

    # COPY only THIS file (FILES=...). Snowflake's load history skips it on re-run.
    cur.execute(f"""
        COPY INTO raw_yellow_tripdata FROM @raw_stage
          FILES = ('{fname}')
          FILE_FORMAT = (FORMAT_NAME = ff_parquet)
          MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    """)
    loaded = cur.execute(
        "SELECT COUNT(*) FROM raw_yellow_tripdata "
        "WHERE YEAR(tpep_pickup_datetime)=2023 "
        f"AND MONTH(tpep_pickup_datetime)={int(month.split('-')[1])}"
    ).fetchone()[0]
    print(f"   COPY done — {loaded:,} {month} rows now in raw table")

    if not keep_local:
        os.remove(path)
        print(f"   freed local {fname}")


def main():
    months = [m.strip() for m in os.environ.get("MONTHS", "2023-01").split(",")]
    keep_local = os.environ.get("KEEP_LOCAL", "") not in ("", "0", "false", "False")
    cfg = get_config()

    print("1) Connecting to Snowflake...")
    con = snowflake.connector.connect(
        account=cfg["account"], user=cfg["user"], password=cfg["password"],
        role=cfg["role"], warehouse=cfg["warehouse"],
    )
    cur = con.cursor()
    try:
        print(f"2) Ensuring {cfg['database']}.{cfg['raw_schema']} ...")
        cur.execute(f"USE WAREHOUSE {cfg['warehouse']}")
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {cfg['database']}")
        cur.execute(f"USE DATABASE {cfg['database']}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {cfg['raw_schema']}")
        cur.execute(f"USE SCHEMA {cfg['raw_schema']}")

        print("3) Setup + raw table...")
        run_sql_file(cur, "01_setup.sql")
        run_sql_file(cur, "02_raw_table.sql")

        print(f"4) Loading {len(months)} month(s), one at a time...")
        for month in months:
            load_month(cur, month, keep_local)

        total = cur.execute("SELECT COUNT(*) FROM raw_yellow_tripdata").fetchone()[0]
        print(f"\nDone. raw_yellow_tripdata now holds {total:,} rows.")
        print("Next: cd dbt && dbt seed && dbt run && dbt test")
    finally:
        cur.close()
        con.close()


if __name__ == "__main__":
    main()
