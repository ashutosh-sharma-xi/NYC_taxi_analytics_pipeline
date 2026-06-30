"""Ingestion: download Yellow Taxi Parquet -> land in NYC_TAXI.RAW.raw_yellow_tripdata.

    python ingestion/load.py            # loads MONTHS from env (default 2023-01)
    MONTHS=2023-01,2023-02 python ingestion/load.py

This is the ONLY job that writes raw data. All modeling happens in dbt, which
reads this table as a source. Idempotent: Snowflake COPY tracks loaded files, so
re-running a month is a no-op; a new month appends. dbt's incremental fct_trips
then processes only the new rows — the same code serves a one-off 2023 batch and
a recurring monthly/daily load.
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


def main():
    months = tuple(os.environ.get("MONTHS", "2023-01").split(","))
    cfg = get_config()

    print(f"1) Downloading {len(months)} month(s) from CloudFront...")
    files = download(months)

    print("2) Connecting to Snowflake...")
    con = snowflake.connector.connect(
        account=cfg["account"], user=cfg["user"], password=cfg["password"],
        role=cfg["role"], warehouse=cfg["warehouse"],
    )
    cur = con.cursor()
    try:
        print(f"3) Ensuring {cfg['database']}.{cfg['raw_schema']} ...")
        cur.execute(f"USE WAREHOUSE {cfg['warehouse']}")
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {cfg['database']}")
        cur.execute(f"USE DATABASE {cfg['database']}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {cfg['raw_schema']}")
        cur.execute(f"USE SCHEMA {cfg['raw_schema']}")

        print("4) Setup + raw table...")
        run_sql_file(cur, "01_setup.sql")
        run_sql_file(cur, "02_raw_table.sql")

        print("5) PUT files to internal stage...")
        for f in files:
            uri = "file://" + os.path.abspath(f).replace("\\", "/")
            cur.execute(f"PUT '{uri}' @raw_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
            print(f"   put {os.path.basename(f)}")

        print("6) COPY INTO raw_yellow_tripdata (skips already-loaded files)...")
        cur.execute("""
            COPY INTO raw_yellow_tripdata FROM @raw_stage
              FILE_FORMAT = (FORMAT_NAME = ff_parquet)
              PATTERN = '.*yellow_tripdata_.*[.]parquet'
              MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """)
        total = cur.execute("SELECT COUNT(*) FROM raw_yellow_tripdata").fetchone()[0]
        print(f"\nDone. raw_yellow_tripdata now holds {total:,} rows.")
        print("Next: cd dbt && dbt seed && dbt run && dbt test")
    finally:
        cur.close()
        con.close()


if __name__ == "__main__":
    main()
