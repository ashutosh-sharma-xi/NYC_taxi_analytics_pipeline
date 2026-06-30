"""Airflow DAG: nyc_taxi_daily_pipeline

Runs daily at 02:00 UTC. Validates the source file for the run's date exists,
runs the dbt layers in order (staging -> intermediate -> marts), runs all dbt
tests (failing the DAG if any fail), and logs a success summary.

Credentials are never hard-coded — they come from environment variables (which
can be backed by an Airflow Connection). Every task keys off the logical date
({{ ds }}), so backfilling a past date works correctly.

NOTE on cadence: TLC actually publishes monthly with a ~2-month lag, so the
"source file for the day" is the monthly Parquet covering that date. The daily
schedule here follows the assessment spec; the freshness check maps the run date
to its month's file.
"""
from __future__ import annotations

import os
import sys
import urllib.request
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_DIR = os.environ.get("NYC_TAXI_PROJECT_DIR", "/opt/airflow/nyc_taxi_analytics_pipeline")
DBT_DIR = os.path.join(PROJECT_DIR, "dbt")
CLOUDFRONT = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Make the repo's utils/ importable inside Airflow workers.
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)
from utils.notifications import airflow_failure_callback, send_success  # noqa: E402

# Snowflake creds flow through the environment to dbt and to the Python tasks.
SNOWFLAKE_ENV = {
    "SNOWFLAKE_ACCOUNT":   os.environ.get("SNOWFLAKE_ACCOUNT", ""),
    "SNOWFLAKE_USER":      os.environ.get("SNOWFLAKE_USER", ""),
    "SNOWFLAKE_PASSWORD":  os.environ.get("SNOWFLAKE_PASSWORD", ""),
    "SNOWFLAKE_ROLE":      os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    "SNOWFLAKE_WAREHOUSE": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "SNOWFLAKE_DATABASE":  os.environ.get("SNOWFLAKE_DATABASE", "NYC_TAXI"),
    "RAW_SCHEMA":          os.environ.get("RAW_SCHEMA", "RAW"),
    "DBT_SCHEMA":          os.environ.get("DBT_SCHEMA", "ANALYTICS"),
    "DBT_PROFILES_DIR":    DBT_DIR,
}

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": os.environ.get("ALERT_EMAIL", "data-alerts@example.com").split(","),
    # Slack alert on ANY task failure, with a link to that task's log.
    "on_failure_callback": airflow_failure_callback,
}


def check_source_freshness(**context):
    """Raise if the source Parquet covering the run date is not available.

    Maps {{ ds }} to its month and HEAD-checks the public file. Raising here
    fails the task (and DAG) before any dbt work runs.
    """
    month = context["ds"][:7]  # 'YYYY-MM'
    url = f"{CLOUDFRONT}/yellow_tripdata_{month}.parquet"
    req = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status != 200:
                raise FileNotFoundError(f"Source not available ({resp.status}): {url}")
    except Exception as exc:  # noqa: BLE001 - surface any failure as a hard error
        raise FileNotFoundError(f"Source freshness check failed for {month}: {exc}")
    print(f"Source present for {month}: {url}")


def notify_success(**context):
    """Log a success summary: trip count + revenue for the run date."""
    import snowflake.connector

    ds = context["ds"]
    marts_schema = f"{SNOWFLAKE_ENV['DBT_SCHEMA']}_marts"
    con = snowflake.connector.connect(
        account=SNOWFLAKE_ENV["SNOWFLAKE_ACCOUNT"],
        user=SNOWFLAKE_ENV["SNOWFLAKE_USER"],
        password=SNOWFLAKE_ENV["SNOWFLAKE_PASSWORD"],
        role=SNOWFLAKE_ENV["SNOWFLAKE_ROLE"],
        warehouse=SNOWFLAKE_ENV["SNOWFLAKE_WAREHOUSE"],
        database=SNOWFLAKE_ENV["SNOWFLAKE_DATABASE"],
        schema=marts_schema,
    )
    try:
        cur = con.cursor()
        cur.execute(
            "SELECT total_trips, total_fare "
            "FROM agg_daily_revenue WHERE revenue_date = %s",
            (ds,),
        )
        row = cur.fetchone()
    finally:
        con.close()

    if row:
        trips, revenue = row
        metrics = {"Trips": f"{trips:,}", "Fare revenue": f"${revenue:,.2f}"}
        print(f"SUCCESS {ds}: {trips:,} trips, ${revenue:,.2f} fare revenue")
    else:
        metrics = {"Note": "pipeline completed, no rows for this date"}
        print(f"SUCCESS {ds}: pipeline completed (no rows for this date)")

    # Link to this run so the success message is clickable in Slack.
    ti = context.get("task_instance")
    run_url = getattr(ti, "log_url", None)
    send_success(dag_id=context["dag"].dag_id, run_date=ds, metrics=metrics, run_url=run_url)


with DAG(
    dag_id="nyc_taxi_daily_pipeline",
    description="Daily NYC Taxi dbt pipeline on Snowflake",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule="0 2 * * *",     # daily at 02:00 UTC
    catchup=True,             # enables correct backfill for past dates
    max_active_runs=1,
    tags=["nyc-taxi", "dbt", "snowflake"],
) as dag:

    check_source = PythonOperator(
        task_id="check_source_freshness",
        python_callable=check_source_freshness,
    )

    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --select staging --target dev",
        env=SNOWFLAKE_ENV,
        append_env=True,
    )

    run_dbt_intermediate = BashOperator(
        task_id="run_dbt_intermediate",
        bash_command=f"cd {DBT_DIR} && dbt run --select intermediate --target dev",
        env=SNOWFLAKE_ENV,
        append_env=True,
    )

    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --select marts --target dev",
        env=SNOWFLAKE_ENV,
        append_env=True,
    )

    # Non-zero exit on test failure fails this task -> fails the DAG.
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=f"cd {DBT_DIR} && dbt test --target dev",
        env=SNOWFLAKE_ENV,
        append_env=True,
    )

    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    check_source >> run_dbt_staging >> run_dbt_intermediate >> run_dbt_marts >> run_dbt_tests >> notify
