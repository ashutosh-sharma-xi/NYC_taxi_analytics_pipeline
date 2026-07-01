"""(Bonus) PySpark batch processor for the full historical dataset.

Demonstrates the same ingestion + DQ logic at scale, independent of Snowflake.
Reads the 2023 Parquet files, applies the data-quality filters, derives
trip_duration_minutes, and writes a cleaned dataset partitioned by year/month —
the shape you'd land in a data lake (S3/GCS) before loading a warehouse.

Run locally:
    pip install pyspark
    python spark/process_historical.py --input data --output spark_output

This is illustrative: for ~4 GB DuckDB/Snowflake are simpler. Spark earns its
keep once the history spans many years and exceeds single-node memory.
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark():
    """Create a local SparkSession (UTC timezone) for the batch job."""
    return (
        SparkSession.builder
        .appName("nyc_taxi_historical")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def process(spark, input_dir, output_dir, min_minutes=1, max_minutes=180):
    """Clean the historical Parquet and write it back out, partitioned.

    - Reads all 2023 files, derives trip_duration_minutes.
    - Applies the same data-quality filters as the dbt pipeline.
    - Writes cleaned Parquet partitioned by pickup year/month.
    """
    df = spark.read.parquet(f"{input_dir}/yellow_tripdata_2023-*.parquet")

    cleaned = (
        df.withColumn(
            "trip_duration_minutes",
            (F.col("tpep_dropoff_datetime").cast("long")
             - F.col("tpep_pickup_datetime").cast("long")) / 60,
        )
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("passenger_count") > 0)
        .filter(F.col("trip_duration_minutes") >= min_minutes)
        .filter(F.col("trip_duration_minutes") <= max_minutes)
        .filter(F.year("tpep_pickup_datetime") == 2023)
        .withColumn("pickup_year", F.year("tpep_pickup_datetime"))
        .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
    )

    print(f"Clean rows: {cleaned.count():,}")
    (
        cleaned.write
        .mode("overwrite")
        .partitionBy("pickup_year", "pickup_month")
        .parquet(output_dir)
    )
    print(f"Wrote partitioned Parquet to {output_dir}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="data", help="dir with yellow_tripdata_2023-*.parquet")
    p.add_argument("--output", default="spark_output", help="output dir for cleaned Parquet")
    p.add_argument("--min-minutes", type=int, default=1)
    p.add_argument("--max-minutes", type=int, default=180)
    args = p.parse_args()

    spark = build_spark()
    try:
        process(spark, args.input, args.output, args.min_minutes, args.max_minutes)
    finally:
        spark.stop()
