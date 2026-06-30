"""Download Yellow Taxi Parquet from the public CloudFront mirror.

The s3://nyc-tlc bucket denies anonymous access, so we pull from CloudFront
(public HTTPS). The zone lookup is NOT downloaded here — it lives as a dbt seed
(dbt/seeds/taxi_zone_lookup.csv), since it's a small, static dimension.
"""
import os
import urllib.request

BASE = "https://d37ci6vzurychx.cloudfront.net"
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def _fetch(url, dest):
    if os.path.exists(dest):
        print(f"   exists: {os.path.basename(dest)}")
        return
    print(f"   downloading {os.path.basename(dest)} ...")
    urllib.request.urlretrieve(url, dest)
    print(f"   saved   {os.path.basename(dest)} ({os.path.getsize(dest) // 1024:,} KB)")


def download(months=("2023-01",)):
    """Download the given months of Yellow Taxi data. Returns local file paths."""
    os.makedirs(DATA_DIR, exist_ok=True)
    files = []
    for m in months:
        m = m.strip()
        dest = os.path.join(DATA_DIR, f"yellow_tripdata_{m}.parquet")
        _fetch(f"{BASE}/trip-data/yellow_tripdata_{m}.parquet", dest)
        files.append(dest)
    return files


if __name__ == "__main__":
    download(tuple(os.environ.get("MONTHS", "2023-01").split(",")))
