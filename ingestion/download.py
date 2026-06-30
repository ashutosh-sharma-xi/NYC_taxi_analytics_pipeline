"""Stream Yellow Taxi Parquet from the public CloudFront mirror to local disk.

The s3://nyc-tlc bucket blocks anonymous access (and may be requester-pays), so we
pull from CloudFront over HTTPS — same files, free, no credentials. Files are
streamed in chunks (never fully held in memory) with a small retry loop, then the
loader PUTs them to a Snowflake stage and COPYs into the raw table.

The zone lookup is NOT downloaded here — it lives as a dbt seed.
"""
import os
import time

import requests

BASE = "https://d37ci6vzurychx.cloudfront.net"
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
CHUNK = 1 << 20  # 1 MiB


def _stream_to_disk(url, dest, retries=3, timeout=60):
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        print(f"   exists: {os.path.basename(dest)}")
        return
    tmp = dest + ".part"
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                total = int(r.headers.get("Content-Length", 0))
                done = 0
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=CHUNK):
                        if chunk:
                            f.write(chunk)
                            done += len(chunk)
            os.replace(tmp, dest)  # atomic: only a complete download becomes final
            mb = os.path.getsize(dest) / 1e6
            print(f"   saved   {os.path.basename(dest)} ({mb:,.1f} MB)")
            return
        except (requests.RequestException, OSError) as exc:
            if os.path.exists(tmp):
                os.remove(tmp)
            if attempt == retries:
                raise
            wait = 2 ** attempt
            print(f"   retry {attempt}/{retries} after error ({exc}); waiting {wait}s")
            time.sleep(wait)


def download(months=("2023-01",)):
    """Stream the given months of Yellow Taxi data. Returns local file paths."""
    os.makedirs(DATA_DIR, exist_ok=True)
    files = []
    for m in months:
        m = m.strip()
        dest = os.path.join(DATA_DIR, f"yellow_tripdata_{m}.parquet")
        print(f"   downloading yellow_tripdata_{m}.parquet ...")
        _stream_to_disk(f"{BASE}/trip-data/yellow_tripdata_{m}.parquet", dest)
        files.append(dest)
    return files


if __name__ == "__main__":
    download(tuple(os.environ.get("MONTHS", "2023-01").split(",")))
