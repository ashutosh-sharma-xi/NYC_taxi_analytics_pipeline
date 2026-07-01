"""Stream Yellow Taxi Parquet from the public CloudFront mirror to local disk.

The s3://nyc-tlc bucket blocks anonymous access (and may be requester-pays), so we
pull from CloudFront over HTTPS — same files, free, no credentials. Files are
streamed in chunks (never fully held in memory) with a small retry loop, then the
loader PUTs them to a Snowflake stage and COPYs into the raw table.

The zone lookup is NOT downloaded here — it lives as a dbt seed.
"""
import logging
import os
import time

import requests

log = logging.getLogger("ingest.download")

BASE = "https://d37ci6vzurychx.cloudfront.net"
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
CHUNK = 1 << 20  # 1 MiB


def _stream_to_disk(url, dest, retries=3, timeout=60):
    """Download one file to disk, a chunk at a time.

    - Skips it if the file is already downloaded.
    - Writes to a temp '.part' file then renames, so a half-download never looks done.
    - Retries a few times on network errors, waiting longer each attempt.
    """
    name = os.path.basename(dest)
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        log.info("cache hit, skipping download: %s", name)
        return
    tmp = dest + ".part"
    for attempt in range(1, retries + 1):
        try:
            t0 = time.perf_counter()
            with requests.get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=CHUNK):
                        if chunk:
                            f.write(chunk)
            os.replace(tmp, dest)  # atomic: only a complete download becomes final
            secs = max(time.perf_counter() - t0, 1e-6)
            mb = os.path.getsize(dest) / 1e6
            log.info("downloaded %s — %.1f MB in %.1fs (%.1f MB/s)", name, mb, secs, mb / secs)
            return
        except (requests.RequestException, OSError) as exc:
            if os.path.exists(tmp):
                os.remove(tmp)
            if attempt == retries:
                log.error("download FAILED for %s after %d attempts: %s", name, retries, exc)
                raise
            wait = 2 ** attempt
            log.warning("download error for %s (%s) — retry %d/%d in %ds", name, exc, attempt, retries, wait)
            time.sleep(wait)


def download(months=("2023-01",)):
    """Download the given months of Yellow Taxi Parquet.

    - One file per month, saved under data/.
    - Returns the list of local file paths.
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    files = []
    for m in months:
        m = m.strip()
        dest = os.path.join(DATA_DIR, f"yellow_tripdata_{m}.parquet")
        log.info("fetching month %s", m)
        _stream_to_disk(f"{BASE}/trip-data/yellow_tripdata_{m}.parquet", dest)
        files.append(dest)
    return files


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    download(tuple(os.environ.get("MONTHS", "2023-01").split(",")))
