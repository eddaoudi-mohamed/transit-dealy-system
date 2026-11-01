import argparse
import logging
import time
import zipfile
from pathlib import Path

import pyarrow.parquet as pq
import requests

from transit_pipeline.config import load_config
from transit_pipeline.ingestion.manifest import already_ingested, append_record, make_record

log = logging.getLogger("ingestion")


class DownloadError(Exception):
    pass


def validate(path, dest_name):
    """Reject files that are not what their extension says (e.g. an HTML error page)."""
    suffix = Path(dest_name).suffix.lower()
    if suffix == ".zip":
        if not zipfile.is_zipfile(path):
            raise DownloadError("not a valid zip file")
    elif suffix == ".parquet":
        try:
            pq.ParquetFile(path)
        except Exception as e:
            raise DownloadError(f"not a valid parquet file: {e}")


def download_one(url, dest, timeout, max_retries, backoff):
    """Download to <dest>.part, validate, then rename to <dest>. Retries with growing waits."""
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    last_error = None

    for attempt in range(1, max_retries + 2):          # first try + max_retries retries
        try:
            with requests.get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
            if tmp.stat().st_size == 0:
                raise DownloadError("empty file")
            validate(tmp, dest.name)
            tmp.replace(dest)                           # atomic rename
            return dest
        except (requests.RequestException, DownloadError, OSError) as e:
            last_error = e
            tmp.unlink(missing_ok=True)
            if attempt <= max_retries:
                wait = backoff * 2 ** (attempt - 1)     # 2s, 4s, 8s ...
                log.warning("attempt %d failed for %s: %s (retry in %ss)", attempt, url, e, wait)
                time.sleep(wait)

    raise DownloadError(f"could not download {url} after {max_retries} retries: {last_error}")


def setup_logging(logs_dir):
    handlers = [logging.StreamHandler()]
    try:
        Path(logs_dir).mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(Path(logs_dir) / "ingestion.log"))
    except OSError:
        pass
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s",
                        handlers=handlers)


def main():
    ap = argparse.ArgumentParser(description="Download raw files into data/raw and record them.")
    ap.add_argument("--limit", type=int, help="download at most N files")
    ap.add_argument("--dry-run", action="store_true", help="list what would be downloaded")
    ap.add_argument("--force", action="store_true", help="download again even if already in manifest")
    args = ap.parse_args()

    cfg = load_config()
    setup_logging(cfg["paths"]["logs_dir"])
    raw_dir = Path(cfg["paths"]["raw_dir"])
    manifest = cfg["paths"]["manifest"]
    dl = cfg["download"]

    todo = []
    for entry in cfg["downloads"]:
        url = entry["url"]
        if "PASTE" in url:
            log.warning("skipping %s: url not filled in", entry["name"])
            continue
        if not args.force and already_ingested(manifest, url):
            log.info("skip (already downloaded): %s", entry["name"])
            continue
        todo.append(entry)

    if args.limit is not None:
        todo = todo[: args.limit]

    ok = failed = 0
    for entry in todo:
        url, dest = entry["url"], raw_dir / entry["dest"]
        if args.dry_run:
            log.info("DRY RUN would download %s -> %s", url, dest)
            continue
        try:
            download_one(url, dest, dl["timeout_seconds"], dl["max_retries"], dl["backoff_seconds"])
            append_record(manifest, make_record(url, dest, "downloaded"))
            log.info("downloaded %s (%.1f MB)", entry["name"], dest.stat().st_size / 1e6)
            ok += 1
        except DownloadError as e:
            append_record(manifest, make_record(url, dest, "failed", error=str(e)))
            log.error("FAILED %s: %s", entry["name"], e)
            failed += 1

    log.info("done: %d downloaded, %d failed, %d planned", ok, failed, len(todo))


if __name__ == "__main__":
    main()