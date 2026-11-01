import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


def sha256_file(path, chunk_size=1024 * 1024):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk_size), b""):
            h.update(block)
    return h.hexdigest()


def read_manifest(manifest_path):
    p = Path(manifest_path)
    if not p.exists():
        return []
    with open(p) as f:
        return [json.loads(line) for line in f if line.strip()]


def already_ingested(manifest_path, source_key):
    """True only if the manifest says 'downloaded' AND the file is still on disk."""
    for r in read_manifest(manifest_path):
        if (r["source_key"] == source_key
                and r["status"] == "downloaded"
                and Path(r["local_path"]).exists()):
            return True
    return False


def append_record(manifest_path, record):
    p = Path(manifest_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "a") as f:
        f.write(json.dumps(record) + "\n")


def make_record(source_key, local_path, status, error=None):
    path = Path(local_path)
    ok = status == "downloaded" and path.exists()
    now = datetime.now(timezone.utc).isoformat()
    return {
        "source_key": source_key,
        "local_path": str(path),
        "size_bytes": path.stat().st_size if ok else None,
        "checksum": sha256_file(path) if ok else None,
        "discovered_at": now,
        "downloaded_at": now if ok else None,
        "status": status,
        "error": error,
    }