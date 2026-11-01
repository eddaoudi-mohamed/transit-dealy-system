import io
import zipfile

import pytest
import requests

from transit_pipeline.ingestion import download_files as dl
from transit_pipeline.ingestion.manifest import already_ingested, append_record, make_record


def zip_bytes():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("a.txt", "hello")
    return buf.getvalue()


class FakeResponse:
    def __init__(self, content=b"", status=200):
        self.content, self.status = content, status

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def raise_for_status(self):
        if self.status >= 400:
            raise requests.HTTPError(f"HTTP {self.status}")

    def iter_content(self, chunk_size=1):
        if self.content:
            yield self.content


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(dl.time, "sleep", lambda s: None)


def test_download_success(tmp_path, monkeypatch):
    monkeypatch.setattr(dl.requests, "get", lambda *a, **k: FakeResponse(zip_bytes()))
    dest = tmp_path / "ok.zip"
    dl.download_one("http://x/ok.zip", dest, timeout=5, max_retries=2, backoff=1)
    assert dest.exists()
    assert not (tmp_path / "ok.zip.part").exists()


def test_retry_then_success(tmp_path, monkeypatch):
    calls = {"n": 0}

    def fake_get(*a, **k):
        calls["n"] += 1
        return FakeResponse(status=503) if calls["n"] < 3 else FakeResponse(zip_bytes())

    monkeypatch.setattr(dl.requests, "get", fake_get)
    dest = tmp_path / "ok.zip"
    dl.download_one("http://x/ok.zip", dest, timeout=5, max_retries=3, backoff=1)
    assert dest.exists()
    assert calls["n"] == 3


def test_fails_after_retries(tmp_path, monkeypatch):
    monkeypatch.setattr(dl.requests, "get", lambda *a, **k: FakeResponse(status=503))
    dest = tmp_path / "bad.zip"
    with pytest.raises(dl.DownloadError):
        dl.download_one("http://x/bad.zip", dest, timeout=5, max_retries=2, backoff=1)
    assert not dest.exists()
    assert not (tmp_path / "bad.zip.part").exists()


def test_empty_file_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(dl.requests, "get", lambda *a, **k: FakeResponse(b""))
    dest = tmp_path / "empty.zip"
    with pytest.raises(dl.DownloadError):
        dl.download_one("http://x/empty.zip", dest, timeout=5, max_retries=1, backoff=1)
    assert not dest.exists()


def test_invalid_zip_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(dl.requests, "get", lambda *a, **k: FakeResponse(b"<html>error</html>"))
    dest = tmp_path / "fake.zip"
    with pytest.raises(dl.DownloadError):
        dl.download_one("http://x/fake.zip", dest, timeout=5, max_retries=1, backoff=1)
    assert not dest.exists()


def test_already_ingested(tmp_path):
    f = tmp_path / "a.zip"
    f.write_bytes(zip_bytes())
    manifest = tmp_path / "manifest.jsonl"
    append_record(manifest, make_record("http://x/a.zip", f, "downloaded"))
    assert already_ingested(manifest, "http://x/a.zip")
    f.unlink()                                          # file gone -> must download again
    assert not already_ingested(manifest, "http://x/a.zip")