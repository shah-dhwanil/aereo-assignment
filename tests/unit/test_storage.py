"""Unit tests for the storage protocol + atomic disk implementation."""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

from aereo_assignment.services.storage import DiskStorage


async def test_write_then_read_roundtrip(tmp_path: Path):
    storage = DiskStorage(tmp_path)
    job_id = uuid.uuid4()
    key = storage.path_for(job_id, "CERT-0001")
    assert key == f"{job_id}/CERT-0001.pdf"
    await storage.write(key, b"%PDF-1.4 hello")
    assert await storage.exists(key)
    assert await storage.read(key) == b"%PDF-1.4 hello"


async def test_write_is_atomic_overwrite(tmp_path: Path):
    storage = DiskStorage(tmp_path)
    job_id = uuid.uuid4()
    key = storage.path_for(job_id, "CERT-0001")
    await storage.write(key, b"v1")
    await storage.write(key, b"v2")
    assert await storage.read(key) == b"v2"


async def test_exists_false_for_missing(tmp_path: Path):
    storage = DiskStorage(tmp_path)
    assert await storage.exists("does/not-exist.pdf") is False


async def test_write_cleans_tmp_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    storage = DiskStorage(tmp_path)
    job_id = uuid.uuid4()
    key = storage.path_for(job_id, "CERT-0001")

    def _boom(src: str | os.PathLike[str], dst: str | os.PathLike[str]) -> None:
        raise OSError("disk on fire")

    monkeypatch.setattr(os, "replace", _boom)
    with pytest.raises(OSError):
        await storage.write(key, b"data")
    leftovers = list(Path(tmp_path).rglob("*.tmp"))
    assert leftovers == []
