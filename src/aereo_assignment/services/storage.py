"""Storage protocol + atomic disk implementation."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Protocol
from uuid import UUID, uuid4


class Storage(Protocol):
    """Blob store for generated certificate PDFs."""

    def path_for(self, job_id: UUID, certificate_number: str) -> str:
        """Return the posix-style key for a certificate PDF."""
        ...

    async def write(self, key: str, data: bytes) -> None: ...
    async def read(self, key: str) -> bytes: ...
    async def exists(self, key: str) -> bool: ...


class DiskStorage:
    """Store blobs as files under ``root`` with atomic writes."""

    def __init__(self, root: Path) -> None:
        self._root = root

    def path_for(self, job_id: UUID, certificate_number: str) -> str:
        return f"{job_id}/{certificate_number}.pdf"

    def _target(self, key: str) -> Path:
        return self._root / key

    def _write_sync(self, key: str, data: bytes) -> None:
        target = self._target(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.parent / f".{target.name}.{uuid4().hex}.tmp"
        try:
            tmp.write_bytes(data)
            os.replace(tmp, target)
        except Exception:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            raise

    async def write(self, key: str, data: bytes) -> None:
        await asyncio.to_thread(self._write_sync, key, data)

    async def read(self, key: str) -> bytes:
        return await asyncio.to_thread(self._target(key).read_bytes)

    async def exists(self, key: str) -> bool:
        return await asyncio.to_thread(self._target(key).exists)
