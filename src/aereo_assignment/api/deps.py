"""Shared API dependencies (pagination helper)."""

from __future__ import annotations

from dataclasses import dataclass

from fastapi import Query

from aereo_assignment.config import get_settings
from aereo_assignment.db.models import JobStatus


def _page_defaults() -> tuple[int, int]:
    try:
        settings = get_settings()
    except Exception:  # noqa: BLE001 - any settings failure falls back to defaults
        return 50, 200
    return settings.page_size_default, settings.page_size_max


_DEFAULT_SIZE, _MAX_SIZE = _page_defaults()


@dataclass(frozen=True)
class PageParams:
    status: JobStatus | None
    page: int
    size: int

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.size

    @property
    def limit(self) -> int:
        return self.size


def pagination(
    status: JobStatus | None = None,
    page: int = Query(1, ge=1),
    size: int = Query(_DEFAULT_SIZE, ge=1, le=_MAX_SIZE),
) -> PageParams:
    return PageParams(status=status, page=page, size=size)
