"""Shared API shapes: pagination envelope, counts, error envelope."""

from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class Page(BaseModel, Generic[T]):
    items: list[T]
    page: int
    size: int
    total_items: int
    total_pages: int


class Counts(BaseModel):
    """Per-job certificate counts (7 keys; ``pending`` is PENDING only)."""

    total: int
    done: int
    failed: int
    pending: int
    processing: int
    retrying: int
    cancelled: int


class ErrorBody(BaseModel):
    code: str
    message: str
    details: list[Any] = []


class ErrorEnvelope(BaseModel):
    error: ErrorBody
