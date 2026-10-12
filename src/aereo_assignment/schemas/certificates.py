"""Retrieval-path response schemas (spec sections 8.5, 8.8)."""

from __future__ import annotations

from datetime import date, datetime
from uuid import UUID

from pydantic import BaseModel

from aereo_assignment.db.models import CertificateStatus


class CertificateView(BaseModel):
    """One row of ``GET /api/v1/jobs/{id}/certificates`` (spec section 8.5)."""

    certificate_id: UUID
    recipient_name: str
    recipient_email: str
    status: CertificateStatus
    certificate_number: str
    attempts: int
    error_message: str | None
    download_url: str | None
    generated_at: datetime | None


class VerifyView(BaseModel):
    """``GET /api/v1/verify/{number}`` (spec section 8.8)."""

    certificate_number: str
    recipient_name: str
    recipient_email: str
    event_name: str
    event_date: date
    issuer: str
    status: CertificateStatus
    issued_at: datetime | None
    job_id: UUID
