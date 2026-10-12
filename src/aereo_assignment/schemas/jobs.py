"""Submit-path request/response schemas.

Envelope-level pydantic only: per-row content rules (email shape, name
bounds, metadata limits) live in ``validate_recipients`` so invalid rows
can become per-certificate FAILED rows instead of killing the request.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Annotated, Any
from uuid import UUID

from pydantic import BaseModel, Field, StringConstraints

from aereo_assignment.db.models import JobStatus

Stripped200 = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)
]
StrippedName = Annotated[str, StringConstraints(strip_whitespace=True)]


class RecipientIn(BaseModel):
    name: StrippedName
    email: StrippedName
    extra_metadata: dict[str, Any] = Field(default_factory=dict)


class JobCreateRequest(BaseModel):
    event_name: Stripped200
    event_date: date
    issuer: Stripped200
    description: str | None = Field(default=None, max_length=1000)
    recipients: list[RecipientIn] = Field(min_length=1, max_length=5000)


class JobAcceptedResponse(BaseModel):
    job_id: UUID
    status: JobStatus
    total: int
    valid: int
    invalid: int
    created_at: datetime
