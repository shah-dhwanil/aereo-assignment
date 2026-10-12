"""ORM models for jobs and certificates (spec section 9)."""

from __future__ import annotations

import enum
import uuid
from datetime import date, datetime

from sqlalchemy import (
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    SmallInteger,
    Text,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from aereo_assignment.db.base import Base


class JobStatus(enum.StrEnum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    COMPLETED_WITH_ERRORS = "COMPLETED_WITH_ERRORS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class CertificateStatus(enum.StrEnum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    RETRYING = "RETRYING"
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


def _job_status_enum() -> Enum:
    return Enum(
        JobStatus,
        name="job_status",
        values_callable=lambda es: [e.value for e in es],
    )


def _cert_status_enum() -> Enum:
    return Enum(
        CertificateStatus,
        name="cert_status",
        values_callable=lambda es: [e.value for e in es],
    )


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        Index("ix_jobs_idempotency_key", "idempotency_key"),
        Index("ix_jobs_status_created_at", "status", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    idempotency_key: Mapped[str | None] = mapped_column(
        Text, unique=True, nullable=True
    )
    request_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    event_name: Mapped[str] = mapped_column(Text, nullable=False)
    event_date: Mapped[date] = mapped_column(Date, nullable=False)
    issuer: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[JobStatus] = mapped_column(
        _job_status_enum(),
        nullable=False,
        default=JobStatus.PENDING,
        server_default="PENDING",
    )
    submission_response: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    certificates: Mapped[list[Certificate]] = relationship(
        back_populates="job",
        cascade="all, delete-orphan",
    )


class Certificate(Base):
    __tablename__ = "certificates"
    __table_args__ = (Index("ix_certificates_job_id_status", "job_id", "status"),)

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
    )
    recipient_name: Mapped[str] = mapped_column(Text, nullable=False)
    recipient_email: Mapped[str] = mapped_column(Text, nullable=False)
    extra_metadata: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    certificate_number: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    status: Mapped[CertificateStatus] = mapped_column(
        _cert_status_enum(),
        nullable=False,
        default=CertificateStatus.PENDING,
        server_default="PENDING",
    )
    attempts: Mapped[int] = mapped_column(
        SmallInteger,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    claim_token: Mapped[uuid.UUID | None] = mapped_column(Uuid, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    file_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )
    generated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    job: Mapped[Job] = relationship(back_populates="certificates")
