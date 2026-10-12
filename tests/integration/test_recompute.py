"""Integration tests for the shared ``recompute_job`` (Task 10).

TDD Step 1: these fail before implementation (no ``recompute_job`` in
``services/recompute.py`` yet). Real PG via the session-scoped container
``session_factory`` fixture.
"""

from __future__ import annotations

import uuid
from datetime import date
from uuid import UUID

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from aereo_assignment.db.models import (
    Certificate,
    CertificateStatus,
    Job,
    JobStatus,
)
from aereo_assignment.repositories.certificates import CertificatesRepository
from aereo_assignment.repositories.jobs import JobsRepository
from aereo_assignment.services.recompute import recompute_job


def _job(**kwargs) -> Job:
    kwargs.setdefault("event_name", "Recompute Test Event")
    kwargs.setdefault("event_date", date(2026, 10, 1))
    kwargs.setdefault("issuer", "Aereo")
    kwargs.setdefault("idempotency_key", f"recompute-{uuid.uuid4().hex}")
    kwargs.setdefault("status", JobStatus.PROCESSING)
    return Job(**kwargs)


def _cert(job_id: UUID, status: CertificateStatus) -> Certificate:
    return Certificate(
        job_id=job_id,
        recipient_name="Ada Lovelace",
        recipient_email="ada@example.com",
        extra_metadata={},
        certificate_number=f"REC-2026-{uuid.uuid4().hex[:12].upper()}",
        status=status,
    )


async def _seed(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    job_status: JobStatus = JobStatus.PROCESSING,
    cert_statuses: list[CertificateStatus],
) -> UUID:
    async with session_factory() as s:
        job = await JobsRepository(s).insert(_job(status=job_status))
        await s.flush()
        await CertificatesRepository(s).insert_many(
            [_cert(job.id, st) for st in cert_statuses]
        )
        await s.commit()
        return job.id


@pytest.mark.parametrize(
    ("certs", "expected"),
    [
        (
            [CertificateStatus.DONE, CertificateStatus.DONE],
            JobStatus.COMPLETED,
        ),
        (
            [CertificateStatus.DONE, CertificateStatus.FAILED],
            JobStatus.COMPLETED_WITH_ERRORS,
        ),
        (
            [CertificateStatus.FAILED, CertificateStatus.FAILED],
            JobStatus.FAILED,
        ),
        (
            [CertificateStatus.DONE, CertificateStatus.PENDING],
            JobStatus.PROCESSING,
        ),
        (
            [CertificateStatus.RETRYING],
            JobStatus.PROCESSING,
        ),
    ],
)
async def test_recompute_transitions(
    session_factory, certs: list[CertificateStatus], expected: JobStatus
) -> None:
    job_id = await _seed(session_factory, cert_statuses=certs)
    async with session_factory() as s:
        assert await recompute_job(s, job_id) == expected
    async with session_factory() as s:
        job = await JobsRepository(s).get(job_id)
        assert job is not None
        assert job.status == expected


async def test_recompute_pending_job_with_finished_certs_goes_processing(
    session_factory,
) -> None:
    # M7 edge: the dispatch CAS has not run yet (job still PENDING) but a
    # cert already finished; recompute may legally drive PENDING -> PROCESSING.
    job_id = await _seed(
        session_factory,
        job_status=JobStatus.PENDING,
        cert_statuses=[CertificateStatus.DONE, CertificateStatus.PENDING],
    )
    async with session_factory() as s:
        before = await JobsRepository(s).get(job_id)
        assert before is not None and before.status == JobStatus.PENDING
    async with session_factory() as s:
        assert await recompute_job(s, job_id) == JobStatus.PROCESSING
    async with session_factory() as s:
        after = await JobsRepository(s).get(job_id)
        assert after is not None and after.status == JobStatus.PROCESSING


async def test_recompute_after_cancel_is_dropped(session_factory) -> None:
    # H2: cancel wins the race; the recompute write is dropped, never an
    # un-cancel. Rowcount 0 -> None, job stays CANCELLED.
    job_id = await _seed(
        session_factory,
        cert_statuses=[CertificateStatus.DONE, CertificateStatus.PENDING],
    )
    async with session_factory() as s:
        rowcount = await JobsRepository(s).cas_transition(
            job_id,
            from_statuses=[JobStatus.PENDING, JobStatus.PROCESSING],
            to_status=JobStatus.CANCELLED,
            set_completed_at=True,
        )
        assert rowcount == 1
        await s.commit()
    async with session_factory() as s:
        assert await recompute_job(s, job_id) is None
    async with session_factory() as s:
        job = await JobsRepository(s).get(job_id)
        assert job is not None and job.status == JobStatus.CANCELLED


async def test_recompute_unknown_job_returns_none(session_factory) -> None:
    async with session_factory() as s:
        assert await recompute_job(s, uuid.uuid4()) is None
