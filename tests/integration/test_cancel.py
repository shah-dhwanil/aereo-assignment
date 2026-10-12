"""Integration tests for POST /api/v1/jobs/{id}/cancel (Task 10).

TDD Step 1: these fail before implementation (no ``cancel_job`` service /
cancel route yet). App fixture follows the T8 pattern (``RecordingBroker``
via ``app.state.broker``, ``dependency_overrides`` for the session).
"""

from __future__ import annotations

import uuid
from datetime import date
from uuid import UUID

import pytest
from conftest import RecordingBroker
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from aereo_assignment.db.models import (
    Certificate,
    CertificateStatus,
    Job,
    JobStatus,
)
from aereo_assignment.repositories.certificates import CertificatesRepository
from aereo_assignment.repositories.jobs import JobsRepository


@pytest.fixture
def app(session_factory, tmp_path):
    from aereo_assignment.config import Settings
    from aereo_assignment.db.session import get_session
    from aereo_assignment.main import create_app

    settings = Settings(
        database_url="postgresql+asyncpg://localhost:5432/test",
        redis_url="redis://localhost:6379/0",
        storage_dir=tmp_path / "storage",
    )
    application = create_app(settings=settings)
    application.state.broker = RecordingBroker()
    settings.storage_dir.mkdir(parents=True, exist_ok=True)

    async def _override():
        async with session_factory() as s:
            yield s

    application.dependency_overrides[get_session] = _override
    return application


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


def _job(**kwargs) -> Job:
    kwargs.setdefault("event_name", "Cancel Test Event")
    kwargs.setdefault("event_date", date(2026, 10, 1))
    kwargs.setdefault("issuer", "Aereo")
    kwargs.setdefault("idempotency_key", f"cancel-{uuid.uuid4().hex}")
    kwargs.setdefault("status", JobStatus.PROCESSING)
    return Job(**kwargs)


def _cert(job_id: UUID, status: CertificateStatus) -> Certificate:
    return Certificate(
        job_id=job_id,
        recipient_name="Ada Lovelace",
        recipient_email="ada@example.com",
        extra_metadata={},
        certificate_number=f"CXL-2026-{uuid.uuid4().hex[:12].upper()}",
        status=status,
    )


async def _seed(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    job_status: JobStatus = JobStatus.PROCESSING,
    cert_statuses: list[CertificateStatus] | None = None,
) -> UUID:
    async with session_factory() as s:
        job = await JobsRepository(s).insert(_job(status=job_status))
        await s.flush()
        if cert_statuses:
            await CertificatesRepository(s).insert_many(
                [_cert(job.id, st) for st in cert_statuses]
            )
        await s.commit()
        return job.id


async def _cert_statuses(
    session_factory, job_id: UUID
) -> dict[UUID, CertificateStatus]:
    async with session_factory() as s:
        rows = await CertificatesRepository(s).list_for_job(
            job_id, status=None, offset=0, limit=100
        )
        return {row.id: row.status for row in rows}


async def test_cancel_soft_cancels_pending_and_retrying_but_not_processing(
    client, session_factory
) -> None:
    job_id = await _seed(
        session_factory,
        job_status=JobStatus.PROCESSING,
        cert_statuses=[
            CertificateStatus.PENDING,
            CertificateStatus.PENDING,
            CertificateStatus.RETRYING,
            CertificateStatus.PROCESSING,
            CertificateStatus.DONE,
            CertificateStatus.FAILED,
        ],
    )
    resp = await client.post(f"/api/v1/jobs/{job_id}/cancel")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["job_id"] == str(job_id)
    assert body["status"] == "CANCELLED"
    assert body["counts"] == {
        "total": 6,
        "done": 1,
        "failed": 1,
        "pending": 0,
        "processing": 1,
        "retrying": 0,
        "cancelled": 3,
    }

    statuses = sorted((await _cert_statuses(session_factory, job_id)).values())
    assert statuses == sorted(
        [
            CertificateStatus.CANCELLED,
            CertificateStatus.CANCELLED,
            CertificateStatus.CANCELLED,
            CertificateStatus.PROCESSING,
            CertificateStatus.DONE,
            CertificateStatus.FAILED,
        ]
    )
    async with session_factory() as s:
        job = await JobsRepository(s).get(job_id)
        assert job is not None
        assert job.status == JobStatus.CANCELLED
        assert job.completed_at is not None


async def test_cancel_idempotent_repeat_200(client, session_factory) -> None:
    job_id = await _seed(
        session_factory,
        job_status=JobStatus.PENDING,
        cert_statuses=[CertificateStatus.PENDING],
    )
    first = await client.post(f"/api/v1/jobs/{job_id}/cancel")
    assert first.status_code == 200, first.text
    second = await client.post(f"/api/v1/jobs/{job_id}/cancel")
    assert second.status_code == 200, second.text
    assert second.json() == first.json()
    assert second.json()["status"] == "CANCELLED"


@pytest.mark.parametrize(
    "terminal",
    [
        JobStatus.COMPLETED,
        JobStatus.COMPLETED_WITH_ERRORS,
        JobStatus.FAILED,
    ],
)
async def test_cancel_completed_job_409(client, session_factory, terminal) -> None:
    job_id = await _seed(
        session_factory,
        job_status=terminal,
        cert_statuses=[CertificateStatus.DONE],
    )
    resp = await client.post(f"/api/v1/jobs/{job_id}/cancel")
    assert resp.status_code == 409, resp.text
    assert resp.json()["error"]["code"] == "JOB_ALREADY_FINAL"


async def test_cancel_unknown_404(client) -> None:
    resp = await client.post(f"/api/v1/jobs/{uuid.uuid4()}/cancel")
    assert resp.status_code == 404, resp.text
    assert resp.json()["error"]["code"] == "JOB_NOT_FOUND"
