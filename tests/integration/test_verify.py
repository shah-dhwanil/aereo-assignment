"""Integration tests for GET /api/v1/verify/{number} (Task 12).

TDD Step 1: these fail before implementation. App fixture follows the
T8/T10 pattern. Rows are scoped to this module via TAG.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
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

TAG = uuid.uuid4().hex[:8]


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


async def _seed(
    session_factory: async_sessionmaker[AsyncSession],
    index: int,
    status: CertificateStatus,
) -> tuple[UUID, Certificate]:
    async with session_factory() as s:
        job = await JobsRepository(s).insert(
            Job(
                event_name=f"Verify-{TAG}-{index}",
                event_date=date(2026, 3, 15),
                issuer="Aereo Academy",
                idempotency_key=f"ver-{TAG}-{index}-{uuid.uuid4().hex}",
                status=JobStatus.PROCESSING,
            )
        )
        await s.flush()
        cert = Certificate(
            job_id=job.id,
            recipient_name="Dhwanil Shah",
            recipient_email="d@x.com",
            extra_metadata={},
            certificate_number=f"VFY-{TAG}-{index}-{uuid.uuid4().hex[:8].upper()}",
            status=status,
            generated_at=datetime.now(UTC)
            if status is CertificateStatus.DONE
            else None,
        )
        await CertificatesRepository(s).insert_many([cert])
        await s.commit()
        return job.id, cert


async def test_verify_done_200(client, session_factory) -> None:
    job_id, cert = await _seed(session_factory, 1, CertificateStatus.DONE)
    resp = await client.get(f"/api/v1/verify/{cert.certificate_number}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body == {
        "certificate_number": cert.certificate_number,
        "recipient_name": "Dhwanil Shah",
        "recipient_email": "d@x.com",
        "event_name": f"Verify-{TAG}-1",
        "event_date": "2026-03-15",
        "issuer": "Aereo Academy",
        "status": "DONE",
        "issued_at": body["issued_at"],
        "job_id": str(job_id),
    }
    assert body["issued_at"] is not None
    assert datetime.fromisoformat(body["issued_at"]) == cert.generated_at


async def test_verify_pending_number_200_with_status(client, session_factory) -> None:
    job_id, cert = await _seed(session_factory, 2, CertificateStatus.PENDING)
    resp = await client.get(f"/api/v1/verify/{cert.certificate_number}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "PENDING"
    assert body["issued_at"] is None
    assert body["certificate_number"] == cert.certificate_number
    assert body["job_id"] == str(job_id)


async def test_verify_unknown_404(client) -> None:
    resp = await client.get(f"/api/v1/verify/CERT-2026-NOT{uuid.uuid4().hex[:8]}")
    assert resp.status_code == 404, resp.text
    assert resp.json()["error"]["code"] == "CERTIFICATE_NOT_FOUND"
