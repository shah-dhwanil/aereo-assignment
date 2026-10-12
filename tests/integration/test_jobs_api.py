"""Integration tests for GET /api/v1/jobs + GET /api/v1/jobs/{id} (Task 10).

TDD Step 1: these fail before implementation (no list/detail service /
routes yet). App fixture follows the T8 pattern (``RecordingBroker`` via
``app.state.broker``, ``dependency_overrides`` for the session).

NOTE: the container DB is shared across the whole pytest session, so other
test modules' rows are visible here. Assertions are subset/relationship
based, never on absolute totals.
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


def _job(index: int, status: JobStatus) -> Job:
    return Job(
        event_name=f"ListTest-{TAG}-{index}",
        event_date=date(2026, 10, 1),
        issuer="Aereo",
        idempotency_key=f"list-{TAG}-{index}-{uuid.uuid4().hex}",
        status=status,
    )


def _cert(job_id: UUID, status: CertificateStatus) -> Certificate:
    return Certificate(
        job_id=job_id,
        recipient_name="Ada Lovelace",
        recipient_email="ada@example.com",
        extra_metadata={},
        certificate_number=f"LST-2026-{uuid.uuid4().hex[:12].upper()}",
        status=status,
    )


async def _seed(
    session_factory: async_sessionmaker[AsyncSession],
    index: int,
    status: JobStatus,
    cert_statuses: list[CertificateStatus] | None = None,
) -> UUID:
    async with session_factory() as s:
        job = await JobsRepository(s).insert(_job(index, status))
        await s.flush()
        if cert_statuses:
            await CertificatesRepository(s).insert_many(
                [_cert(job.id, st) for st in cert_statuses]
            )
        await s.commit()
        return job.id


async def test_list_jobs_pagination_and_filter(client, session_factory) -> None:
    mine: list[UUID] = []
    statuses = [
        JobStatus.PENDING,
        JobStatus.PROCESSING,
        JobStatus.COMPLETED,
        JobStatus.COMPLETED,
        JobStatus.FAILED,
    ]
    for i, st in enumerate(statuses):
        mine.append(await _seed(session_factory, i, st))
    counted_id = await _seed(
        session_factory,
        99,
        JobStatus.PROCESSING,
        cert_statuses=[
            CertificateStatus.PENDING,
            CertificateStatus.PENDING,
            CertificateStatus.FAILED,
        ],
    )

    # Size above the max clamps to 200 instead of 422.
    resp = await client.get("/api/v1/jobs", params={"size": 500})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["size"] == 200
    assert body["total_items"] >= 6
    by_id = {item["job_id"]: item for item in body["items"]}
    for job_id in [*mine, counted_id]:
        assert str(job_id) in by_id
    counted = by_id[str(counted_id)]
    assert counted["counts"] == {
        "total": 3,
        "done": 0,
        "failed": 1,
        "pending": 2,
        "processing": 0,
        "retrying": 0,
        "cancelled": 0,
    }
    for item in body["items"]:
        assert set(item["counts"]) == {
            "total",
            "done",
            "failed",
            "pending",
            "processing",
            "retrying",
            "cancelled",
        }

    # Page math is internally consistent.
    first = await client.get("/api/v1/jobs", params={"size": 2, "page": 1})
    assert first.status_code == 200, first.text
    first_body = first.json()
    assert first_body["page"] == 1
    assert first_body["size"] == 2
    assert first_body["total_pages"] == -(
        -first_body["total_items"] // first_body["size"]
    )
    assert len(first_body["items"]) <= 2
    second = await client.get("/api/v1/jobs", params={"size": 2, "page": 2})
    assert second.status_code == 200, second.text
    second_body = second.json()
    assert second_body["page"] == 2
    page1_ids = {item["job_id"] for item in first_body["items"]}
    page2_ids = {item["job_id"] for item in second_body["items"]}
    assert page1_ids and page2_ids
    assert page1_ids.isdisjoint(page2_ids)

    # Status filter: every returned row matches, and our COMPLETED rows show up.
    filt = await client.get("/api/v1/jobs", params={"status": "COMPLETED", "size": 200})
    assert filt.status_code == 200, filt.text
    filt_body = filt.json()
    assert filt_body["items"]
    assert all(item["status"] == "COMPLETED" for item in filt_body["items"])
    filt_ids = {item["job_id"] for item in filt_body["items"]}
    assert {str(mine[2]), str(mine[3])} <= filt_ids


async def test_job_detail_shape(client, session_factory) -> None:
    job_id = await _seed(
        session_factory,
        7,
        JobStatus.PROCESSING,
        cert_statuses=[
            CertificateStatus.PENDING,
            CertificateStatus.PROCESSING,
            CertificateStatus.DONE,
            CertificateStatus.FAILED,
        ],
    )
    resp = await client.get(f"/api/v1/jobs/{job_id}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["job_id"] == str(job_id)
    assert body["event_name"] == f"ListTest-{TAG}-7"
    assert body["event_date"] == "2026-10-01"
    assert body["issuer"] == "Aereo"
    assert body["status"] == "PROCESSING"
    assert body["counts"] == {
        "total": 4,
        "done": 1,
        "failed": 1,
        "pending": 1,
        "processing": 1,
        "retrying": 0,
        "cancelled": 0,
    }
    assert body["created_at"]
    assert body["completed_at"] is None
    assert body["updated_at"]


async def test_job_detail_404(client) -> None:
    resp = await client.get(f"/api/v1/jobs/{uuid.uuid4()}")
    assert resp.status_code == 404, resp.text
    assert resp.json()["error"]["code"] == "JOB_NOT_FOUND"
