"""Integration tests for POST /api/v1/jobs submit path (Task 8).

TDD Step 1: these fail before implementation (no app/schemas/service yet).
App fixture uses RecordingBroker via app.state.broker (never plain
InMemoryBroker, which would execute dispatch inline).
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from conftest import RecordingBroker
from httpx import ASGITransport, AsyncClient


def _payload(n: int = 3, **overrides) -> dict:
    recipients = [
        {"name": f"Person {i}", "email": f"person{i}@example.com"} for i in range(n)
    ]
    base = {
        "event_name": "Advanced Robotics Workshop 2026",
        "event_date": "2026-03-15",
        "issuer": "Aereo Academy",
        "description": "For successful completion.",
        "recipients": recipients,
    }
    base.update(overrides)
    return base


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
    broker = RecordingBroker()
    application.state.broker = broker
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


def _broker(app) -> RecordingBroker:
    return app.state.broker


async def test_submit_returns_202_with_counts(client) -> None:
    resp = await client.post("/api/v1/jobs", json=_payload(3))
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["total"] == 3
    assert body["valid"] == 3
    assert body["invalid"] == 0
    assert body["status"] == "PENDING"
    assert body["job_id"]
    assert body["created_at"]


async def test_mixed_validity_invalid_row_becomes_failed_cert(
    client, app, session_factory
) -> None:
    from aereo_assignment.db.models import CertificateStatus
    from aereo_assignment.repositories.certificates import CertificatesRepository

    payload = _payload(3)
    payload["recipients"][1] = {"name": "Bad Row", "email": "not-an-email"}
    resp = await client.post("/api/v1/jobs", json=payload)
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["total"] == 3
    assert body["valid"] == 2
    assert body["invalid"] == 1

    job_id = body["job_id"]
    async with session_factory() as s:
        rows = await CertificatesRepository(s).list_for_job(
            __import__("uuid").UUID(job_id), status=None, offset=0, limit=10
        )
        assert len(rows) == 3
        failed = [c for c in rows if c.status == CertificateStatus.FAILED]
        assert len(failed) == 1
        row = failed[0]
        assert row.attempts == 0
        assert "email" in (row.error_message or "").lower()

    broker = _broker(app)
    assert len(broker.kicked) == 1
    assert broker.kicked[0].task_name == "dispatch_job"
    blob = (
        broker.kicked[0].message.decode()
        if isinstance(broker.kicked[0].message, (bytes, bytearray))
        else str(broker.kicked[0].message)
    )
    assert str(row.id) not in blob


async def test_zero_valid_422_with_row_details(client, session_factory) -> None:
    from uuid import UUID  # noqa: F401 (keep import local per repo style)

    from aereo_assignment.repositories.jobs import JobsRepository

    async with session_factory() as s:
        before = await JobsRepository(s).count(status=None)
    payload = _payload(2)
    payload["recipients"] = [
        {"name": "   ", "email": "not-an-email"},
        {"name": "Bad", "email": "also-bad"},
    ]
    resp = await client.post("/api/v1/jobs", json=payload)
    assert resp.status_code == 422, resp.text
    body = resp.json()
    assert body["error"]["code"] == "VALIDATION_ERROR"
    details = body["error"]["details"]
    assert len(details) == 2
    for d in details:
        assert "index" in d and "field" in d and "message" in d
    async with session_factory() as s:
        after = await JobsRepository(s).count(status=None)
    assert after == before


async def test_schema_error_uses_envelope(client) -> None:
    payload = _payload(1)
    del payload["event_name"]
    resp = await client.post("/api/v1/jobs", json=payload)
    assert resp.status_code == 422, resp.text
    body = resp.json()
    assert "error" in body
    assert body["error"]["code"] == "VALIDATION_ERROR"
    blob = str(body["error"]["details"])
    assert "event_name" in blob


async def test_replay_same_key_returns_snapshot_plus_header(client) -> None:
    key = f"replay-{uuid.uuid4().hex}"
    payload = _payload(2)
    first = await client.post(
        "/api/v1/jobs", json=payload, headers={"Idempotency-Key": key}
    )
    assert first.status_code == 202, first.text
    assert "Idempotency-Replayed" not in first.headers
    second = await client.post(
        "/api/v1/jobs", json=payload, headers={"Idempotency-Key": key}
    )
    assert second.status_code == 202, second.text
    assert second.headers.get("Idempotency-Replayed") == "true"
    assert second.json() == first.json()
    assert second.json()["job_id"] == first.json()["job_id"]


async def test_same_key_different_payload_409(client) -> None:
    key = f"conflict-{uuid.uuid4().hex}"
    first = await client.post(
        "/api/v1/jobs", json=_payload(1), headers={"Idempotency-Key": key}
    )
    assert first.status_code == 202, first.text
    other = _payload(1)
    other["event_name"] = "A Totally Different Event"
    resp = await client.post(
        "/api/v1/jobs", json=other, headers={"Idempotency-Key": key}
    )
    assert resp.status_code == 409, resp.text
    assert resp.json()["error"]["code"] == "IDEMPOTENCY_KEY_REUSED"


async def test_concurrent_same_key_one_202(client) -> None:
    key = f"race-{uuid.uuid4().hex}"
    payload = _payload(2)

    async def _one():
        return await client.post(
            "/api/v1/jobs", json=payload, headers={"Idempotency-Key": key}
        )

    first, second = await asyncio.gather(_one(), _one())
    assert first.status_code == 202, first.text
    assert second.status_code == 202, second.text
    assert first.status_code < 500 and second.status_code < 500
    assert first.json()["job_id"] == second.json()["job_id"]
    replayed = [
        r for r in (first, second) if r.headers.get("Idempotency-Replayed") == "true"
    ]
    assert len(replayed) == 1


async def test_no_key_creates_duplicate_jobs(client) -> None:
    payload = _payload(1)
    first = await client.post("/api/v1/jobs", json=payload)
    second = await client.post("/api/v1/jobs", json=payload)
    assert first.status_code == 202, first.text
    assert second.status_code == 202, second.text
    assert first.json()["job_id"] != second.json()["job_id"]


async def test_dispatch_enqueue_failure_still_202(client, app) -> None:
    broker = _broker(app)

    async def _boom(message) -> None:
        raise RuntimeError("redis down")

    broker.kick = _boom  # type: ignore[method-assign]
    resp = await client.post("/api/v1/jobs", json=_payload(1))
    assert resp.status_code == 202, resp.text
