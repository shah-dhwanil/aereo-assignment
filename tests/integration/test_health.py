"""Integration tests for GET /health (Task 12).

TDD Step 1: these fail before implementation (the placeholder returns
``{"status": "ok"}`` with no checks and never 503s).

The OK path needs a real Redis, so this module spins a throwaway
``redis:7`` container (session-scoped) and points the app settings at it.
"""

from __future__ import annotations

import pytest
from conftest import RecordingBroker
from httpx import ASGITransport, AsyncClient


@pytest.fixture(scope="session")
def redis_url():
    from testcontainers.redis import RedisContainer

    with RedisContainer("redis:7") as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"


@pytest.fixture
def app(session_factory, tmp_path, redis_url):
    from aereo_assignment.config import Settings
    from aereo_assignment.db.session import get_session
    from aereo_assignment.main import create_app

    settings = Settings(
        database_url="postgresql+asyncpg://localhost:5432/test",
        redis_url=redis_url,
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


async def test_health_ok(client) -> None:
    resp = await client.get("/health")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "status": "ok",
        "checks": {"database": "ok", "redis": "ok"},
    }


async def test_health_degraded_503(client, monkeypatch) -> None:
    async def _boom(self) -> None:
        raise ConnectionError("redis down")

    monkeypatch.setattr("redis.asyncio.Redis.ping", _boom)
    resp = await client.get("/health")
    assert resp.status_code == 503, resp.text
    body = resp.json()
    assert body["status"] == "error"
    assert body["checks"]["database"] == "ok"
    assert "redis down" in body["checks"]["redis"]
