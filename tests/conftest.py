"""Shared pytest fixtures.

Task 1 seeds this module with imports only; real fixtures land in Task 2.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator

import pytest
from alembic.config import Config as AlembicConfig
from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.pool import NullPool
from testcontainers.postgres import PostgresContainer

from aereo_assignment.config import Settings
from aereo_assignment.db.session import create_engine_from, create_session_factory
from alembic import command as alembic_command


def _container_url(raw: str, dialect: str) -> str:
    """Point a testcontainers URL at the given SQLAlchemy dialect."""
    _, _, rest = raw.partition("://")
    return f"postgresql+{dialect}://{rest}"


def _urls(container: PostgresContainer) -> tuple[str, str]:
    raw = container.get_connection_url()
    return _container_url(raw, "psycopg"), _container_url(raw, "asyncpg")


@pytest.fixture(scope="session")
def pg_container() -> Iterator[PostgresContainer]:
    with PostgresContainer("postgres:16") as container:
        yield container


@pytest.fixture(scope="session")
def migrate(pg_container: PostgresContainer) -> None:
    """Upgrade the container database to head exactly once per session."""
    _, async_url = _urls(pg_container)
    cfg = AlembicConfig("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", async_url)
    alembic_command.upgrade(cfg, "head")


@pytest.fixture(scope="session")
def engine(pg_container: PostgresContainer, migrate: None) -> Iterator[Engine]:
    """Synchronous engine for blocking inspection (e.g. ``inspect``).

    The app itself is async-only; a sync driver (psycopg, test-only) backs
    this fixture because an asyncpg engine cannot perform blocking IO from
    a sync test (``MissingGreenlet``). ``sync_engine`` is aliased to self so
    ``inspect(engine.sync_engine)`` works. Async tests use
    ``session_factory``/``session`` instead.
    """
    sync_url, _ = _urls(pg_container)
    eng = create_engine(sync_url)
    eng.sync_engine = eng  # type: ignore[attr-defined]
    yield eng
    eng.dispose()


@pytest.fixture(scope="session")
def session_factory(
    pg_container: PostgresContainer, migrate: None
) -> Iterator[async_sessionmaker[AsyncSession]]:
    """Async sessions over the container DB.

    ``NullPool``: the engine is session-scoped but tests run in
    function-scoped event loops, and pooled asyncpg connections are bound to
    the loop that opened them — a shared pool hands out dead connections to
    later tests. NullPool opens/closes per checkout in the current loop.
    """
    _, async_url = _urls(pg_container)
    settings = Settings(database_url=async_url, redis_url="redis://localhost:6379/0")
    eng = create_engine_from(settings, poolclass=NullPool)
    try:
        yield create_session_factory(eng)
    finally:
        asyncio.run(eng.dispose())


@pytest.fixture
async def session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    async with session_factory() as sess:
        try:
            yield sess
        finally:
            await sess.rollback()
