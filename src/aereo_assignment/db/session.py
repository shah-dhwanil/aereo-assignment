"""Async engine and session factory helpers."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from aereo_assignment.config import Settings

session_factory: async_sessionmaker[AsyncSession] | None = None
"""Module-level factory installed by the app lifespan; read by ``get_session``."""


def create_engine_from(settings: Settings, **kwargs: Any) -> AsyncEngine:
    """Build an async engine from application settings.

    Extra ``kwargs`` pass through to ``create_async_engine`` (e.g. tests use
    ``poolclass=NullPool`` so a session-scoped engine is safe to share across
    function-scoped event loops).
    """
    return create_async_engine(settings.database_url, **kwargs)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Build a session factory with ``expire_on_commit=False``."""
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency yielding a session from the module-level factory."""
    if session_factory is None:
        raise RuntimeError("session_factory is not initialised")
    async with session_factory() as session:
        yield session
