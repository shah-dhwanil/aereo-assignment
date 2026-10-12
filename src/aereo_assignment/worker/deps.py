"""Task dependency providers over monkeypatchable module-level state.

Each ``get_*_deps`` is a plain sync function usable as a ``TaskiqDepends``
default. Tests override the module globals (``_session_factory``, ``_broker``,
``_settings``, ``_storage``) via ``monkeypatch``; in production the globals
lazily initialise from the environment on first use.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from taskiq import AsyncBroker

from aereo_assignment.config import Settings, get_settings
from aereo_assignment.db.session import create_engine_from, create_session_factory
from aereo_assignment.services.storage import DiskStorage, Storage


@dataclass(frozen=True)
class DispatchDeps:
    session_factory: async_sessionmaker[AsyncSession]
    broker: AsyncBroker


@dataclass(frozen=True)
class GenerateDeps:
    session_factory: async_sessionmaker[AsyncSession]
    storage: Storage
    settings: Settings
    broker: AsyncBroker


@dataclass(frozen=True)
class ReconcileDeps:
    session_factory: async_sessionmaker[AsyncSession]
    broker: AsyncBroker
    settings: Settings


_session_factory: async_sessionmaker[AsyncSession] | None = None
_broker: AsyncBroker | None = None
_settings: Settings | None = None
_storage: Storage | None = None


def _get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = get_settings()
    return _settings


def _get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        _session_factory = create_session_factory(
            create_engine_from(_get_settings())
        )
    return _session_factory


def _get_broker() -> AsyncBroker:
    global _broker
    if _broker is None:
        from aereo_assignment.worker.broker import broker

        _broker = broker
    return _broker


def _get_storage() -> Storage:
    global _storage
    if _storage is None:
        _storage = DiskStorage(_get_settings().storage_dir)
    return _storage


def get_dispatch_deps() -> DispatchDeps:
    """Provide :class:`DispatchDeps` from module-level state."""
    return DispatchDeps(
        session_factory=_get_session_factory(), broker=_get_broker()
    )


def get_generate_deps() -> GenerateDeps:
    """Provide :class:`GenerateDeps` from module-level state."""
    settings = _get_settings()
    return GenerateDeps(
        session_factory=_get_session_factory(),
        storage=_get_storage(),
        settings=settings,
        broker=_get_broker(),
    )


def get_reconcile_deps() -> ReconcileDeps:
    """Provide :class:`ReconcileDeps` from module-level state."""
    return ReconcileDeps(
        session_factory=_get_session_factory(),
        broker=_get_broker(),
        settings=_get_settings(),
    )
