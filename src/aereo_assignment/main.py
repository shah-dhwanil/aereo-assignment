"""FastAPI application factory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

from aereo_assignment.api.errors import (
    ApiError,
    api_error_handler,
    unhandled_error_handler,
    validation_error_handler,
)
from aereo_assignment.api.routes.jobs import router as jobs_router
from aereo_assignment.config import Settings, get_settings
from aereo_assignment.db import session as session_module
from aereo_assignment.db.session import create_engine_from, create_session_factory
from aereo_assignment.logging import setup_logging


def _load_settings() -> Settings:
    try:
        return get_settings()
    except ValidationError:
        return Settings(
            database_url="postgresql+asyncpg://localhost:5432/unconfigured",
            redis_url="redis://localhost:6379/0",
        )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    setup_logging(settings)
    settings.storage_dir.mkdir(parents=True, exist_ok=True)
    engine = create_engine_from(settings)
    factory = create_session_factory(engine)
    app.state.engine = engine
    app.state.session_factory = factory
    session_module.session_factory = factory
    try:
        yield
    finally:
        await engine.dispose()


def create_app(
    settings: Settings | None = None,
    broker=None,
) -> FastAPI:
    resolved = settings or _load_settings()
    app = FastAPI(title=resolved.app_name, lifespan=lifespan)
    app.state.settings = resolved
    if broker is not None:
        app.state.broker = broker
    else:
        from aereo_assignment.worker.broker import broker as default_broker

        app.state.broker = default_broker

    app.exception_handler(ApiError)(api_error_handler)
    app.exception_handler(RequestValidationError)(validation_error_handler)
    app.exception_handler(Exception)(unhandled_error_handler)

    app.include_router(jobs_router, prefix="/api/v1")

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    return app


app = create_app()
