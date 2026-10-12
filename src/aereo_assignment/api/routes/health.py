"""Deep health check: Postgres ``SELECT 1`` + Redis ``PING`` (spec section 8.9)."""

from __future__ import annotations

import redis.asyncio as redis_asyncio
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from aereo_assignment.db.session import get_session

router = APIRouter(tags=["health"])


@router.get("/health")
async def health(
    request: Request,
    session: AsyncSession = Depends(get_session),  # noqa: B008 - FastAPI DI idiom
) -> JSONResponse:
    settings = request.app.state.settings
    checks: dict[str, str] = {}
    try:
        await session.execute(text("SELECT 1"))
        checks["database"] = "ok"
    except Exception as exc:  # noqa: BLE001 - health must report, never raise
        checks["database"] = f"error: {exc}"
    try:
        client = redis_asyncio.from_url(settings.redis_url)
        try:
            await client.ping()
            checks["redis"] = "ok"
        finally:
            await client.aclose()
    except Exception as exc:  # noqa: BLE001 - health must report, never raise
        checks["redis"] = f"error: {exc}"
    if all(value == "ok" for value in checks.values()):
        return JSONResponse(status_code=200, content={"status": "ok", "checks": checks})
    return JSONResponse(status_code=503, content={"status": "error", "checks": checks})
