"""Job routes (POST submit only in this task)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from aereo_assignment.db.session import get_session
from aereo_assignment.schemas.jobs import JobCreateRequest
from aereo_assignment.services.job_service import submit_job

router = APIRouter(tags=["jobs"])


@router.post("/jobs", status_code=202)
async def create_job(
    payload: JobCreateRequest,
    request: Request,
    session: AsyncSession = Depends(get_session),  # noqa: B008 - FastAPI DI idiom
) -> JSONResponse:
    key = request.headers.get("Idempotency-Key") or None
    broker = request.app.state.broker
    settings = request.app.state.settings
    result = await submit_job(
        session,
        payload=payload,
        idempotency_key=key,
        broker=broker,
        settings=settings,
    )
    body = dict(result.job.submission_response or {})
    if result.replayed:
        return JSONResponse(
            status_code=202,
            content=body,
            headers={"Idempotency-Replayed": "true"},
        )
    return JSONResponse(status_code=202, content=body)
