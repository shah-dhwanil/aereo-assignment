"""Job routes: submit, list, detail, cancel."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from aereo_assignment.db.models import JobStatus
from aereo_assignment.db.session import get_session
from aereo_assignment.schemas.common import Page
from aereo_assignment.schemas.jobs import (
    JobCreateRequest,
    JobDetail,
    JobSummary,
    JobView,
)
from aereo_assignment.services.job_service import (
    cancel_job,
    get_job_view,
    list_jobs_page,
    submit_job,
)

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


@router.get("/jobs")
async def list_jobs(
    request: Request,
    session: AsyncSession = Depends(get_session),  # noqa: B008 - FastAPI DI idiom
    status: JobStatus | None = None,
    page: int = Query(1, ge=1),
    size: int | None = Query(default=None, ge=1),
) -> Page[JobSummary]:
    settings = request.app.state.settings
    if size is None:
        size = settings.page_size_default
    else:
        size = min(size, settings.page_size_max)
    return await list_jobs_page(session, status=status, page=page, size=size)


@router.get("/jobs/{job_id}")
async def get_job(
    job_id: UUID,
    session: AsyncSession = Depends(get_session),  # noqa: B008 - FastAPI DI idiom
) -> JobDetail:
    return await get_job_view(session, job_id)


@router.post("/jobs/{job_id}/cancel")
async def cancel(
    job_id: UUID,
    session: AsyncSession = Depends(get_session),  # noqa: B008 - FastAPI DI idiom
) -> JobView:
    return await cancel_job(session, job_id)
