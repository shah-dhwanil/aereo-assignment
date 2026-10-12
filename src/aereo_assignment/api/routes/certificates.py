"""Certificate retrieval routes: list, PDF download, ZIP bundle."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import Response, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.background import BackgroundTask

from aereo_assignment.db.models import CertificateStatus
from aereo_assignment.db.session import get_session
from aereo_assignment.schemas.certificates import CertificateView
from aereo_assignment.schemas.common import Page
from aereo_assignment.services.certificate_service import (
    build_job_zip,
    download_certificate,
    list_certificates_page,
)
from aereo_assignment.services.storage import DiskStorage

router = APIRouter(tags=["certificates"])


@router.get("/jobs/{job_id}/certificates")
async def list_certificates(
    job_id: UUID,
    request: Request,
    session: AsyncSession = Depends(get_session),  # noqa: B008 - FastAPI DI idiom
    status: CertificateStatus | None = None,
    page: int = Query(1, ge=1),
    size: int | None = Query(default=None, ge=1),
) -> Page[CertificateView]:
    settings = request.app.state.settings
    if size is None:
        size = settings.page_size_default
    else:
        size = min(size, settings.page_size_max)
    return await list_certificates_page(
        session,
        job_id,
        status=status,
        page=page,
        size=size,
        base_url=settings.base_url,
    )


@router.get("/jobs/{job_id}/certificates/{cert_id}/download")
async def download(
    job_id: UUID,
    cert_id: UUID,
    request: Request,
    session: AsyncSession = Depends(get_session),  # noqa: B008 - FastAPI DI idiom
) -> Response:
    storage = DiskStorage(request.app.state.settings.storage_dir)
    cert, data = await download_certificate(session, job_id, cert_id, storage)
    return Response(
        content=data,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="{cert.certificate_number}.pdf"'
        },
    )


@router.get("/jobs/{job_id}/download.zip")
async def download_zip(
    job_id: UUID,
    request: Request,
    session: AsyncSession = Depends(get_session),  # noqa: B008 - FastAPI DI idiom
) -> StreamingResponse:
    storage = DiskStorage(request.app.state.settings.storage_dir)
    file, _skipped = await build_job_zip(session, job_id, storage)
    return StreamingResponse(
        file, media_type="application/zip", background=BackgroundTask(file.close)
    )
