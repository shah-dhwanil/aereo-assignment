"""Retrieval services: cert listing, PDF download, ZIP bundle, verify lookup.

All reads are read-only against Postgres (+ the blob store); the only
state rule enforced here is X2 — a DONE row whose file is gone stays DONE
(``download_certificate`` raises ``FileMissing``; ``build_job_zip`` skips
the member and records it in the ``SKIPPED.txt`` manifest).
"""

from __future__ import annotations

import logging
import zipfile
from tempfile import SpooledTemporaryFile
from typing import BinaryIO
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from aereo_assignment.api.errors import (
    CertificateNotFound,
    CertNotFound,
    CertNotReady,
    FileMissing,
    JobNotFound,
    NoCertificatesReady,
)
from aereo_assignment.db.models import Certificate, CertificateStatus
from aereo_assignment.repositories.certificates import CertificatesRepository
from aereo_assignment.repositories.jobs import JobsRepository
from aereo_assignment.schemas.certificates import CertificateView, VerifyView
from aereo_assignment.schemas.common import Page
from aereo_assignment.services.storage import Storage

logger = logging.getLogger(__name__)

_ZIP_SPOOL_MAX_SIZE = 8 * 1024 * 1024
_LIST_PAGE_LIMIT = 500


def _download_url(base_url: str, job_id: UUID, cert_id: UUID) -> str:
    return f"{base_url}/api/v1/jobs/{job_id}/certificates/{cert_id}/download"


def _view(cert: Certificate, *, base_url: str) -> CertificateView:
    return CertificateView(
        certificate_id=cert.id,
        recipient_name=cert.recipient_name,
        recipient_email=cert.recipient_email,
        status=cert.status,
        certificate_number=cert.certificate_number,
        attempts=cert.attempts,
        error_message=cert.error_message,
        download_url=_download_url(base_url, cert.job_id, cert.id)
        if cert.status is CertificateStatus.DONE
        else None,
        generated_at=cert.generated_at,
    )


async def list_certificates_page(
    session: AsyncSession,
    job_id: UUID,
    *,
    status: CertificateStatus | None,
    page: int,
    size: int,
    base_url: str,
) -> Page[CertificateView]:
    """One page of certificate rows for a job; unknown jobs raise ``JobNotFound``."""
    if await JobsRepository(session).get(job_id) is None:
        raise JobNotFound()
    repo = CertificatesRepository(session)
    rows = await repo.list_for_job(
        job_id, status=status, offset=(page - 1) * size, limit=size
    )
    counts = await repo.status_counts([job_id])
    by_status = counts.get(job_id, {})
    total_items = (
        by_status.get(status, 0) if status is not None else sum(by_status.values())
    )
    total_pages = (total_items + size - 1) // size if total_items else 0
    return Page[CertificateView](
        items=[_view(cert, base_url=base_url) for cert in rows],
        page=page,
        size=size,
        total_items=total_items,
        total_pages=total_pages,
    )


async def download_certificate(
    session: AsyncSession,
    job_id: UUID,
    cert_id: UUID,
    storage: Storage,
) -> tuple[Certificate, bytes]:
    """Return the (row, PDF bytes) for a DONE certificate.

    Non-DONE rows raise ``CertNotReady``; DONE rows whose file is gone raise
    ``FileMissing`` — the row stays DONE (X2), never demoted.
    """
    cert = await CertificatesRepository(session).get(cert_id)
    if cert is None or cert.job_id != job_id:
        raise CertNotFound()
    if cert.status is not CertificateStatus.DONE:
        raise CertNotReady()
    key = cert.file_path or storage.path_for(job_id, cert.certificate_number)
    if not await storage.exists(key):
        raise FileMissing()
    try:
        return cert, await storage.read(key)
    except OSError as exc:
        logger.warning("certificate file vanished for %s: %s", key, exc)
        raise FileMissing() from exc


async def _all_done(session: AsyncSession, job_id: UUID) -> list[Certificate]:
    repo = CertificatesRepository(session)
    out: list[Certificate] = []
    offset = 0
    while True:
        batch = await repo.list_for_job(
            job_id,
            status=CertificateStatus.DONE,
            offset=offset,
            limit=_LIST_PAGE_LIMIT,
        )
        out.extend(batch)
        if len(batch) < _LIST_PAGE_LIMIT:
            break
        offset += _LIST_PAGE_LIMIT
    return out


async def build_job_zip(
    session: AsyncSession, job_id: UUID, storage: Storage
) -> tuple[BinaryIO, list[str]]:
    """Bundle every DONE certificate whose file exists into a rewound ZIP.

    Returns the (rewound ``SpooledTemporaryFile(max_size=8MB)``, skipped
    certificate numbers). Missing files are skipped, logged, and listed in
    a ``SKIPPED.txt`` manifest inside the ZIP. Zero DONE rows raise
    ``NoCertificatesReady``; unknown jobs raise ``JobNotFound``.
    """
    if await JobsRepository(session).get(job_id) is None:
        raise JobNotFound()
    done = await _all_done(session, job_id)
    if not done:
        raise NoCertificatesReady()
    skipped: list[str] = []
    present: list[tuple[Certificate, bytes]] = []
    for cert in done:
        key = cert.file_path or storage.path_for(job_id, cert.certificate_number)
        if not await storage.exists(key):
            logger.warning("skipping missing certificate file %s", key)
            skipped.append(cert.certificate_number)
            continue
        try:
            present.append((cert, await storage.read(key)))
        except OSError as exc:
            logger.warning("skipping unreadable certificate file %s: %s", key, exc)
            skipped.append(cert.certificate_number)
    spool: BinaryIO = SpooledTemporaryFile(  # noqa: SIM115 - returned to caller; closed via BackgroundTask
        max_size=_ZIP_SPOOL_MAX_SIZE
    )
    with zipfile.ZipFile(spool, "w", zipfile.ZIP_DEFLATED) as zf:
        for cert, data in present:
            zf.writestr(f"{cert.certificate_number}.pdf", data)
        if skipped:
            zf.writestr("SKIPPED.txt", "\n".join([*skipped, ""]))
    spool.seek(0)
    return spool, skipped


async def verify_certificate(session: AsyncSession, number: str) -> VerifyView:
    """Public verification lookup by certificate number.

    Known numbers always return 200-shaped data (even when not DONE, with
    ``issued_at`` None); unknown numbers raise ``CertificateNotFound``.
    """
    cert = await CertificatesRepository(session).get_by_number(number)
    if cert is None:
        raise CertificateNotFound()
    job = await JobsRepository(session).get(cert.job_id)
    assert job is not None, "certificate references a missing job"
    return VerifyView(
        certificate_number=cert.certificate_number,
        recipient_name=cert.recipient_name,
        recipient_email=cert.recipient_email,
        event_name=job.event_name,
        event_date=job.event_date,
        issuer=job.issuer,
        status=cert.status,
        issued_at=cert.generated_at,
        job_id=cert.job_id,
    )
