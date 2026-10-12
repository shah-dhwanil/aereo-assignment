"""PDF generation task: claim a certificate, render, store, CAS the outcome.

Claim/CAS ownership (spec section 10.1): ``claim`` hands out a fresh token per
delivery; every ``cas_outcome`` is predicated on still holding that token, so
a stale owner can never clobber the winner (C3). Retry taxonomy: permanent
errors fail fast, transient errors below the DB-attempts cap go back to
RETRYING and raise for ``SmartRetryMiddleware`` redelivery (the DB cap — not
the middleware counter — is authoritative), transient errors at the cap fail
as exhausted.
"""

from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

from pydantic import ValidationError
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession
from taskiq import TaskiqDepends

from aereo_assignment.config import Settings, get_settings
from aereo_assignment.db.models import CertificateStatus, JobStatus
from aereo_assignment.pdf.renderer import render_certificate
from aereo_assignment.repositories.certificates import CertificatesRepository
from aereo_assignment.repositories.jobs import JobsRepository
from aereo_assignment.services.errors import classify_error
from aereo_assignment.services.recompute import decide_job_status
from aereo_assignment.worker.broker import broker
from aereo_assignment.worker.deps import GenerateDeps, get_generate_deps


def _load_settings() -> Settings:
    """Settings for task labels; falls back when env is unconfigured.

    Import-time wiring must succeed without env configured (same pattern as
    ``worker.broker``): label values just need to exist at build time, while
    the core always reads the per-invocation ``deps.settings``.
    """
    try:
        return get_settings()
    except ValidationError:
        return Settings(
            database_url="postgresql+asyncpg://localhost:5432/unconfigured",
            redis_url="redis://localhost:6379/0",
        )


_task_settings = _load_settings()


async def recompute_job(session: AsyncSession, job_id: UUID) -> JobStatus | None:
    """Recompute and CAS-apply the job status from certificate counts.

    Minimal private step owned by this task; Task 10 moves it unchanged to
    ``services/recompute.py``. Returns ``None`` when the job row is gone.
    """
    jobs = JobsRepository(session)
    if await jobs.get(job_id) is None:
        return None
    counts = await CertificatesRepository(session).status_counts([job_id])
    decision = decide_job_status(counts.get(job_id, {}))
    await jobs.recompute_cas(job_id, decision)
    await session.commit()
    return decision


# NOTE: installed taskiq 0.13 ``broker.task`` takes the name positionally
# (``task_name``); ``name=`` would silently become a label and the task would
# register as ``module:func``. ``retry_on_error``/``max_retries``/``delay`` are
# labels consumed by SmartRetryMiddleware.
@broker.task(
    "generate_pdf",
    retry_on_error=True,
    max_retries=_task_settings.max_generation_attempts,
    delay=_task_settings.retry_backoff_seconds,
)
async def generate_pdf(
    cert_id: str, deps: GenerateDeps = TaskiqDepends(get_generate_deps)  # noqa: B008 - taskiq DI idiom mandated by spec
) -> str:
    """Taskiq wrapper: payloads are strings; the core parses ``UUID(...)``."""
    return await generate_pdf_core(UUID(cert_id), deps)


async def _cas_outcome(
    deps: GenerateDeps,
    cert_id: UUID,
    *,
    token: UUID,
    to_status: CertificateStatus,
    error_message: str | None = None,
    file_path: str | None = None,
    generated_at=None,
) -> int:
    async with deps.session_factory() as session:
        rowcount = await CertificatesRepository(session).cas_outcome(
            cert_id,
            token=token,
            to_status=to_status,
            error_message=error_message,
            file_path=file_path,
            generated_at=generated_at,
        )
        await session.commit()
        return rowcount


async def _recompute(deps: GenerateDeps, job_id: UUID) -> JobStatus | None:
    async with deps.session_factory() as session:
        return await recompute_job(session, job_id)


async def generate_pdf_core(cert_id: UUID, deps: GenerateDeps) -> str:
    """Claim, render, store, CAS the outcome, recompute the job.

    Returns one of ``"done"``, ``"skipped:unclaimed"``, ``"skipped:stale"``,
    ``"failed:permanent"``, ``"failed:exhausted"``; re-raises transient errors
    below the attempts cap for middleware redelivery.
    """
    settings = deps.settings
    token = uuid4()
    async with deps.session_factory() as session:
        cert = await CertificatesRepository(session).claim(
            cert_id, max_attempts=settings.max_generation_attempts, token=token
        )
        await session.commit()
        if cert is None:
            return "skipped:unclaimed"
        job = await JobsRepository(session).get(cert.job_id)
        assert job is not None, "certificate references a missing job"
        job_id = cert.job_id
        attempts = cert.attempts
        recipient_name = cert.recipient_name
        recipient_email = cert.recipient_email
        certificate_number = cert.certificate_number
        extra_metadata = dict(cert.extra_metadata or {})
        event_name = job.event_name
        event_date = job.event_date
        issuer = job.issuer
        description = job.description
    verify_url = f"{settings.base_url}/api/v1/verify/{certificate_number}"
    try:
        pdf = await asyncio.to_thread(
            render_certificate,
            recipient_name=recipient_name,
            recipient_email=recipient_email,
            event_name=event_name,
            event_date=event_date,
            issuer=issuer,
            description=description,
            certificate_number=certificate_number,
            verify_url=verify_url,
            extra_metadata=extra_metadata,
        )
        key = deps.storage.path_for(job_id, certificate_number)
        await deps.storage.write(key, pdf)
    except Exception as exc:
        kind = classify_error(exc)
        if kind == "permanent":
            await _cas_outcome(
                deps,
                cert_id,
                token=token,
                to_status=CertificateStatus.FAILED,
                error_message=str(exc),
            )
            await _recompute(deps, job_id)
            return "failed:permanent"
        if attempts >= settings.max_generation_attempts:
            await _cas_outcome(
                deps,
                cert_id,
                token=token,
                to_status=CertificateStatus.FAILED,
                error_message=str(exc),
            )
            await _recompute(deps, job_id)
            return "failed:exhausted"
        await _cas_outcome(
            deps,
            cert_id,
            token=token,
            to_status=CertificateStatus.RETRYING,
            error_message=str(exc),
        )
        raise
    rowcount = await _cas_outcome(
        deps,
        cert_id,
        token=token,
        to_status=CertificateStatus.DONE,
        file_path=key,
        generated_at=func.now(),
    )
    if rowcount == 0:
        return "skipped:stale"
    await _recompute(deps, job_id)
    return "done"
