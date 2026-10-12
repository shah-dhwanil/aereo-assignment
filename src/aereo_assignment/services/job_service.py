"""Submit-path service: validate, insert job + certs, best-effort dispatch."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from uuid import UUID, uuid4

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from taskiq import AsyncBroker

from aereo_assignment.api.errors import (
    IdempotencyKeyReused,
    JobAlreadyFinal,
    JobNotFound,
    ZeroValidRows,
)
from aereo_assignment.config import Settings
from aereo_assignment.db.models import Certificate, CertificateStatus, Job, JobStatus
from aereo_assignment.repositories.certificates import CertificatesRepository
from aereo_assignment.repositories.jobs import JobsRepository
from aereo_assignment.schemas.common import Counts, Page
from aereo_assignment.schemas.jobs import (
    JobCreateRequest,
    JobDetail,
    JobSummary,
    JobView,
)
from aereo_assignment.services.idempotency import canonical_request_hash
from aereo_assignment.services.numbering import generate_unique_numbers
from aereo_assignment.services.validation import validate_recipients
from aereo_assignment.worker.tasks.dispatch import dispatch_job

logger = logging.getLogger(__name__)

_MAX_NUMBER_ATTEMPTS = 5


@dataclass
class SubmitResult:
    job: Job
    total: int
    valid: int
    invalid: int
    replayed: bool


def _snapshot_for(job: Job, *, total: int, valid: int, invalid: int) -> dict:
    created_at = getattr(job, "created_at", None)
    if created_at is None:
        created_iso = datetime.now().astimezone().isoformat()
    else:
        created_iso = created_at.isoformat()
    return {
        "job_id": str(job.id),
        "status": JobStatus.PENDING.value,
        "total": total,
        "valid": valid,
        "invalid": invalid,
        "created_at": created_iso,
    }


async def submit_job(
    session: AsyncSession,
    *,
    payload: JobCreateRequest,
    idempotency_key: str | None,
    broker: AsyncBroker,
    settings: Settings,
) -> SubmitResult:
    """Validate rows, insert job + certs in one TX, best-effort dispatch."""
    if not idempotency_key:
        idempotency_key = None

    valid, row_errors = validate_recipients(payload.recipients)
    if not valid:
        raise ZeroValidRows(
            details=[
                {"index": e.index, "field": e.field, "message": e.message}
                for e in row_errors
            ]
        )

    request_hash = canonical_request_hash(payload.model_dump(mode="json"))
    total = len(payload.recipients)
    n_valid = len(valid)
    n_invalid = len(row_errors)

    jobs = JobsRepository(session)
    if idempotency_key is not None:
        existing = await jobs.get_by_idempotency_key(idempotency_key)
        if existing is not None:
            if existing.request_hash == request_hash:
                snap = existing.submission_response or {}
                return SubmitResult(
                    job=existing,
                    total=int(snap.get("total", total)),
                    valid=int(snap.get("valid", n_valid)),
                    invalid=int(snap.get("invalid", n_invalid)),
                    replayed=True,
                )
            raise IdempotencyKeyReused()

    job = Job(
        id=uuid4(),
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        event_name=payload.event_name,
        event_date=payload.event_date,
        issuer=payload.issuer,
        description=payload.description,
        status=JobStatus.PENDING,
    )
    try:
        await jobs.insert(job)
        await session.flush()
        await session.refresh(job)
    except IntegrityError:
        await session.rollback()
        if idempotency_key is not None:
            existing = await jobs.get_by_idempotency_key(idempotency_key)
            if existing is not None:
                if existing.request_hash == request_hash:
                    snap = existing.submission_response or {}
                    return SubmitResult(
                        job=existing,
                        total=int(snap.get("total", total)),
                        valid=int(snap.get("valid", n_valid)),
                        invalid=int(snap.get("invalid", n_invalid)),
                        replayed=True,
                    )
                raise IdempotencyKeyReused() from None
        raise

    created_at = getattr(job, "created_at", None)
    year = created_at.year if created_at is not None else date.today().year

    error_by_index = {e.index: e for e in row_errors}
    certs_repo = CertificatesRepository(session)
    numbers = generate_unique_numbers(year=year, count=total)
    for attempt in range(_MAX_NUMBER_ATTEMPTS):
        valid_iter = iter(valid)
        certs: list[Certificate] = []
        for i in range(total):
            number = numbers[i]
            if i in error_by_index:
                err = error_by_index[i]
                raw = payload.recipients[i]
                name = raw.name if isinstance(raw.name, str) else ""
                email = raw.email if isinstance(raw.email, str) else ""
                meta = (
                    dict(raw.extra_metadata)
                    if isinstance(raw.extra_metadata, dict)
                    else {}
                )
                certs.append(
                    Certificate(
                        job_id=job.id,
                        recipient_name=name,
                        recipient_email=email,
                        extra_metadata=meta,
                        certificate_number=number,
                        status=CertificateStatus.FAILED,
                        attempts=0,
                        error_message=f"{err.field}: {err.message}",
                    )
                )
            else:
                v = next(valid_iter)
                certs.append(
                    Certificate(
                        job_id=job.id,
                        recipient_name=v.name,
                        recipient_email=v.email,
                        extra_metadata=dict(v.extra_metadata),
                        certificate_number=number,
                        status=CertificateStatus.PENDING,
                        attempts=0,
                    )
                )
        try:
            async with session.begin_nested():
                await certs_repo.insert_many(certs)
            break
        except IntegrityError:
            for stale in certs:
                session.expunge(stale)
            if attempt == _MAX_NUMBER_ATTEMPTS - 1:
                raise
            numbers = generate_unique_numbers(year=year, count=total)
            continue

    snapshot = _snapshot_for(job, total=total, valid=n_valid, invalid=n_invalid)
    job.submission_response = snapshot
    await session.commit()

    try:
        await dispatch_job.kicker().with_broker(broker).kiq(str(job.id))
    except Exception:
        logger.warning("dispatch enqueue failed for job %s", job.id, exc_info=True)

    return SubmitResult(
        job=job, total=total, valid=n_valid, invalid=n_invalid, replayed=False
    )


def build_counts(counts_by_status: Mapping[CertificateStatus, int]) -> Counts:
    """Map per-status certificate counts to the 7-key ``Counts`` shape.

    ``pending`` counts PENDING only and ``processing`` counts PROCESSING
    (plan Global Constraints / Ruling R3 family).
    """
    total = sum(counts_by_status.values())
    return Counts(
        total=total,
        done=counts_by_status.get(CertificateStatus.DONE, 0),
        failed=counts_by_status.get(CertificateStatus.FAILED, 0),
        pending=counts_by_status.get(CertificateStatus.PENDING, 0),
        processing=counts_by_status.get(CertificateStatus.PROCESSING, 0),
        retrying=counts_by_status.get(CertificateStatus.RETRYING, 0),
        cancelled=counts_by_status.get(CertificateStatus.CANCELLED, 0),
    )


async def get_job_view(session: AsyncSession, job_id: UUID) -> JobDetail:
    """Full job detail with derived counts; raises ``JobNotFound``."""
    job = await JobsRepository(session).get(job_id)
    if job is None:
        raise JobNotFound()
    counts = await CertificatesRepository(session).status_counts([job_id])
    return JobDetail(
        job_id=job.id,
        event_name=job.event_name,
        status=job.status,
        counts=build_counts(counts.get(job_id, {})),
        created_at=job.created_at,
        completed_at=job.completed_at,
        event_date=job.event_date,
        issuer=job.issuer,
        updated_at=job.updated_at,
    )


async def list_jobs_page(
    session: AsyncSession,
    *,
    status: JobStatus | None,
    page: int,
    size: int,
) -> Page[JobSummary]:
    """One page of job summaries, each with derived counts."""
    jobs = JobsRepository(session)
    total_items = await jobs.count(status=status)
    rows = await jobs.list(status=status, offset=(page - 1) * size, limit=size)
    counts = await CertificatesRepository(session).status_counts(
        [job.id for job in rows]
    )
    items = [
        JobSummary(
            job_id=job.id,
            event_name=job.event_name,
            status=job.status,
            counts=build_counts(counts.get(job.id, {})),
            created_at=job.created_at,
            completed_at=job.completed_at,
        )
        for job in rows
    ]
    total_pages = (total_items + size - 1) // size if total_items else 0
    return Page[JobSummary](
        items=items,
        page=page,
        size=size,
        total_items=total_items,
        total_pages=total_pages,
    )


async def cancel_job(session: AsyncSession, job_id: UUID) -> JobView:
    """Soft-cancel a job: CAS to CANCELLED, cancel open certs, return a view.

    Idempotent on an already-CANCELLED job (200 with the same state).
    Raises ``JobNotFound`` for unknown ids and ``JobAlreadyFinal`` when the
    job already reached a non-cancel terminal state.
    """
    jobs = JobsRepository(session)
    rowcount = await jobs.cas_transition(
        job_id,
        from_statuses=[JobStatus.PENDING, JobStatus.PROCESSING],
        to_status=JobStatus.CANCELLED,
        set_completed_at=True,
    )
    if rowcount == 0:
        job = await jobs.get(job_id)
        if job is None:
            raise JobNotFound()
        if job.status != JobStatus.CANCELLED:
            raise JobAlreadyFinal()
    await CertificatesRepository(session).cancel_open(job_id)
    await session.commit()
    counts = await CertificatesRepository(session).status_counts([job_id])
    return JobView(
        job_id=job_id,
        status=JobStatus.CANCELLED,
        counts=build_counts(counts.get(job_id, {})),
    )
