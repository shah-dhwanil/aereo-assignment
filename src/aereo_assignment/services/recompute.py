"""Pure job-status recompute decision (spec section 10.2) plus the shared
``recompute_job`` step (spec section 10.2 SQL, H2 terminal guard).

``recompute_job`` used to live as a private copy in
``worker.tasks.generate``; Task 10 moved it here unchanged in signature so
the worker and reconcile share one implementation.
"""

from __future__ import annotations

from collections.abc import Mapping
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from aereo_assignment.db.models import CertificateStatus, JobStatus
from aereo_assignment.repositories.certificates import CertificatesRepository
from aereo_assignment.repositories.jobs import JobsRepository


def decide_job_status(counts: Mapping[CertificateStatus, int]) -> JobStatus:
    """Map per-status certificate counts to the job status.

    - all DONE -> COMPLETED
    - all FAILED (total > 0) -> FAILED
    - only DONE + FAILED present, at least one of each -> COMPLETED_WITH_ERRORS
    - anything else (unfinished rows remain, empty, or all-zero) -> PROCESSING
    """
    done = counts.get(CertificateStatus.DONE, 0)
    failed = counts.get(CertificateStatus.FAILED, 0)
    total = sum(counts.values())
    if total == 0:
        return JobStatus.PROCESSING
    if done == total:
        return JobStatus.COMPLETED
    if failed == total:
        return JobStatus.FAILED
    if done > 0 and failed > 0 and done + failed == total:
        return JobStatus.COMPLETED_WITH_ERRORS
    return JobStatus.PROCESSING


async def recompute_job(session: AsyncSession, job_id: UUID) -> JobStatus | None:
    """Recompute and CAS-apply the job status from certificate counts.

    Returns the applied decision, or ``None`` when the job row is gone or a
    racing terminal transition (e.g. cancel, H2) won and the write was
    dropped — the job is never un-cancelled.
    """
    jobs = JobsRepository(session)
    if await jobs.get(job_id) is None:
        return None
    counts = await CertificatesRepository(session).status_counts([job_id])
    decision = decide_job_status(counts.get(job_id, {}))
    rowcount = await jobs.recompute_cas(job_id, decision)
    await session.commit()
    if rowcount == 0:
        return None
    return decision
