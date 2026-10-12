"""Periodic + startup recovery sweep (spec section 10.3, H3/C1/C2/X1).

One implementation, two triggers: the ``reconcile`` task below fires on the
scheduler ``interval`` label, and ``worker.broker.reconcile_on_startup`` runs
the same :func:`run_reconcile` at broker startup. All staleness comparisons
use the database clock inside the repositories (M5); every step below runs in
the brief's order because later steps depend on earlier ones settling first.
"""

from __future__ import annotations

from uuid import UUID

from pydantic import ValidationError
from taskiq import TaskiqDepends

from aereo_assignment.config import Settings, get_settings
from aereo_assignment.db.models import CertificateStatus, JobStatus
from aereo_assignment.repositories.certificates import CertificatesRepository
from aereo_assignment.repositories.jobs import JobsRepository
from aereo_assignment.services.recompute import recompute_job
from aereo_assignment.worker.broker import broker
from aereo_assignment.worker.deps import ReconcileDeps, get_reconcile_deps
from aereo_assignment.worker.tasks.dispatch import dispatch_job
from aereo_assignment.worker.tasks.generate import generate_pdf

ReconcileSummary = dict[str, int]
# Keys: stale_certs_reset, stale_certs_failed, certs_reenqueued,
#       jobs_dispatched, jobs_recomputed.

_EXHAUSTED_MESSAGE = "reconcile: generation attempts exhausted"


def _load_settings() -> Settings:
    """Settings for task labels; falls back when env is unconfigured.

    Import-time wiring must succeed without env configured (same pattern as
    ``worker.broker`` and ``worker.tasks.generate``): label values just need
    to exist at build time, while the core always reads the per-invocation
    ``deps.settings``.
    """
    try:
        return get_settings()
    except ValidationError:
        return Settings(
            database_url="postgresql+asyncpg://localhost:5432/unconfigured",
            redis_url="redis://localhost:6379/0",
        )


_task_settings = _load_settings()


# NOTE: installed taskiq 0.13 ``broker.task`` takes the name positionally
# (``task_name``); ``name=`` would silently become a label and the task would
# register as ``module:func``. The ``schedule`` label is consumed by
# ``LabelScheduleSource`` (verified: it reads ``task.labels["schedule"]`` and
# accepts entries with an ``interval`` key).
@broker.task(
    "reconcile",
    schedule=[{"interval": _task_settings.reconcile_interval_seconds}],
)
async def reconcile(
    deps: ReconcileDeps = TaskiqDepends(get_reconcile_deps),  # noqa: B008 - taskiq DI idiom mandated by spec
) -> ReconcileSummary:
    """Taskiq wrapper: periodic trigger delegates to :func:`run_reconcile`."""
    return await run_reconcile(deps)


async def run_reconcile(deps: ReconcileDeps) -> ReconcileSummary:
    """Sweep crashed work back into the pipeline; return per-bucket counts.

    Steps run in order: (1) stale in-flight certs are failed at the attempts
    cap (with a job recompute each) or collected for reset; (2) the collected
    ids are reset to PENDING and immediately re-enqueued — never relying on
    stale-PENDING detection for ids reset in this same pass, since they now
    carry a fresh ``updated_at``; (3) other stale PENDING certs (lost
    messages for never-claimed rows) are re-enqueued; (4) stale PENDING jobs
    are re-dispatched (fan-out runs regardless of the job CAS, X1) and stale
    PROCESSING jobs are recomputed (closes the crash-between-DONE-and-recompute
    window, C2).
    """
    settings = deps.settings
    grace = settings.reconcile_grace_seconds
    max_attempts = settings.max_generation_attempts
    summary: ReconcileSummary = {
        "stale_certs_reset": 0,
        "stale_certs_failed": 0,
        "certs_reenqueued": 0,
        "jobs_dispatched": 0,
        "jobs_recomputed": 0,
    }

    # Step 1: sweep stale in-flight certs; snapshot (id, job_id) first so no
    # later commit expiry can fault attribute access.
    async with deps.session_factory() as session:
        stale = await CertificatesRepository(session).find_stale(
            statuses=[CertificateStatus.PROCESSING, CertificateStatus.RETRYING],
            grace_seconds=grace,
        )
        snapshot = [(c.id, c.job_id, c.attempts) for c in stale]
        reset_ids: list[UUID] = []
        exhausted_job_ids: list[UUID] = []
        for cert_id, job_id, attempts in snapshot:
            if attempts >= max_attempts:
                rowcount = await CertificatesRepository(session).force_fail_stale(
                    cert_id, _EXHAUSTED_MESSAGE, max_attempts=max_attempts
                )
                summary["stale_certs_failed"] += rowcount
                if rowcount:
                    exhausted_job_ids.append(job_id)
            else:
                reset_ids.append(cert_id)
        await session.commit()
    for job_id in exhausted_job_ids:
        async with deps.session_factory() as session:
            await recompute_job(session, job_id)
        summary["jobs_recomputed"] += 1

    # Step 2: reset, then immediately re-enqueue (fresh updated_at means step
    # 3's stale-PENDING scan would miss these — hence the direct kiq).
    if reset_ids:
        async with deps.session_factory() as session:
            summary["stale_certs_reset"] = await CertificatesRepository(
                session
            ).reset_to_pending(reset_ids)
            await session.commit()
        for cert_id in reset_ids:
            await generate_pdf.kicker().with_broker(deps.broker).kiq(str(cert_id))
            summary["certs_reenqueued"] += 1

    # Step 3: lost messages for never-claimed certs.
    async with deps.session_factory() as session:
        pending_ids = list(
            await CertificatesRepository(session).find_stale_pending(
                grace_seconds=grace
            )
        )
    for cert_id in pending_ids:
        await generate_pdf.kicker().with_broker(deps.broker).kiq(str(cert_id))
        summary["certs_reenqueued"] += 1

    # Step 4: stale jobs — PENDING re-dispatches, PROCESSING recomputes.
    async with deps.session_factory() as session:
        stale_jobs = await JobsRepository(session).find_stale(
            statuses=[JobStatus.PENDING, JobStatus.PROCESSING],
            grace_seconds=grace,
        )
        pending_job_ids = [j.id for j in stale_jobs if j.status == JobStatus.PENDING]
        processing_job_ids = [
            j.id for j in stale_jobs if j.status == JobStatus.PROCESSING
        ]
    for job_id in pending_job_ids:
        await dispatch_job.kicker().with_broker(deps.broker).kiq(str(job_id))
        summary["jobs_dispatched"] += 1
    for job_id in processing_job_ids:
        async with deps.session_factory() as session:
            await recompute_job(session, job_id)
        summary["jobs_recomputed"] += 1

    return summary
