"""Fan-out task: enqueue one ``generate_pdf`` message per PENDING certificate."""

from __future__ import annotations

from uuid import UUID

from taskiq import TaskiqDepends

from aereo_assignment.db.models import JobStatus
from aereo_assignment.repositories.certificates import CertificatesRepository
from aereo_assignment.repositories.jobs import JobsRepository
from aereo_assignment.worker.broker import broker
from aereo_assignment.worker.deps import DispatchDeps, get_dispatch_deps

try:
    from aereo_assignment.worker.tasks.generate import generate_pdf
except ImportError:  # Task 8 provides the generate task; tests patch this name.
    generate_pdf = None  # type: ignore[assignment]


# NOTE: installed taskiq 0.13 ``broker.task`` takes the name positionally
# (``task_name``); ``name=`` would silently become a label and the task would
# register as ``module:func``. ``retry_on_error``/``max_retries`` are labels
# consumed by SmartRetryMiddleware.
@broker.task("dispatch_job", retry_on_error=True, max_retries=5)
async def dispatch_job(
    job_id: str, deps: DispatchDeps = TaskiqDepends(get_dispatch_deps)  # noqa: B008 - taskiq DI idiom mandated by spec
) -> dict[str, int]:
    """Taskiq wrapper: payloads are strings; cores parse ``UUID(...)``."""
    return await dispatch_job_core(UUID(job_id), deps)


async def dispatch_job_core(job_id: UUID, deps: DispatchDeps) -> dict[str, int]:
    """CAS the job PENDING->PROCESSING, then kick ``generate_pdf`` per id.

    The CAS rowcount is IGNORED: fan-out runs even on a miss (X1), and the
    per-certificate claim CAS in Task 9 absorbs duplicate kicks.
    """
    async with deps.session_factory() as session:
        await JobsRepository(session).cas_transition(
            job_id,
            from_statuses=[JobStatus.PENDING],
            to_status=JobStatus.PROCESSING,
            set_completed_at=False,
        )
        ids = list(await CertificatesRepository(session).pending_ids(job_id))
        await session.commit()
    if generate_pdf is None:  # pragma: no cover - wired once Task 8 lands
        raise RuntimeError("generate_pdf task is not available")
    for cert_id in ids:
        await generate_pdf.kicker().with_broker(deps.broker).kiq(str(cert_id))
    return {"enqueued": len(ids)}
