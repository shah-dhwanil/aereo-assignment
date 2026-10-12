"""Persistence for jobs: reads plus CAS status transitions."""

from __future__ import annotations

from collections.abc import Sequence
from uuid import UUID

from sqlalchemy import case, func, literal, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from aereo_assignment.db.models import Job, JobStatus

TERMINAL_JOB_STATUSES = (
    JobStatus.COMPLETED,
    JobStatus.COMPLETED_WITH_ERRORS,
    JobStatus.FAILED,
    JobStatus.CANCELLED,
)


def stale_cutoff(grace_seconds: int):
    """DB-clock expression for ``updated_at`` staleness comparisons."""
    # Positional make_interval(years, months, weeks, days, hours, mins, secs):
    # SQLAlchemy 2.x ``func`` accepts no keyword arguments, so the brief's
    # ``make_interval(secs=:grace)`` sketch cannot be written literally.
    return func.now() - func.make_interval(0, 0, 0, 0, 0, 0, grace_seconds)


class JobsRepository:
    """Reads and compare-and-swap writes for :class:`Job` rows.

    Every mutating method returns the affected rowcount; 0 means the CAS
    predicate missed (concurrent transition won) and the caller must no-op.
    All staleness comparisons use the database clock, never app time.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert(self, job: Job) -> Job:
        """Add + flush; IntegrityError propagates to the caller."""
        self._session.add(job)
        await self._session.flush()
        return job

    async def get(self, job_id: UUID) -> Job | None:
        return await self._session.get(Job, job_id)

    async def get_by_idempotency_key(self, key: str) -> Job | None:
        stmt = select(Job).where(Job.idempotency_key == key)
        return (await self._session.execute(stmt)).scalars().one_or_none()

    async def list(
        self, *, status: JobStatus | None, offset: int, limit: int
    ) -> Sequence[Job]:
        stmt = select(Job).order_by(Job.created_at, Job.id)
        if status is not None:
            stmt = stmt.where(Job.status == status)
        stmt = stmt.offset(offset).limit(limit)
        return (await self._session.execute(stmt)).scalars().all()

    async def count(self, *, status: JobStatus | None) -> int:
        stmt = select(func.count()).select_from(Job)
        if status is not None:
            stmt = stmt.where(Job.status == status)
        return (await self._session.execute(stmt)).scalar_one()

    async def cas_transition(
        self,
        job_id: UUID,
        *,
        from_statuses: Sequence[JobStatus],
        to_status: JobStatus,
        set_completed_at: bool,
    ) -> int:
        if not from_statuses:
            return 0
        values: dict = {"status": to_status, "updated_at": func.now()}
        if set_completed_at:
            values["completed_at"] = func.now()
        stmt = (
            update(Job)
            .where(Job.id == job_id, Job.status.in_(from_statuses))
            .values(**values)
        )
        result = await self._session.execute(stmt)
        return result.rowcount

    async def recompute_cas(self, job_id: UUID, decision: JobStatus) -> int:
        """Apply a recompute decision; dropped when the job is terminal.

        Spec §10.2 SQL: the terminal guard lives in the ``WHERE`` clause so a
        racing cancel can never be un-done; ``completed_at`` is set exactly
        once via ``COALESCE``.
        """
        stmt = (
            update(Job)
            .where(Job.id == job_id, Job.status.not_in(TERMINAL_JOB_STATUSES))
            .values(
                status=decision,
                completed_at=func.coalesce(
                    Job.completed_at,
                    case(
                        (
                            literal(decision).in_(TERMINAL_JOB_STATUSES),
                            func.now(),
                        ),
                        else_=None,
                    ),
                ),
                updated_at=func.now(),
            )
        )
        result = await self._session.execute(stmt)
        return result.rowcount

    async def find_stale(
        self, *, statuses: Sequence[JobStatus], grace_seconds: int
    ) -> Sequence[Job]:
        if not statuses:
            return []
        stmt = select(Job).where(
            Job.status.in_(statuses),
            Job.updated_at < stale_cutoff(grace_seconds),
        )
        return (await self._session.execute(stmt)).scalars().all()
