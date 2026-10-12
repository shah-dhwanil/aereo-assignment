"""Persistence for certificates: reads plus claim/outcome CAS transitions."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from aereo_assignment.db.models import Certificate, CertificateStatus
from aereo_assignment.repositories.jobs import stale_cutoff

OPEN_STATUSES = (CertificateStatus.PENDING, CertificateStatus.RETRYING)
STALE_IN_FLIGHT_STATUSES = (
    CertificateStatus.PROCESSING,
    CertificateStatus.RETRYING,
)


class CertificatesRepository:
    """Reads and compare-and-swap writes for :class:`Certificate` rows.

    Mutating methods return the affected rowcount (0 = CAS missed, no-op),
    except :meth:`claim` which returns the claimed row (``None`` on miss).
    Staleness always uses the database clock.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_many(self, certs: Sequence[Certificate]) -> None:
        self._session.add_all(certs)
        await self._session.flush()

    async def get(self, cert_id: UUID) -> Certificate | None:
        return await self._session.get(Certificate, cert_id)

    async def get_by_number(self, number: str) -> Certificate | None:
        stmt = select(Certificate).where(Certificate.certificate_number == number)
        return (await self._session.execute(stmt)).scalars().one_or_none()

    async def list_for_job(
        self,
        job_id: UUID,
        *,
        status: CertificateStatus | None,
        offset: int,
        limit: int,
    ) -> Sequence[Certificate]:
        stmt = (
            select(Certificate)
            .where(Certificate.job_id == job_id)
            .order_by(Certificate.created_at, Certificate.id)
        )
        if status is not None:
            stmt = stmt.where(Certificate.status == status)
        stmt = stmt.offset(offset).limit(limit)
        return (await self._session.execute(stmt)).scalars().all()

    async def status_counts(
        self, job_ids: Sequence[UUID]
    ) -> dict[UUID, dict[CertificateStatus, int]]:
        job_ids = list(job_ids)
        if not job_ids:
            return {}
        stmt = (
            select(Certificate.job_id, Certificate.status, func.count())
            .where(Certificate.job_id.in_(job_ids))
            .group_by(Certificate.job_id, Certificate.status)
        )
        out: dict[UUID, dict[CertificateStatus, int]] = {}
        for job_id, status, n in (await self._session.execute(stmt)).all():
            out.setdefault(job_id, {})[status] = n
        return out

    async def pending_ids(self, job_id: UUID) -> Sequence[UUID]:
        stmt = (
            select(Certificate.id)
            .where(
                Certificate.job_id == job_id,
                Certificate.status == CertificateStatus.PENDING,
            )
            .order_by(Certificate.id)
        )
        return (await self._session.execute(stmt)).scalars().all()

    async def claim(
        self, cert_id: UUID, *, max_attempts: int, token: UUID
    ) -> Certificate | None:
        stmt = (
            update(Certificate)
            .where(
                Certificate.id == cert_id,
                Certificate.status.in_(OPEN_STATUSES),
                Certificate.attempts < max_attempts,
            )
            .values(
                status=CertificateStatus.PROCESSING,
                claim_token=token,
                attempts=Certificate.attempts + 1,
                updated_at=func.now(),
            )
            .returning(Certificate)
        )
        result = await self._session.execute(stmt)
        return result.scalars().first()

    async def cas_outcome(
        self,
        cert_id: UUID,
        *,
        token: UUID,
        to_status: CertificateStatus,
        error_message: str | None = None,
        file_path: str | None = None,
        generated_at: datetime | None = None,
    ) -> int:
        # Set outcome columns unconditionally: None clears. A cert that went
        # PROCESSING -> RETRYING(error="boom") -> PROCESSING -> DONE must not
        # leave the stale error on the DONE row (user-visible wrong data).
        values: dict = {
            "status": to_status,
            "error_message": error_message,
            "file_path": file_path,
            "generated_at": generated_at,
            "updated_at": func.now(),
        }
        stmt = (
            update(Certificate)
            .where(
                Certificate.id == cert_id,
                Certificate.status == CertificateStatus.PROCESSING,
                Certificate.claim_token == token,
            )
            .values(**values)
        )
        result = await self._session.execute(stmt)
        return result.rowcount

    async def cancel_open(self, job_id: UUID) -> int:
        stmt = (
            update(Certificate)
            .where(
                Certificate.job_id == job_id,
                Certificate.status.in_(OPEN_STATUSES),
            )
            .values(status=CertificateStatus.CANCELLED, updated_at=func.now())
        )
        result = await self._session.execute(stmt)
        return result.rowcount

    async def find_stale(
        self, *, statuses: Sequence[CertificateStatus], grace_seconds: int
    ) -> Sequence[Certificate]:
        if not statuses:
            return []
        stmt = select(Certificate).where(
            Certificate.status.in_(statuses),
            Certificate.updated_at < stale_cutoff(grace_seconds),
        )
        return (await self._session.execute(stmt)).scalars().all()

    async def reset_to_pending(self, ids: Sequence[UUID]) -> int:
        ids = list(ids)
        if not ids:
            return 0
        stmt = (
            update(Certificate)
            .where(Certificate.id.in_(ids))
            .values(
                status=CertificateStatus.PENDING,
                claim_token=None,
                updated_at=func.now(),
            )
        )
        result = await self._session.execute(stmt)
        return result.rowcount

    async def force_fail_stale(
        self, cert_id: UUID, message: str, *, max_attempts: int
    ) -> int:
        """Sweep an at-cap in-flight cert to FAILED (H3 crash-at-cap path)."""
        stmt = (
            update(Certificate)
            .where(
                Certificate.id == cert_id,
                Certificate.status.in_(STALE_IN_FLIGHT_STATUSES),
                Certificate.attempts >= max_attempts,
            )
            .values(
                status=CertificateStatus.FAILED,
                error_message=message,
                updated_at=func.now(),
            )
        )
        result = await self._session.execute(stmt)
        return result.rowcount

    async def find_stale_pending(self, *, grace_seconds: int) -> Sequence[UUID]:
        """IDs of never-claimed PENDING certs older than the grace window."""
        stmt = select(Certificate.id).where(
            Certificate.status == CertificateStatus.PENDING,
            Certificate.updated_at < stale_cutoff(grace_seconds),
        )
        return (await self._session.execute(stmt)).scalars().all()
