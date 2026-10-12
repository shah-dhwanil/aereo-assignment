"""Unit tests for ``dispatch_job_core`` (Task 7).

DB-backed via the session-scoped container ``session_factory`` fixture: the
core under test runs the real ``JobsRepository``/``CertificatesRepository``
while ``generate_pdf`` (Task 8, not yet implemented) is replaced by a recorder
fake and the broker is a :class:`RecordingBroker` (no Redis required).
"""

from __future__ import annotations

import uuid
from datetime import date
from uuid import UUID

from conftest import RecordingBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from aereo_assignment.db.models import (
    Certificate,
    CertificateStatus,
    Job,
    JobStatus,
)
from aereo_assignment.repositories.certificates import CertificatesRepository
from aereo_assignment.repositories.jobs import JobsRepository
from aereo_assignment.worker import deps as deps_module
from aereo_assignment.worker.deps import DispatchDeps, get_dispatch_deps
from aereo_assignment.worker.tasks import dispatch as dispatch_module
from aereo_assignment.worker.tasks.dispatch import dispatch_job, dispatch_job_core


class _FakeGenerateTask:
    """Recorder stand-in for the Task 8 ``generate_pdf`` task."""

    def __init__(self) -> None:
        self.kicked: list[str] = []
        self.brokers: list = []

    def kicker(self) -> _FakeGenerateTask:
        return self

    def with_broker(self, broker) -> _FakeGenerateTask:
        self.brokers.append(broker)
        return self

    async def kiq(self, cert_id: str, *args, **kwargs):
        self.kicked.append(cert_id)


def _job(**kwargs) -> Job:
    kwargs.setdefault("event_name", "Dispatch Test Event")
    kwargs.setdefault("event_date", date(2026, 10, 1))
    kwargs.setdefault("issuer", "Aereo")
    kwargs.setdefault("idempotency_key", f"dispatch-{uuid.uuid4().hex}")
    return Job(**kwargs)


def _cert(job_id: UUID, **kwargs) -> Certificate:
    kwargs.setdefault("recipient_name", "Ada Lovelace")
    kwargs.setdefault("recipient_email", "ada@example.com")
    kwargs.setdefault(
        "certificate_number", f"DSP-2026-{uuid.uuid4().hex[:12].upper()}"
    )
    return Certificate(job_id=job_id, **kwargs)


async def _seed(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    job_status: JobStatus = JobStatus.PENDING,
    cert_statuses: tuple[CertificateStatus, ...] = (
        CertificateStatus.PENDING,
        CertificateStatus.PENDING,
        CertificateStatus.FAILED,
    ),
) -> tuple[UUID, list[UUID]]:
    async with session_factory() as s:
        job = await JobsRepository(s).insert(_job(status=job_status))
        await s.flush()
        certs = [_cert(job.id, status=st) for st in cert_statuses]
        await CertificatesRepository(s).insert_many(certs)
        await s.flush()
        ids = [c.id for c in certs]
        await s.commit()
        return job.id, ids


def _deps(
    session_factory: async_sessionmaker[AsyncSession], monkeypatch
) -> tuple[DispatchDeps, _FakeGenerateTask]:
    """DispatchDeps over the container DB + recorder fake for generate_pdf."""
    fake = _FakeGenerateTask()
    monkeypatch.setattr(dispatch_module, "generate_pdf", fake)
    return DispatchDeps(session_factory=session_factory, broker=RecordingBroker()), fake


async def test_dispatch_enqueues_only_pending_certificates(
    session_factory, monkeypatch
) -> None:
    job_id, cert_ids = await _seed(session_factory)
    deps, fake = _deps(session_factory, monkeypatch)

    result = await dispatch_job_core(job_id, deps)

    assert result == {"enqueued": 2}
    assert sorted(fake.kicked) == sorted([str(cert_ids[0]), str(cert_ids[1])])
    assert all(isinstance(payload, str) for payload in fake.kicked)
    assert fake.brokers and all(b is deps.broker for b in fake.brokers)
    async with session_factory() as s:
        job = await JobsRepository(s).get(job_id)
        assert job is not None
        assert job.status == JobStatus.PROCESSING


async def test_dispatch_survives_job_cas_miss(session_factory, monkeypatch) -> None:
    job_id, cert_ids = await _seed(
        session_factory,
        job_status=JobStatus.PROCESSING,
        cert_statuses=(CertificateStatus.PENDING,),
    )
    deps, fake = _deps(session_factory, monkeypatch)

    result = await dispatch_job_core(job_id, deps)  # must not raise

    assert result == {"enqueued": 1}
    assert fake.kicked == [str(cert_ids[0])]


async def test_dispatch_rerun_is_idempotent(session_factory, monkeypatch) -> None:
    job_id, cert_ids = await _seed(
        session_factory,
        cert_statuses=(
            CertificateStatus.PENDING,
            CertificateStatus.PENDING,
        ),
    )
    deps, fake = _deps(session_factory, monkeypatch)

    first = await dispatch_job_core(job_id, deps)
    second = await dispatch_job_core(job_id, deps)

    assert first == {"enqueued": 2}
    assert second == {"enqueued": 2}  # re-enqueued; claim CAS absorbs (Task 9)
    assert len(fake.kicked) == 4
    assert sorted(fake.kicked) == sorted([str(c) for c in cert_ids] * 2)


async def test_dispatch_task_registers_with_retry_labels() -> None:
    assert dispatch_job.task_name == "dispatch_job"
    assert dispatch_job.labels.get("retry_on_error") is True
    assert int(dispatch_job.labels.get("max_retries")) == 5


async def test_get_dispatch_deps_reads_module_state(
    session_factory, monkeypatch
) -> None:
    broker = RecordingBroker()
    monkeypatch.setattr(deps_module, "_session_factory", session_factory)
    monkeypatch.setattr(deps_module, "_broker", broker)

    deps = get_dispatch_deps()

    assert deps.session_factory is session_factory
    assert deps.broker is broker
