"""Integration tests for periodic + startup reconcile (Task 11).

TDD Step 1: these fail before implementation (no
``aereo_assignment.worker.tasks.reconcile`` module yet). Real PG via the
session-scoped container ``session_factory`` fixture; ``InMemoryBroker`` with
``await_inplace=True`` so kiq'd generate/dispatch tasks execute inline; deps
built directly with module-state monkeypatching for TaskiqDepends resolution
(T9 pattern: register task originals on the fresh broker).

Row scoping (T10 lesson): the container DB accumulates rows across the
session, so every assertion is scoped to own seeded IDs, and the reconcile
grace is set wide (1800s) while own stale rows are aged 3600s with the DB
clock — other tests' fresh rows never match, regardless of suite timing.
"""

from __future__ import annotations

import sys
import uuid
from datetime import UTC, date, datetime
from uuid import UUID

from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from taskiq import InMemoryBroker

from aereo_assignment.config import Settings
from aereo_assignment.db.models import (
    Certificate,
    CertificateStatus,
    Job,
    JobStatus,
)
from aereo_assignment.repositories.certificates import CertificatesRepository
from aereo_assignment.repositories.jobs import JobsRepository
from aereo_assignment.services.storage import DiskStorage
from aereo_assignment.worker import deps as deps_module
from aereo_assignment.worker.deps import ReconcileDeps
from aereo_assignment.worker.tasks.dispatch import dispatch_job
from aereo_assignment.worker.tasks.generate import generate_pdf
from aereo_assignment.worker.tasks.reconcile import reconcile, run_reconcile

reconcile_module = sys.modules["aereo_assignment.worker.tasks.reconcile"]

GRACE = 1800
AGED = 3600
MAX_ATTEMPTS = 4

ZERO_SUMMARY = {
    "stale_certs_reset": 0,
    "stale_certs_failed": 0,
    "certs_reenqueued": 0,
    "jobs_dispatched": 0,
    "jobs_recomputed": 0,
}


def _test_broker() -> InMemoryBroker:
    """Fresh broker with generate/dispatch registered for inline execution."""
    broker = InMemoryBroker(await_inplace=True)
    # T9 lesson: fresh brokers need explicit task registration.
    broker.register_task(
        generate_pdf.original_func,
        task_name=generate_pdf.task_name,
        **generate_pdf.labels,
    )
    broker.register_task(
        dispatch_job.original_func,
        task_name=dispatch_job.task_name,
        **dispatch_job.labels,
    )
    return broker


def _wire(
    monkeypatch,
    session_factory: async_sessionmaker[AsyncSession],
    tmp_path,
    broker: InMemoryBroker,
) -> ReconcileDeps:
    settings = Settings(
        database_url="postgresql+asyncpg://localhost:5432/test",
        redis_url="redis://localhost:6379/0",
        storage_dir=tmp_path / "storage",
        base_url="http://test",
        reconcile_grace_seconds=GRACE,
    )
    storage = DiskStorage(settings.storage_dir)
    monkeypatch.setattr(deps_module, "_session_factory", session_factory)
    monkeypatch.setattr(deps_module, "_settings", settings)
    monkeypatch.setattr(deps_module, "_storage", storage)
    monkeypatch.setattr(deps_module, "_broker", broker)
    return ReconcileDeps(
        session_factory=session_factory, broker=broker, settings=settings
    )


def _job(tag: str, **kwargs) -> Job:
    kwargs.setdefault("event_name", f"Reconcile {tag} Event")
    kwargs.setdefault("event_date", date(2026, 10, 1))
    kwargs.setdefault("issuer", "Aereo")
    kwargs.setdefault("idempotency_key", f"reconcile-{tag}-{uuid.uuid4().hex}")
    kwargs.setdefault("status", JobStatus.PROCESSING)
    return Job(**kwargs)


def _cert(job_id: UUID, tag: str, **kwargs) -> Certificate:
    kwargs.setdefault("recipient_name", "Ada Lovelace")
    kwargs.setdefault("recipient_email", "ada@example.com")
    kwargs.setdefault(
        "certificate_number", f"REC-{tag}-{uuid.uuid4().hex[:12].upper()}"
    )
    return Certificate(job_id=job_id, **kwargs)


async def _age_certs(
    session_factory: async_sessionmaker[AsyncSession], ids: list[UUID]
) -> None:
    async with session_factory() as s:
        await s.execute(
            update(Certificate)
            .where(Certificate.id.in_(ids))
            .values(updated_at=func.now() - func.make_interval(0, 0, 0, 0, 0, 0, AGED))
        )
        await s.commit()


async def _age_jobs(
    session_factory: async_sessionmaker[AsyncSession], ids: list[UUID]
) -> None:
    from aereo_assignment.db.models import Job as JobModel

    async with session_factory() as s:
        await s.execute(
            update(JobModel)
            .where(JobModel.id.in_(ids))
            .values(updated_at=func.now() - func.make_interval(0, 0, 0, 0, 0, 0, AGED))
        )
        await s.commit()


async def test_resets_stale_processing_cert_and_regenerates(
    session_factory, tmp_path, monkeypatch
) -> None:
    broker = _test_broker()
    deps = _wire(monkeypatch, session_factory, tmp_path, broker)
    async with session_factory() as s:
        job = await JobsRepository(s).insert(_job("reset"))
        await s.flush()
        cert = _cert(
            job.id,
            "reset",
            status=CertificateStatus.PROCESSING,
            attempts=1,
            claim_token=uuid.uuid4(),  # orphan token: crashed owner
        )
        await CertificatesRepository(s).insert_many([cert])
        await s.flush()
        job_id, cert_id = job.id, cert.id
        await s.commit()
    await _age_certs(session_factory, [cert_id])

    summary = await run_reconcile(deps)

    assert summary == ZERO_SUMMARY | {
        "stale_certs_reset": 1,
        "certs_reenqueued": 1,
    }
    async with session_factory() as s:
        row = await CertificatesRepository(s).get(cert_id)
        assert row is not None
        assert row.status == CertificateStatus.DONE
        assert row.file_path is not None
        assert row.generated_at is not None
        storage = DiskStorage(deps.settings.storage_dir)
        assert (await storage.read(row.file_path)).startswith(b"%PDF")
        job = await JobsRepository(s).get(job_id)
        assert job is not None
        assert job.status == JobStatus.COMPLETED


async def test_exhausted_stale_cert_swept_to_failed(
    session_factory, tmp_path, monkeypatch
) -> None:
    broker = _test_broker()
    deps = _wire(monkeypatch, session_factory, tmp_path, broker)
    async with session_factory() as s:
        job = await JobsRepository(s).insert(_job("exhaust"))
        await s.flush()
        sibling = _cert(job.id, "exhaust-done", status=CertificateStatus.DONE)
        doomed = _cert(
            job.id,
            "exhaust-doomed",
            status=CertificateStatus.PROCESSING,
            attempts=MAX_ATTEMPTS,
            claim_token=uuid.uuid4(),
        )
        await CertificatesRepository(s).insert_many([sibling, doomed])
        await s.flush()
        job_id, doomed_id, sibling_id = job.id, doomed.id, sibling.id
        await s.commit()
    await _age_certs(session_factory, [doomed_id])

    summary = await run_reconcile(deps)

    assert summary == ZERO_SUMMARY | {
        "stale_certs_failed": 1,
        "jobs_recomputed": 1,
    }
    async with session_factory() as s:
        row = await CertificatesRepository(s).get(doomed_id)
        assert row is not None
        assert row.status == CertificateStatus.FAILED
        assert row.error_message is not None and "exhausted" in row.error_message
        sibling_row = await CertificatesRepository(s).get(sibling_id)
        assert sibling_row is not None
        assert sibling_row.status == CertificateStatus.DONE
        job = await JobsRepository(s).get(job_id)
        assert job is not None
        assert job.status == JobStatus.COMPLETED_WITH_ERRORS


async def test_stale_pending_job_gets_dispatch(
    session_factory, tmp_path, monkeypatch
) -> None:
    broker = _test_broker()
    deps = _wire(monkeypatch, session_factory, tmp_path, broker)
    async with session_factory() as s:
        job = await JobsRepository(s).insert(_job("dispatch", status=JobStatus.PENDING))
        await s.flush()
        # Certs are fresh (never-claimed, inside the grace window): only the
        # stale PENDING job may fire, via dispatch fan-out.
        certs = [_cert(job.id, "dispatch") for _ in range(2)]
        await CertificatesRepository(s).insert_many(certs)
        await s.flush()
        job_id, cert_ids = job.id, [c.id for c in certs]
        await s.commit()
    await _age_jobs(session_factory, [job_id])

    summary = await run_reconcile(deps)

    assert summary == ZERO_SUMMARY | {"jobs_dispatched": 1}
    async with session_factory() as s:
        for cert_id in cert_ids:
            row = await CertificatesRepository(s).get(cert_id)
            assert row is not None
            assert row.status == CertificateStatus.DONE
        job = await JobsRepository(s).get(job_id)
        assert job is not None
        assert job.status == JobStatus.COMPLETED


async def test_stale_processing_job_recomputed_after_crash_window(
    session_factory, tmp_path, monkeypatch
) -> None:
    broker = _test_broker()
    deps = _wire(monkeypatch, session_factory, tmp_path, broker)
    async with session_factory() as s:
        job = await JobsRepository(s).insert(_job("crash"))
        await s.flush()
        certs = [
            _cert(
                job.id,
                "crash",
                status=CertificateStatus.DONE,
                file_path=f"jobs/{job.id}/old.pdf",
                generated_at=datetime.now(UTC),
            )
            for _ in range(2)
        ]
        await CertificatesRepository(s).insert_many(certs)
        await s.flush()
        job_id, cert_ids = job.id, [c.id for c in certs]
        before = {c.id: (c.status, c.file_path, c.updated_at) for c in certs}
        await s.commit()
    await _age_jobs(session_factory, [job_id])

    summary = await run_reconcile(deps)

    assert summary == ZERO_SUMMARY | {"jobs_recomputed": 1}
    async with session_factory() as s:
        job = await JobsRepository(s).get(job_id)
        assert job is not None
        assert job.status == JobStatus.COMPLETED
        for cert_id in cert_ids:
            row = await CertificatesRepository(s).get(cert_id)
            assert row is not None
            status, file_path, updated_at = before[cert_id]
            assert row.status == status == CertificateStatus.DONE
            assert row.file_path == file_path
            assert row.updated_at == updated_at


async def test_fresh_rows_untouched(session_factory, tmp_path, monkeypatch) -> None:
    broker = _test_broker()
    deps = _wire(monkeypatch, session_factory, tmp_path, broker)
    async with session_factory() as s:
        pending_job = await JobsRepository(s).insert(
            _job("fresh-pending", status=JobStatus.PENDING)
        )
        await s.flush()
        pending_cert = _cert(pending_job.id, "fresh-pending")
        await CertificatesRepository(s).insert_many([pending_cert])
        await s.flush()
        proc_job = await JobsRepository(s).insert(_job("fresh-proc"))
        await s.flush()
        proc_cert = _cert(
            proc_job.id, "fresh-proc", status=CertificateStatus.PROCESSING
        )
        await CertificatesRepository(s).insert_many([proc_cert])
        await s.flush()
        ids = {
            "pending_job": pending_job.id,
            "pending_cert": pending_cert.id,
            "proc_job": proc_job.id,
            "proc_cert": proc_cert.id,
        }
        await s.commit()

    summary = await run_reconcile(deps)

    assert summary == ZERO_SUMMARY
    async with session_factory() as s:
        assert (await JobsRepository(s).get(ids["pending_job"])).status == (
            JobStatus.PENDING
        )
        assert (
            await CertificatesRepository(s).get(ids["pending_cert"])
        ).status == CertificateStatus.PENDING
        assert (await JobsRepository(s).get(ids["proc_job"])).status == (
            JobStatus.PROCESSING
        )
        assert (
            await CertificatesRepository(s).get(ids["proc_cert"])
        ).status == CertificateStatus.PROCESSING


async def test_startup_hook_registered(monkeypatch) -> None:
    from taskiq.events import TaskiqEvents

    from aereo_assignment.worker import broker as broker_module

    calls: list = []
    sentinel = object()

    async def _fake_run(passed_deps):
        calls.append(passed_deps)
        return dict(ZERO_SUMMARY)

    monkeypatch.setattr(reconcile_module, "run_reconcile", _fake_run)
    monkeypatch.setattr(deps_module, "get_reconcile_deps", lambda: sentinel)

    assert (
        broker_module.reconcile_on_startup
        in broker_module.broker.event_handlers[TaskiqEvents.WORKER_STARTUP]
    )
    assert (
        broker_module.reconcile_on_startup
        in broker_module.broker.event_handlers[TaskiqEvents.CLIENT_STARTUP]
    )
    await broker_module.reconcile_on_startup(object())
    assert calls == [sentinel]


async def test_reconcile_task_carries_interval_schedule_label() -> None:
    from taskiq.schedule_sources import LabelScheduleSource

    from aereo_assignment.worker import broker as broker_module

    assert reconcile.task_name == "reconcile"
    assert reconcile.labels.get("schedule") == [
        {"interval": broker_module.settings.reconcile_interval_seconds}
    ]
    source = LabelScheduleSource(broker_module.broker)
    await source.startup()
    found = [s for s in await source.get_schedules() if s.task_name == "reconcile"]
    assert len(found) == 1
    assert found[0].interval == broker_module.settings.reconcile_interval_seconds
