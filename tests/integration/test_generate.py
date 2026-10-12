"""Integration tests for ``generate_pdf`` claim/render/store/retry taxonomy.

TDD Step 1: these fail before implementation (no
``aereo_assignment.worker.tasks.generate`` module yet). Real PG via the
session-scoped container ``session_factory`` fixture, ``DiskStorage`` over
``tmp_path``, deps built directly — no broker needed except the wiring test.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, date, datetime
from uuid import UUID

import pytest
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
from aereo_assignment.worker.deps import GenerateDeps
from aereo_assignment.worker.tasks import generate as generate_module
from aereo_assignment.worker.tasks.generate import generate_pdf, generate_pdf_core

MAX_ATTEMPTS = 4


def _settings(tmp_path, **overrides) -> Settings:
    kwargs: dict = {
        "database_url": "postgresql+asyncpg://localhost:5432/test",
        "redis_url": "redis://localhost:6379/0",
        "storage_dir": tmp_path / "storage",
        "base_url": "http://test",
    }
    kwargs.update(overrides)
    return Settings(**kwargs)


def _deps(
    session_factory: async_sessionmaker[AsyncSession], tmp_path, **overrides
) -> GenerateDeps:
    settings = _settings(tmp_path, **overrides)
    return GenerateDeps(
        session_factory=session_factory,
        storage=DiskStorage(settings.storage_dir),
        settings=settings,
        broker=InMemoryBroker(),
    )


def _job(**kwargs) -> Job:
    kwargs.setdefault("event_name", "Generate Test Event")
    kwargs.setdefault("event_date", date(2026, 10, 1))
    kwargs.setdefault("issuer", "Aereo")
    kwargs.setdefault("idempotency_key", f"generate-{uuid.uuid4().hex}")
    kwargs.setdefault("status", JobStatus.PROCESSING)
    return Job(**kwargs)


def _cert(job_id: UUID, **kwargs) -> Certificate:
    kwargs.setdefault("recipient_name", "Ada Lovelace")
    kwargs.setdefault("recipient_email", "ada@example.com")
    kwargs.setdefault(
        "certificate_number", f"GEN-2026-{uuid.uuid4().hex[:12].upper()}"
    )
    return Certificate(job_id=job_id, **kwargs)


async def _seed(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    job_status: JobStatus = JobStatus.PROCESSING,
    n_certs: int = 1,
    **cert_kwargs,
) -> tuple[UUID, list[UUID]]:
    async with session_factory() as s:
        job = await JobsRepository(s).insert(_job(status=job_status))
        await s.flush()
        certs = [_cert(job.id, **cert_kwargs) for _ in range(n_certs)]
        await CertificatesRepository(s).insert_many(certs)
        await s.flush()
        ids = [c.id for c in certs]
        await s.commit()
        return job.id, ids


async def test_generate_happy_path_marks_done_and_writes_file(
    session_factory, tmp_path
) -> None:
    job_id, (cert_id,) = await _seed(session_factory)
    deps = _deps(session_factory, tmp_path)

    assert await generate_pdf_core(cert_id, deps) == "done"

    async with session_factory() as s:
        row = await CertificatesRepository(s).get(cert_id)
        assert row is not None
        assert row.status == CertificateStatus.DONE
        assert row.file_path is not None
        assert row.generated_at is not None
        assert row.error_message is None
        data = await deps.storage.read(row.file_path)
        assert data.startswith(b"%PDF")
        job = await JobsRepository(s).get(job_id)
        assert job is not None
        assert job.status == JobStatus.COMPLETED


async def test_permanent_error_fails_fast(
    session_factory, tmp_path, monkeypatch
) -> None:
    job_id, (cert_id,) = await _seed(session_factory)
    deps = _deps(session_factory, tmp_path)

    def _boom(**kwargs) -> bytes:
        raise ValueError("bad row")

    monkeypatch.setattr(generate_module, "render_certificate", _boom)

    assert await generate_pdf_core(cert_id, deps) == "failed:permanent"

    async with session_factory() as s:
        row = await CertificatesRepository(s).get(cert_id)
        assert row is not None
        assert row.status == CertificateStatus.FAILED
        assert row.error_message is not None and "bad row" in row.error_message
        assert row.attempts == 1
        job = await JobsRepository(s).get(job_id)
        assert job is not None
        assert job.status == JobStatus.FAILED

    # Re-invocation is absorbed: FAILED is not claimable.
    assert await generate_pdf_core(cert_id, deps) == "skipped:unclaimed"


async def test_transient_error_moves_to_retrying_and_redelivers(
    session_factory, tmp_path, monkeypatch
) -> None:
    _, (cert_id,) = await _seed(session_factory)
    deps = _deps(session_factory, tmp_path)

    async def _boom_write(key: str, data: bytes) -> None:
        raise OSError("disk down")

    monkeypatch.setattr(deps.storage, "write", _boom_write)

    with pytest.raises(OSError, match="disk down"):
        await generate_pdf_core(cert_id, deps)
    async with session_factory() as s:
        row = await CertificatesRepository(s).get(cert_id)
        assert row is not None
        assert row.status == CertificateStatus.RETRYING
        assert row.attempts == 1
        assert row.error_message is not None and "disk down" in row.error_message

    with pytest.raises(OSError, match="disk down"):
        await generate_pdf_core(cert_id, deps)
    async with session_factory() as s:
        row = await CertificatesRepository(s).get(cert_id)
        assert row is not None
        assert row.status == CertificateStatus.RETRYING
        assert row.attempts == 2


async def test_stale_owner_transient_returns_skipped(
    session_factory, tmp_path, monkeypatch
) -> None:
    _, (cert_id,) = await _seed(session_factory)
    deps = _deps(session_factory, tmp_path)

    async def _steal_then_boom(key: str, data: bytes) -> None:
        # Lose ownership mid-flight: a rival worker resets and re-claims
        # before our cas_outcome(RETRYING) lands, then the store fails.
        async with session_factory() as s:
            repos = CertificatesRepository(s)
            assert await repos.reset_to_pending([cert_id]) == 1
            assert (
                await repos.claim(
                    cert_id, max_attempts=MAX_ATTEMPTS, token=uuid.uuid4()
                )
                is not None
            )
            await s.commit()
        raise OSError("disk down")

    monkeypatch.setattr(deps.storage, "write", _steal_then_boom)

    assert await generate_pdf_core(cert_id, deps) == "skipped:stale"
    async with session_factory() as s:
        row = await CertificatesRepository(s).get(cert_id)
        assert row is not None
        assert row.status == CertificateStatus.PROCESSING
        assert row.attempts == 2
        assert row.error_message is None


async def test_attempts_cap_exhaustion_fails(
    session_factory, tmp_path, monkeypatch
) -> None:
    cap = 3
    _, (cert_id,) = await _seed(session_factory)
    deps = _deps(session_factory, tmp_path, max_generation_attempts=cap)

    async def _boom_write(key: str, data: bytes) -> None:
        raise OSError("flaky disk")

    monkeypatch.setattr(deps.storage, "write", _boom_write)

    outcomes: list[str] = []
    for _ in range(cap):
        try:
            outcomes.append(await generate_pdf_core(cert_id, deps))
        except OSError:
            outcomes.append("raised")

    assert outcomes == ["raised"] * (cap - 1) + ["failed:exhausted"]
    async with session_factory() as s:
        row = await CertificatesRepository(s).get(cert_id)
        assert row is not None
        assert row.status == CertificateStatus.FAILED
        assert row.attempts == cap


async def test_duplicate_delivery_is_absorbed(session_factory, tmp_path) -> None:
    _, (cert_id,) = await _seed(session_factory)
    deps = _deps(session_factory, tmp_path)

    results = await asyncio.gather(
        generate_pdf_core(cert_id, deps), generate_pdf_core(cert_id, deps)
    )

    assert sorted(results) == ["done", "skipped:unclaimed"]
    async with session_factory() as s:
        row = await CertificatesRepository(s).get(cert_id)
        assert row is not None
        assert row.status == CertificateStatus.DONE
        assert row.attempts == 1


async def test_split_brain_stale_owner_cannot_clobber(session_factory) -> None:
    _, (cert_id,) = await _seed(session_factory)
    t1, t2 = uuid.uuid4(), uuid.uuid4()
    async with session_factory() as s:
        claimed = await CertificatesRepository(s).claim(
            cert_id, max_attempts=MAX_ATTEMPTS, token=t1
        )
        assert claimed is not None
        await s.commit()
    async with session_factory() as s:
        assert await CertificatesRepository(s).reset_to_pending([cert_id]) == 1
        await s.commit()
    async with session_factory() as s:
        claimed = await CertificatesRepository(s).claim(
            cert_id, max_attempts=MAX_ATTEMPTS, token=t2
        )
        assert claimed is not None
        await s.commit()
    async with session_factory() as s:
        repos = CertificatesRepository(s)
        assert (
            await repos.cas_outcome(
                cert_id, token=t1, to_status=CertificateStatus.FAILED
            )
            == 0
        )
        assert (
            await repos.cas_outcome(
                cert_id,
                token=t2,
                to_status=CertificateStatus.DONE,
                file_path="jobs/x/cert.pdf",
                generated_at=datetime.now(UTC),
            )
            == 1
        )
        await s.commit()


async def test_inmemory_broker_wiring(session_factory, tmp_path, monkeypatch) -> None:
    _, (cert_id,) = await _seed(session_factory)
    settings = _settings(tmp_path)
    storage = DiskStorage(settings.storage_dir)
    broker = InMemoryBroker()
    # A fresh broker has an empty local registry (only AsyncSharedBroker
    # populates the global fallback), so register the task on the test broker
    # before kicking through it.
    broker.register_task(
        generate_pdf.original_func,
        task_name=generate_pdf.task_name,
        **generate_pdf.labels,
    )
    monkeypatch.setattr(deps_module, "_session_factory", session_factory)
    monkeypatch.setattr(deps_module, "_storage", storage)
    monkeypatch.setattr(deps_module, "_settings", settings)
    monkeypatch.setattr(deps_module, "_broker", broker)

    task = await generate_pdf.kicker().with_broker(broker).kiq(str(cert_id))
    result = await task.wait_result(timeout=30)

    assert result.return_value == "done"
    async with session_factory() as s:
        row = await CertificatesRepository(s).get(cert_id)
        assert row is not None
        assert row.status == CertificateStatus.DONE
