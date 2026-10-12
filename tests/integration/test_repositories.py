"""Integration tests for JobsRepository / CertificatesRepository (Task 6).

All tests run against the session-scoped throwaway Postgres container via the
``session_factory`` fixture. Committed rows persist for the whole session, so
every test uses unique certificate numbers / idempotency keys and scopes its
assertions to rows it created.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from uuid import UUID

import pytest
from sqlalchemy import func, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from aereo_assignment.db.models import (
    Certificate,
    CertificateStatus,
    Job,
    JobStatus,
)
from aereo_assignment.repositories.certificates import CertificatesRepository
from aereo_assignment.repositories.jobs import JobsRepository

MAX_ATTEMPTS = 4


def _job(**kwargs) -> Job:
    kwargs.setdefault("event_name", "Test Event")
    kwargs.setdefault("event_date", date(2026, 10, 1))
    kwargs.setdefault("issuer", "Aereo")
    kwargs.setdefault("idempotency_key", f"key-{uuid.uuid4().hex}")
    return Job(**kwargs)


def _cert(job_id: UUID, **kwargs) -> Certificate:
    kwargs.setdefault("recipient_name", "Ada Lovelace")
    kwargs.setdefault("recipient_email", "ada@example.com")
    kwargs.setdefault(
        "certificate_number", f"CERT-2026-{uuid.uuid4().hex[:12].upper()}"
    )
    return Certificate(job_id=job_id, **kwargs)


async def _make_job(
    session_factory: async_sessionmaker[AsyncSession], **kwargs
) -> UUID:
    async with session_factory() as s:
        job = await JobsRepository(s).insert(_job(**kwargs))
        await s.commit()
        return job.id


async def _make_cert(
    session_factory: async_sessionmaker[AsyncSession], job_id: UUID, **kwargs
) -> UUID:
    async with session_factory() as s:
        cert = _cert(job_id, **kwargs)
        await CertificatesRepository(s).insert_many([cert])
        await s.commit()
        return cert.id


# --- claim CAS ---


async def test_claim_wins_once(session_factory) -> None:
    job_id = await _make_job(session_factory)
    cert_id = await _make_cert(session_factory, job_id)
    t1, t2 = uuid.uuid4(), uuid.uuid4()
    async with session_factory() as s:
        first = await CertificatesRepository(s).claim(
            cert_id, max_attempts=MAX_ATTEMPTS, token=t1
        )
        assert first is not None
        assert first.attempts == 1
        assert first.claim_token == t1
        assert first.status == CertificateStatus.PROCESSING
        await s.commit()
    async with session_factory() as s:
        repos = CertificatesRepository(s)
        second = await repos.claim(cert_id, max_attempts=MAX_ATTEMPTS, token=t2)
        assert second is None
        row = await repos.get(cert_id)
        assert row is not None
        assert row.status == CertificateStatus.PROCESSING
        assert row.attempts == 1
        assert row.claim_token == t1


async def test_claim_respects_attempts_cap(session_factory) -> None:
    job_id = await _make_job(session_factory)
    cert_id = await _make_cert(session_factory, job_id, attempts=MAX_ATTEMPTS)
    async with session_factory() as s:
        got = await CertificatesRepository(s).claim(
            cert_id, max_attempts=MAX_ATTEMPTS, token=uuid.uuid4()
        )
        assert got is None
        row = await CertificatesRepository(s).get(cert_id)
        assert row is not None
        assert row.status == CertificateStatus.PENDING
        assert row.attempts == MAX_ATTEMPTS


async def test_claim_from_retrying(session_factory) -> None:
    job_id = await _make_job(session_factory)
    cert_id = await _make_cert(
        session_factory,
        job_id,
        status=CertificateStatus.RETRYING,
        attempts=1,
        error_message="boom",
    )
    token = uuid.uuid4()
    async with session_factory() as s:
        got = await CertificatesRepository(s).claim(
            cert_id, max_attempts=MAX_ATTEMPTS, token=token
        )
        assert got is not None
        assert got.status == CertificateStatus.PROCESSING
        assert got.attempts == 2
        assert got.claim_token == token
        await s.commit()


# --- cert outcome CAS ---


async def test_cas_outcome_requires_matching_token(session_factory) -> None:
    job_id = await _make_job(session_factory)
    cert_id = await _make_cert(session_factory, job_id)
    owner = uuid.uuid4()
    async with session_factory() as s:
        claimed = await CertificatesRepository(s).claim(
            cert_id, max_attempts=MAX_ATTEMPTS, token=owner
        )
        assert claimed is not None
        await s.commit()
    async with session_factory() as s:
        repos = CertificatesRepository(s)
        missed = await repos.cas_outcome(
            cert_id, token=uuid.uuid4(), to_status=CertificateStatus.DONE
        )
        assert missed == 0
        assert (await repos.get(cert_id)).status == CertificateStatus.PROCESSING  # type: ignore[union-attr]
        now = datetime.now(UTC)
        hit = await repos.cas_outcome(
            cert_id,
            token=owner,
            to_status=CertificateStatus.DONE,
            file_path="jobs/x/cert.pdf",
            generated_at=now,
        )
        assert hit == 1
        await s.commit()
    async with session_factory() as s:
        row = await CertificatesRepository(s).get(cert_id)
        assert row is not None
        assert row.status == CertificateStatus.DONE
        assert row.file_path == "jobs/x/cert.pdf"
        assert row.generated_at == now


# --- job recompute CAS ---


async def test_recompute_cas_dropped_when_terminal(session_factory) -> None:
    job_id = await _make_job(session_factory, status=JobStatus.CANCELLED)
    async with session_factory() as s:
        n = await JobsRepository(s).recompute_cas(job_id, JobStatus.COMPLETED)
        assert n == 0
        assert (await JobsRepository(s).get(job_id)).status == JobStatus.CANCELLED  # type: ignore[union-attr]


async def test_recompute_cas_happy_path_sets_completed_at_once(
    session_factory,
) -> None:
    job_id = await _make_job(session_factory, status=JobStatus.PROCESSING)
    async with session_factory() as s:
        repos = JobsRepository(s)
        n = await repos.recompute_cas(job_id, JobStatus.PROCESSING)
        assert n == 1
        row = await repos.get(job_id)
        assert row is not None
        assert row.status == JobStatus.PROCESSING
        assert row.completed_at is None
        n = await repos.recompute_cas(job_id, JobStatus.COMPLETED)
        assert n == 1
        await s.commit()
    async with session_factory() as s:
        repos = JobsRepository(s)
        row = await repos.get(job_id)
        assert row is not None
        assert row.status == JobStatus.COMPLETED
        assert row.completed_at is not None
        first_completed = row.completed_at
        # Terminal now: further recomputes are dropped, completed_at preserved.
        assert await repos.recompute_cas(job_id, JobStatus.FAILED) == 0
        assert (await repos.get(job_id)).completed_at == first_completed  # type: ignore[union-attr]


# --- cancel / reset / force-fail ---


async def test_cancel_open_skips_processing_and_done(session_factory) -> None:
    job_id = await _make_job(session_factory)
    async with session_factory() as s:
        await CertificatesRepository(s).insert_many(
            [
                _cert(job_id, status=CertificateStatus.PENDING),
                _cert(job_id, status=CertificateStatus.RETRYING),
                _cert(job_id, status=CertificateStatus.PROCESSING),
                _cert(job_id, status=CertificateStatus.DONE),
                _cert(job_id, status=CertificateStatus.FAILED),
            ]
        )
        await s.commit()
    async with session_factory() as s:
        repos = CertificatesRepository(s)
        n = await repos.cancel_open(job_id)
        assert n == 2
        await s.commit()
    async with session_factory() as s:
        rows = await CertificatesRepository(s).list_for_job(
            job_id, status=None, offset=0, limit=10
        )
        by_status = {c.status for c in rows}
        assert CertificateStatus.CANCELLED in by_status
        assert CertificateStatus.PROCESSING in by_status
        assert CertificateStatus.DONE in by_status
        assert CertificateStatus.FAILED in by_status
        assert CertificateStatus.PENDING not in by_status
        assert CertificateStatus.RETRYING not in by_status


async def test_force_fail_stale_only_at_or_above_cap(session_factory) -> None:
    job_id = await _make_job(session_factory)
    below_id = await _make_cert(
        session_factory, job_id, status=CertificateStatus.PROCESSING, attempts=1
    )
    at_cap_id = await _make_cert(
        session_factory,
        job_id,
        status=CertificateStatus.PROCESSING,
        attempts=MAX_ATTEMPTS,
    )
    retrying_id = await _make_cert(
        session_factory,
        job_id,
        status=CertificateStatus.RETRYING,
        attempts=MAX_ATTEMPTS + 1,
    )
    async with session_factory() as s:
        repos = CertificatesRepository(s)
        assert (
            await repos.force_fail_stale(
                below_id, "exhausted", max_attempts=MAX_ATTEMPTS
            )
            == 0
        )
        assert (
            await repos.force_fail_stale(
                at_cap_id, "exhausted", max_attempts=MAX_ATTEMPTS
            )
            == 1
        )
        assert (
            await repos.force_fail_stale(
                retrying_id, "exhausted", max_attempts=MAX_ATTEMPTS
            )
            == 1
        )
        await s.commit()
    async with session_factory() as s:
        repos = CertificatesRepository(s)
        assert (await repos.get(below_id)).status == CertificateStatus.PROCESSING  # type: ignore[union-attr]
        failed = await repos.get(at_cap_id)
        assert failed is not None
        assert failed.status == CertificateStatus.FAILED
        assert failed.error_message == "exhausted"


async def test_reset_to_pending_clears_token(session_factory) -> None:
    job_id = await _make_job(session_factory)
    cert_id = await _make_cert(session_factory, job_id)
    token = uuid.uuid4()
    async with session_factory() as s:
        claimed = await CertificatesRepository(s).claim(
            cert_id, max_attempts=MAX_ATTEMPTS, token=token
        )
        assert claimed is not None
        await s.commit()
    async with session_factory() as s:
        repos = CertificatesRepository(s)
        n = await repos.reset_to_pending([cert_id])
        assert n == 1
        await s.commit()
    async with session_factory() as s:
        row = await CertificatesRepository(s).get(cert_id)
        assert row is not None
        assert row.status == CertificateStatus.PENDING
        assert row.claim_token is None


# --- staleness uses the DB clock ---


async def test_find_stale_uses_db_clock(session_factory) -> None:
    job_id = await _make_job(session_factory)
    old_id = await _make_cert(
        session_factory, job_id, status=CertificateStatus.PROCESSING
    )
    fresh_id = await _make_cert(
        session_factory, job_id, status=CertificateStatus.PROCESSING
    )
    done_id = await _make_cert(session_factory, job_id, status=CertificateStatus.DONE)
    async with session_factory() as s:
        # Age rows with the DB clock so the test is immune to app/DB skew.
        await s.execute(
            update(Certificate)
            .where(Certificate.id.in_([old_id, done_id]))
            .values(updated_at=func.now() - func.make_interval(0, 0, 0, 0, 0, 0, 3600))
        )
        await s.commit()
    async with session_factory() as s:
        stale = await CertificatesRepository(s).find_stale(
            statuses=[CertificateStatus.PROCESSING, CertificateStatus.RETRYING],
            grace_seconds=120,
        )
        stale_ids = {c.id for c in stale}
        assert old_id in stale_ids
        assert fresh_id not in stale_ids
        assert done_id not in stale_ids


async def test_find_stale_pending_returns_ids(session_factory) -> None:
    job_id = await _make_job(session_factory)
    old_id = await _make_cert(session_factory, job_id)
    fresh_id = await _make_cert(session_factory, job_id)
    async with session_factory() as s:
        await s.execute(
            update(Certificate)
            .where(Certificate.id == old_id)
            .values(updated_at=func.now() - func.make_interval(0, 0, 0, 0, 0, 0, 3600))
        )
        await s.commit()
    async with session_factory() as s:
        ids = await CertificatesRepository(s).find_stale_pending(grace_seconds=120)
        assert old_id in set(ids)
        assert fresh_id not in set(ids)


async def test_jobs_find_stale(session_factory) -> None:
    old_id = await _make_job(session_factory)
    fresh_id = await _make_job(session_factory)
    async with session_factory() as s:
        await s.execute(
            update(Job)
            .where(Job.id == old_id)
            .values(updated_at=func.now() - func.make_interval(0, 0, 0, 0, 0, 0, 3600))
        )
        await s.commit()
    async with session_factory() as s:
        stale = await JobsRepository(s).find_stale(
            statuses=[JobStatus.PENDING, JobStatus.PROCESSING], grace_seconds=120
        )
        stale_ids = {j.id for j in stale}
        assert old_id in stale_ids
        assert fresh_id not in stale_ids


# --- counts / listings ---


async def test_status_counts_group_by(session_factory) -> None:
    job_a = await _make_job(session_factory)
    job_b = await _make_job(session_factory)
    job_c = await _make_job(session_factory)
    async with session_factory() as s:
        await CertificatesRepository(s).insert_many(
            [
                _cert(job_a, status=CertificateStatus.DONE),
                _cert(job_a, status=CertificateStatus.DONE),
                _cert(job_a, status=CertificateStatus.FAILED),
                _cert(job_b, status=CertificateStatus.PENDING),
            ]
        )
        await s.commit()
    async with session_factory() as s:
        counts = await CertificatesRepository(s).status_counts([job_a, job_b, job_c])
    assert counts[job_a] == {
        CertificateStatus.DONE: 2,
        CertificateStatus.FAILED: 1,
    }
    assert counts[job_b] == {CertificateStatus.PENDING: 1}
    assert counts.get(job_c, {}) == {}


async def test_unique_certificate_number_violation_raises(
    session_factory,
) -> None:
    job_id = await _make_job(session_factory)
    number = f"CERT-2026-{uuid.uuid4().hex[:12].upper()}"
    async with session_factory() as s:
        await CertificatesRepository(s).insert_many(
            [_cert(job_id, certificate_number=number)]
        )
        await s.commit()
    async with session_factory() as s:
        with pytest.raises(IntegrityError):
            await CertificatesRepository(s).insert_many(
                [_cert(job_id, certificate_number=number)]
            )


# --- jobs CRUD surface ---


async def test_jobs_insert_get_list_count(session_factory) -> None:
    key = f"key-{uuid.uuid4().hex}"
    job_id = await _make_job(session_factory, idempotency_key=key)
    async with session_factory() as s:
        repos = JobsRepository(s)
        row = await repos.get(job_id)
        assert row is not None
        assert row.event_name == "Test Event"
        assert row.status == JobStatus.PENDING
        assert await repos.get(uuid.uuid4()) is None
        by_key = await repos.get_by_idempotency_key(key)
        assert by_key is not None
        assert by_key.id == job_id
        assert await repos.get_by_idempotency_key("no-such-key") is None
        listed = await repos.list(status=JobStatus.PENDING, offset=0, limit=1000)
        assert job_id in {j.id for j in listed}
        assert await repos.count(status=JobStatus.PENDING) >= 1
        assert await repos.count(status=None) >= await repos.count(
            status=JobStatus.PENDING
        )


async def test_jobs_cas_transition(session_factory) -> None:
    job_id = await _make_job(session_factory)
    async with session_factory() as s:
        repos = JobsRepository(s)
        assert (
            await repos.cas_transition(
                job_id,
                from_statuses=[JobStatus.FAILED],
                to_status=JobStatus.PROCESSING,
                set_completed_at=False,
            )
            == 0
        )
        assert (
            await repos.cas_transition(
                job_id,
                from_statuses=[JobStatus.PENDING],
                to_status=JobStatus.PROCESSING,
                set_completed_at=False,
            )
            == 1
        )
        row = await repos.get(job_id)
        assert row is not None
        assert row.status == JobStatus.PROCESSING
        assert row.completed_at is None
        assert (
            await repos.cas_transition(
                job_id,
                from_statuses=[JobStatus.PROCESSING],
                to_status=JobStatus.CANCELLED,
                set_completed_at=True,
            )
            == 1
        )
        await s.commit()
    async with session_factory() as s:
        row = await JobsRepository(s).get(job_id)
        assert row is not None
        assert row.status == JobStatus.CANCELLED
        assert row.completed_at is not None


async def test_certs_get_list_pending_ids(session_factory) -> None:
    job_id = await _make_job(session_factory)
    async with session_factory() as s:
        repos = CertificatesRepository(s)
        await repos.insert_many(
            [
                _cert(job_id, status=CertificateStatus.PENDING),
                _cert(job_id, status=CertificateStatus.DONE),
            ]
        )
        await s.flush()
        rows = await repos.list_for_job(job_id, status=None, offset=0, limit=10)
        assert len(rows) == 2
        pending = await repos.list_for_job(
            job_id, status=CertificateStatus.PENDING, offset=0, limit=10
        )
        assert len(pending) == 1
        number = rows[0].certificate_number
        assert (await repos.get_by_number(number)) is not None
        assert await repos.get_by_number("CERT-404") is None
        assert await repos.get(uuid.uuid4()) is None
        pending_ids = await repos.pending_ids(job_id)
        assert len(pending_ids) == 1
        assert pending_ids[0] == pending[0].id
        assert await repos.pending_ids(uuid.uuid4()) == []
        limited = await repos.list_for_job(job_id, status=None, offset=0, limit=1)
        assert len(limited) == 1
        assert await repos.list_for_job(job_id, status=None, offset=10, limit=10) == []
        # exercised inside one session; roll back so other tests are unaffected
        await s.rollback()
