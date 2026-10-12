"""Integration tests for certificate retrieval: list, download, ZIP (Task 12).

TDD Step 1: these fail before implementation (no service / schemas / routes
yet). App fixture follows the T8/T10 pattern (``RecordingBroker`` via
``app.state.broker``, ``dependency_overrides`` for the session).

NOTE: the container DB is shared across the whole pytest session, so rows
from other modules are visible. Certificate-list/ZIP/download assertions are
scoped to jobs seeded here (per-job endpoints), never on absolute totals.
"""

from __future__ import annotations

import io
import uuid
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID

import pytest
from conftest import RecordingBroker
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from aereo_assignment.db.models import (
    Certificate,
    CertificateStatus,
    Job,
    JobStatus,
)
from aereo_assignment.repositories.certificates import CertificatesRepository
from aereo_assignment.repositories.jobs import JobsRepository
from aereo_assignment.services.storage import DiskStorage

TAG = uuid.uuid4().hex[:8]
DONE = CertificateStatus.DONE


@pytest.fixture
def app(session_factory, tmp_path):
    from aereo_assignment.config import Settings
    from aereo_assignment.db.session import get_session
    from aereo_assignment.main import create_app

    settings = Settings(
        database_url="postgresql+asyncpg://localhost:5432/test",
        redis_url="redis://localhost:6379/0",
        storage_dir=tmp_path / "storage",
    )
    application = create_app(settings=settings)
    application.state.broker = RecordingBroker()
    settings.storage_dir.mkdir(parents=True, exist_ok=True)

    async def _override():
        async with session_factory() as s:
            yield s

    application.dependency_overrides[get_session] = _override
    return application


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.fixture
def storage(app) -> DiskStorage:
    return DiskStorage(app.state.settings.storage_dir)


def _file_path(storage: DiskStorage, job_id: UUID, number: str) -> Path:
    return Path(storage._root) / str(job_id) / f"{number}.pdf"


async def _seed_job(
    session_factory: async_sessionmaker[AsyncSession], index: int
) -> UUID:
    async with session_factory() as s:
        job = await JobsRepository(s).insert(
            Job(
                event_name=f"Retrieval-{TAG}-{index}",
                event_date=date(2026, 10, 1),
                issuer="Aereo",
                idempotency_key=f"ret-{TAG}-{index}-{uuid.uuid4().hex}",
                status=JobStatus.PROCESSING,
            )
        )
        await s.flush()
        await s.commit()
        return job.id


async def _seed_cert(
    session_factory: async_sessionmaker[AsyncSession],
    job_id: UUID,
    status: CertificateStatus,
    storage: DiskStorage | None = None,
    payload: bytes | None = None,
) -> Certificate:
    number = f"RT-{TAG}-{uuid.uuid4().hex[:8].upper()}"
    key = f"{job_id}/{number}.pdf"
    async with session_factory() as s:
        cert = Certificate(
            job_id=job_id,
            recipient_name="Ada Lovelace",
            recipient_email="ada@example.com",
            extra_metadata={},
            certificate_number=number,
            status=status,
            file_path=key if status is DONE else None,
            generated_at=datetime.now(UTC) if status is DONE else None,
        )
        await CertificatesRepository(s).insert_many([cert])
        await s.commit()
    if payload is not None:
        assert storage is not None
        await storage.write(key, payload)
    return cert


def _payload(number: str) -> bytes:
    return f"%PDF-1.4 bytes-for-{number}".encode()


async def test_cert_list_filters_by_status_and_paginates(
    client, session_factory, storage
) -> None:
    job_id = await _seed_job(session_factory, 1)
    done1 = await _seed_cert(session_factory, job_id, DONE, storage, _payload("a"))
    done2 = await _seed_cert(session_factory, job_id, DONE, storage, _payload("b"))
    pend = await _seed_cert(session_factory, job_id, CertificateStatus.PENDING)
    fail = await _seed_cert(session_factory, job_id, CertificateStatus.FAILED)

    resp = await client.get(f"/api/v1/jobs/{job_id}/certificates")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["page"] == 1
    assert body["total_items"] == 4
    assert body["total_pages"] == 1
    by_number = {item["certificate_number"]: item for item in body["items"]}
    assert set(by_number) == {
        done1.certificate_number,
        done2.certificate_number,
        pend.certificate_number,
        fail.certificate_number,
    }
    for item in body["items"]:
        assert set(item) == {
            "certificate_id",
            "recipient_name",
            "recipient_email",
            "status",
            "certificate_number",
            "attempts",
            "error_message",
            "download_url",
            "generated_at",
        }
        assert item["recipient_name"] == "Ada Lovelace"
        assert item["attempts"] == 0
    for number in (done1.certificate_number, done2.certificate_number):
        item = by_number[number]
        assert item["status"] == "DONE"
        assert item["generated_at"] is not None
        assert item["download_url"] == (
            f"http://localhost:8000/api/v1/jobs/{job_id}"
            f"/certificates/{item['certificate_id']}/download"
        )
    for number in (pend.certificate_number, fail.certificate_number):
        item = by_number[number]
        assert item["download_url"] is None
        assert item["generated_at"] is None

    filt = await client.get(
        f"/api/v1/jobs/{job_id}/certificates", params={"status": "DONE"}
    )
    assert filt.status_code == 200, filt.text
    filt_body = filt.json()
    assert filt_body["total_items"] == 2
    assert {i["certificate_number"] for i in filt_body["items"]} == {
        done1.certificate_number,
        done2.certificate_number,
    }

    p1 = await client.get(
        f"/api/v1/jobs/{job_id}/certificates", params={"size": 2, "page": 1}
    )
    p2 = await client.get(
        f"/api/v1/jobs/{job_id}/certificates", params={"size": 2, "page": 2}
    )
    assert p1.status_code == 200 and p2.status_code == 200
    assert p1.json()["total_pages"] == 2
    ids1 = {i["certificate_id"] for i in p1.json()["items"]}
    ids2 = {i["certificate_id"] for i in p2.json()["items"]}
    assert len(ids1) == 2 and len(ids2) == 2 and ids1.isdisjoint(ids2)

    big = await client.get(f"/api/v1/jobs/{job_id}/certificates", params={"size": 500})
    assert big.status_code == 200, big.text
    assert big.json()["size"] == 200


async def test_cert_list_unknown_job_404(client) -> None:
    resp = await client.get(f"/api/v1/jobs/{uuid.uuid4()}/certificates")
    assert resp.status_code == 404, resp.text
    assert resp.json()["error"]["code"] == "JOB_NOT_FOUND"


async def test_download_pdf_200_with_disposition_header(
    client, session_factory, storage
) -> None:
    job_id = await _seed_job(session_factory, 2)
    cert = await _seed_cert(session_factory, job_id, DONE, storage, None)
    payload = _payload(cert.certificate_number)
    await storage.write(f"{job_id}/{cert.certificate_number}.pdf", payload)

    resp = await client.get(f"/api/v1/jobs/{job_id}/certificates/{cert.id}/download")
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"] == "application/pdf"
    assert resp.headers["content-disposition"] == (
        f'attachment; filename="{cert.certificate_number}.pdf"'
    )
    assert resp.content == payload


async def test_download_not_ready_409(client, session_factory, storage) -> None:
    job_id = await _seed_job(session_factory, 3)
    cert = await _seed_cert(session_factory, job_id, CertificateStatus.PENDING)
    resp = await client.get(f"/api/v1/jobs/{job_id}/certificates/{cert.id}/download")
    assert resp.status_code == 409, resp.text
    assert resp.json()["error"]["code"] == "CERT_NOT_READY"


async def test_download_unknown_cert_404(client, session_factory) -> None:
    job_id = await _seed_job(session_factory, 4)
    resp = await client.get(
        f"/api/v1/jobs/{job_id}/certificates/{uuid.uuid4()}/download"
    )
    assert resp.status_code == 404, resp.text
    assert resp.json()["error"]["code"] == "CERT_NOT_FOUND"


async def test_download_file_missing_409_row_stays_done(
    client, session_factory, storage
) -> None:
    job_id = await _seed_job(session_factory, 5)
    cert = await _seed_cert(session_factory, job_id, DONE, storage, b"gone-soon")
    _file_path(storage, job_id, cert.certificate_number).unlink()

    resp = await client.get(f"/api/v1/jobs/{job_id}/certificates/{cert.id}/download")
    assert resp.status_code == 409, resp.text
    assert resp.json()["error"]["code"] == "FILE_MISSING"

    async with session_factory() as s:
        row = await CertificatesRepository(s).get(cert.id)
        assert row is not None and row.status is DONE


async def test_zip_contains_all_done_pdfs(client, session_factory, storage) -> None:
    job_id = await _seed_job(session_factory, 6)
    certs = []
    for _ in range(2):
        c = await _seed_cert(session_factory, job_id, DONE, storage, None)
        payload = _payload(c.certificate_number)
        await storage.write(f"{job_id}/{c.certificate_number}.pdf", payload)
        certs.append((c, payload))
    await _seed_cert(session_factory, job_id, CertificateStatus.PENDING)

    resp = await client.get(f"/api/v1/jobs/{job_id}/download.zip")
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"] == "application/zip"
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        assert set(zf.namelist()) == {f"{c.certificate_number}.pdf" for c, _ in certs}
        for c, payload in certs:
            assert zf.read(f"{c.certificate_number}.pdf") == payload


async def test_zip_skips_missing_and_writes_manifest(
    client, session_factory, storage
) -> None:
    job_id = await _seed_job(session_factory, 7)
    kept = await _seed_cert(session_factory, job_id, DONE, storage, None)
    await storage.write(
        f"{job_id}/{kept.certificate_number}.pdf", _payload(kept.certificate_number)
    )
    missing = await _seed_cert(session_factory, job_id, DONE, storage, None)
    await storage.write(
        f"{job_id}/{missing.certificate_number}.pdf",
        _payload(missing.certificate_number),
    )
    _file_path(storage, job_id, missing.certificate_number).unlink()

    resp = await client.get(f"/api/v1/jobs/{job_id}/download.zip")
    assert resp.status_code == 200, resp.text
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        names = set(zf.namelist())
        assert f"{kept.certificate_number}.pdf" in names
        assert f"{missing.certificate_number}.pdf" not in names
        assert "SKIPPED.txt" in names
        assert missing.certificate_number in zf.read("SKIPPED.txt").decode()


async def test_zip_zero_done_409(client, session_factory) -> None:
    job_id = await _seed_job(session_factory, 8)
    await _seed_cert(session_factory, job_id, CertificateStatus.PENDING)
    resp = await client.get(f"/api/v1/jobs/{job_id}/download.zip")
    assert resp.status_code == 409, resp.text
    assert resp.json()["error"]["code"] == "NO_CERTIFICATES_READY"


async def test_zip_unknown_job_404(client) -> None:
    resp = await client.get(f"/api/v1/jobs/{uuid.uuid4()}/download.zip")
    assert resp.status_code == 404, resp.text
    assert resp.json()["error"]["code"] == "JOB_NOT_FOUND"
