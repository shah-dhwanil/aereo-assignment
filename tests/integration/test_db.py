import uuid
from datetime import date

from sqlalchemy import inspect, text

from aereo_assignment.db.models import Certificate, Job, CertificateStatus, JobStatus


def test_migrations_create_tables_and_indexes(engine):
    insp = inspect(engine.sync_engine)
    assert {"jobs", "certificates"} <= set(insp.get_table_names())
    idx = {i["name"] for i in insp.get_indexes("certificates")}
    assert "ix_certificates_job_id_status" in idx


async def test_job_and_certificate_roundtrip(session_factory):
    async with session_factory() as s:
        job = Job(id=uuid.uuid4(), event_name="E", event_date=date(2026, 3, 15), issuer="I")
        s.add(job)
        cert = Certificate(id=uuid.uuid4(), job_id=job.id, recipient_name="Dhwaníl",
                           recipient_email="d@x.com", certificate_number="CERT-2026-AAAA2222")
        s.add(cert)
        await s.commit()
        rows = (
            await s.execute(
                text("SELECT status FROM jobs WHERE id = :id"), {"id": job.id}
            )
        ).scalar_one()
        assert rows == JobStatus.PENDING.value
