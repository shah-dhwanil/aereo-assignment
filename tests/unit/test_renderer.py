"""Unit tests for the certificate PDF renderer (pure, sync, no IO)."""

from datetime import date
from io import BytesIO

from pypdf import PdfReader

from aereo_assignment.pdf.renderer import render_certificate


def _kwargs(**overrides):
    params = {
        "recipient_name": "Dhwaníl Shah",
        "recipient_email": "dhwanil@example.com",
        "event_name": "Skyline Marathon",
        "event_date": date(2026, 10, 11),
        "issuer": "Aereo Athletics Club",
        "description": "For finishing the 10K run.",
        "certificate_number": "CERT-2026-AAAA2222",
        "verify_url": "https://example.com/verify/CERT-2026-AAAA2222",
        "extra_metadata": None,
    }
    params.update(overrides)
    return params


def _text(pdf_bytes: bytes) -> str:
    return PdfReader(BytesIO(pdf_bytes)).pages[0].extract_text()


def test_pdf_magic_bytes():
    pdf_bytes = render_certificate(**_kwargs())
    assert pdf_bytes.startswith(b"%PDF")


def test_recipient_name_in_text():
    pdf_bytes = render_certificate(**_kwargs())
    assert "Dhwaníl Shah" in _text(pdf_bytes)


def test_cjk_name_in_text():
    pdf_bytes = render_certificate(**_kwargs(recipient_name="山田太郎"))
    assert "山田太郎" in _text(pdf_bytes)


def test_cert_number_in_text():
    pdf_bytes = render_certificate(**_kwargs())
    assert "CERT-2026-AAAA2222" in _text(pdf_bytes)


def test_qr_image_present():
    pdf_bytes = render_certificate(**_kwargs())
    assert b"/Image" in pdf_bytes.replace(b" ", b"")


def test_mixed_script_name_in_text():
    pdf_bytes = render_certificate(**_kwargs(recipient_name="Dhwaníl 山田太郎"))
    text = _text(pdf_bytes)
    assert "Dhwaníl" in text
    assert "山田太郎" in text


def test_extra_metadata_not_rendered():
    pdf_bytes = render_certificate(**_kwargs(extra_metadata={"zz_meta": "zz-999"}))
    assert pdf_bytes.startswith(b"%PDF")
    assert "zz-999" not in _text(pdf_bytes)
