"""Certificate PDF renderer (pure, sync)."""

from __future__ import annotations

import textwrap
from datetime import date
from io import BytesIO
from pathlib import Path

import segno
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont, TTFontFile
from reportlab.pdfgen import canvas

_FONT_DIR = Path(__file__).resolve().parents[3] / "assets" / "fonts"

_FONTS = (
    ("DejaVuSans", "DejaVuSans.ttf"),
    ("DejaVuSans-Bold", "DejaVuSans-Bold.ttf"),
    # CJK fallback: DejaVu has no CJK Unified Ideographs glyphs, so runs
    # containing codepoints DejaVu lacks are drawn with Noto Sans JP.
    ("NotoSansJP", "NotoSansJP-Regular.ttf"),
    ("NotoSansJP-Bold", "NotoSansJP-Bold.ttf"),
)

for _name, _file in _FONTS:
    if _name not in pdfmetrics.getRegisteredFontNames():
        pdfmetrics.registerFont(TTFont(_name, str(_FONT_DIR / _file)))


def _coverage(path: Path) -> frozenset[int]:
    return frozenset(TTFontFile(str(path), validate=0, subfontIndex=0).charToGlyph)


_REGULAR_COVERAGE = _coverage(_FONT_DIR / "DejaVuSans.ttf")
_BOLD_COVERAGE = _coverage(_FONT_DIR / "DejaVuSans-Bold.ttf")

_MARGIN = 48.0
_QR_SIZE = 110.0


def _runs(text: str, *, bold: bool) -> list[tuple[str, str]]:
    """Split text into (font_name, chunk) runs by DejaVu coverage."""
    primary = "DejaVuSans-Bold" if bold else "DejaVuSans"
    fallback = "NotoSansJP-Bold" if bold else "NotoSansJP"
    coverage = _BOLD_COVERAGE if bold else _REGULAR_COVERAGE
    runs: list[list[str]] = []
    for char in text:
        font = primary if ord(char) in coverage else fallback
        if runs and runs[-1][0] == font:
            runs[-1][1] += char
        else:
            runs.append([font, char])
    return [(font, chunk) for font, chunk in runs]


def _draw_centred(
    c: canvas.Canvas, width: float, y: float, text: str, size: float, *, bold: bool
) -> None:
    runs = _runs(text, bold=bold)
    total = sum(pdfmetrics.stringWidth(chunk, font, size) for font, chunk in runs)
    x = (width - total) / 2
    for font, chunk in runs:
        c.setFont(font, size)
        c.drawString(x, y, chunk)
        x += pdfmetrics.stringWidth(chunk, font, size)


def render_certificate(
    *,
    recipient_name: str,
    recipient_email: str,
    event_name: str,
    event_date: date,
    issuer: str,
    description: str | None,
    certificate_number: str,
    verify_url: str,
    extra_metadata: dict | None,
) -> bytes:
    """Render a landscape A4 certificate PDF and return its bytes.

    ``extra_metadata`` is accepted for API symmetry but never rendered.
    """
    width, height = landscape(A4)
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=landscape(A4))

    c.rect(_MARGIN, _MARGIN, width - 2 * _MARGIN, height - 2 * _MARGIN)
    c.rect(
        _MARGIN + 6,
        _MARGIN + 6,
        width - 2 * (_MARGIN + 6),
        height - 2 * (_MARGIN + 6),
    )

    _draw_centred(c, width, height - 130, issuer, 30, bold=True)
    _draw_centred(c, width, height / 2 + 40, recipient_name, 40, bold=True)
    _draw_centred(c, width, height / 2 + 12, recipient_email, 13, bold=False)
    _draw_centred(
        c,
        width,
        height / 2 - 30,
        f"{event_name} \u00b7 {event_date.isoformat()}",
        18,
        bold=False,
    )

    if description:
        y = height / 2 - 60
        for line in textwrap.wrap(description, width=90):
            _draw_centred(c, width, y, line, 13, bold=False)
            y -= 18

    c.setFont("DejaVuSans", 11)
    c.drawString(_MARGIN + 18, _MARGIN + 18, f"Certificate No: {certificate_number}")

    qr_buf = BytesIO()
    segno.make(verify_url).save(qr_buf, kind="png", scale=4)
    qr_buf.seek(0)
    c.drawImage(
        ImageReader(qr_buf),
        width - _MARGIN - 18 - _QR_SIZE,
        _MARGIN + 18,
        width=_QR_SIZE,
        height=_QR_SIZE,
    )

    c.showPage()
    c.save()
    return buf.getvalue()
