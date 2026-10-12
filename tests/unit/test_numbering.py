"""Unit tests for certificate numbering (pure domain, no IO)."""

import re

from aereo_assignment.services.numbering import (
    generate_certificate_number,
    generate_unique_numbers,
)

_NUMBER_RE = re.compile(r"^CERT-2026-[A-Z2-7]{8}$")


def test_number_format():
    number = generate_certificate_number(year=2026)
    assert _NUMBER_RE.match(number), number


def test_batch_unique():
    numbers = generate_unique_numbers(year=2026, count=5000)
    assert len(numbers) == 5000
    assert len(set(numbers)) == 5000
    assert all(_NUMBER_RE.match(n) for n in numbers)


def test_empty_batch():
    assert generate_unique_numbers(year=2026, count=0) == []
