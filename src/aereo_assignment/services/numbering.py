"""Certificate number generation (pure, no IO).

Format: ``CERT-<year>-<8 random base32 chars>`` (spec section 10.4). The
verify endpoint is keyed on this number, so the format is contractual.
"""

from __future__ import annotations

import secrets

# RFC 4648 base32 alphabet, uppercase, no padding: unambiguous and URL-safe.
_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
_RANDOM_BITS = 8


def generate_certificate_number(*, year: int) -> str:
    suffix = "".join(secrets.choice(_ALPHABET) for _ in range(_RANDOM_BITS))
    return f"CERT-{year}-{suffix}"


def generate_unique_numbers(*, year: int, count: int) -> list[str]:
    """Generate ``count`` distinct numbers (in-batch dedupe via a set)."""
    seen: set[str] = set()
    numbers: list[str] = []
    while len(numbers) < count:
        candidate = generate_certificate_number(year=year)
        if candidate not in seen:
            seen.add(candidate)
            numbers.append(candidate)
    return numbers
