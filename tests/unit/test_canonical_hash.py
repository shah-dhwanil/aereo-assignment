"""Unit tests for the canonical request hash (pure domain, no IO)."""

import re
from datetime import date

from aereo_assignment.services.idempotency import canonical_request_hash


def test_key_order_independent():
    assert canonical_request_hash({"a": 1, "b": 2}) == canonical_request_hash(
        {"b": 2, "a": 1}
    )


def test_different_payload_different_hash():
    assert canonical_request_hash({"a": 1}) != canonical_request_hash({"a": 2})


def test_hash_is_sha256_hex():
    assert re.fullmatch(r"[0-9a-f]{64}", canonical_request_hash({"a": 1}))


def test_non_json_native_values_supported():
    assert canonical_request_hash({"d": date(2026, 3, 15)}) == canonical_request_hash(
        {"d": date(2026, 3, 15)}
    )
