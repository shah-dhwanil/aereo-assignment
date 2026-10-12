"""Canonical request hashing for idempotency keys (pure, no IO).

Stored at submit time as ``jobs.request_hash``; a replayed key with the same
hash returns the stored 202 snapshot, a different hash yields 409
(spec section 10.5).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any


def canonical_request_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()
