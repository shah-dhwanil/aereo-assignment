"""Error taxonomy for generation retries (pure, no IO).

Transient (retry): infrastructure flakes that may succeed on redelivery.
Permanent (fail fast): deterministic bugs — retrying would just burn
attempts and delay the visible FAILED row (spec section 10.1).
"""

from __future__ import annotations

import asyncio
from typing import Literal

from sqlalchemy.exc import InterfaceError, OperationalError

_TRANSIENT = (OSError, asyncio.TimeoutError, OperationalError, InterfaceError)
# Note: ConnectionError and asyncio.TimeoutError already subclass OSError,
# so they are covered by the OSError entry; they stay listed explicitly so
# the tuple mirrors the spec's transient list verbatim.


def classify_error(exc: BaseException) -> Literal["transient", "permanent"]:
    if isinstance(exc, _TRANSIENT):
        return "transient"
    return "permanent"
