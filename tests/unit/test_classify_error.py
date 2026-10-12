"""Unit tests for error taxonomy (pure domain, no IO)."""

import pytest
from sqlalchemy.exc import InterfaceError, OperationalError

from aereo_assignment.services.errors import classify_error

# NOTE: asyncio.TimeoutError is an alias of builtin TimeoutError since
# Python 3.11 (asyncio.TimeoutError is TimeoutError), so the builtin
# covers the spec's transient entry; ruff UP041 enforces the builtin name.


@pytest.mark.parametrize(
    "exc",
    [
        OSError("disk full"),
        ConnectionError("refused"),
        TimeoutError(),
        OperationalError("SELECT 1", {}, Exception("boom")),
        InterfaceError("SELECT 1", {}, Exception("boom")),
    ],
)
def test_transient_errors(exc):
    assert classify_error(exc) == "transient"


@pytest.mark.parametrize(
    "exc",
    [
        ValueError("bad data"),
        RuntimeError("unknown bug"),
        KeyError("missing"),
        Exception("reportlab render failure"),
    ],
)
def test_permanent_errors(exc):
    assert classify_error(exc) == "permanent"
