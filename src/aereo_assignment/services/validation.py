"""Row-level recipient validation (pure, no IO).

Envelope-level shape (recipients list of 1..5000) is enforced by the request
schema; every per-row rule from spec section 8.1 lives here so invalid rows
can become per-certificate FAILED rows inside an accepted job instead of
killing the whole request.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

# Single simple rule: no whitespace/@ in the local or domain part, at least
# one dot in the domain. Deliberately not RFC-complete (no email-validator
# dependency); malformed rows are isolated as FAILED, never fatal.
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

_MAX_NAME_LEN = 200
_MAX_METADATA_KEYS = 10
_MAX_METADATA_STR_LEN = 200


@dataclass
class RecipientIn:
    """One submitted recipient row (unit-test stand-in for the request schema).

    The service only needs ``.name`` / ``.email`` / ``.extra_metadata``,
    so later pydantic schemas are accepted as long as they expose those.
    """

    name: str
    email: str
    extra_metadata: dict[str, str | int | bool] | None = field(default=None)


@dataclass
class ValidRecipient:
    name: str
    email: str
    extra_metadata: dict[str, str | int | bool]


@dataclass
class RowError:
    index: int
    field: str
    message: str


def _field(row: Any, name: str, default: Any = None) -> Any:
    if isinstance(row, Mapping):
        return row.get(name, default)
    return getattr(row, name, default)


def _validate_name(raw: Any) -> str | None:
    """Return an error message, or None when valid."""
    if not isinstance(raw, str):
        return "name is required"
    if not raw.strip():
        return "name must not be blank"
    if len(raw.strip()) > _MAX_NAME_LEN:
        return "name must be at most 200 characters"
    return None


def _validate_email(raw: Any) -> str | None:
    if not isinstance(raw, str):
        return "email is required"
    if not _EMAIL_RE.match(raw.strip()):
        return "email must be a valid email address"
    return None


def _validate_extra_metadata(raw: Any) -> str | None:
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        return "extra_metadata must be an object"
    if len(raw) > _MAX_METADATA_KEYS:
        return "extra_metadata must have at most 10 keys"
    for key, value in raw.items():
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            continue
        if isinstance(value, str):
            if len(value) > _MAX_METADATA_STR_LEN:
                return f"extra_metadata[{key!r}] must be at most 200 characters"
            continue
        return f"extra_metadata[{key!r}] must be a string, integer, or boolean"
    return None


def validate_recipients(
    rows: list[RecipientIn],
) -> tuple[list[ValidRecipient], list[RowError]]:
    """Split rows into valid recipients and per-row errors.

    One error per invalid row (first failure in name -> email ->
    extra_metadata order); ``RowError.index`` is the original position so
    callers can map errors back to the request. A row is either valid or
    erroneous, never both.
    """
    valid: list[ValidRecipient] = []
    errors: list[RowError] = []
    for index, row in enumerate(rows):
        name = _field(row, "name")
        email = _field(row, "email")
        extra = _field(row, "extra_metadata")

        message = _validate_name(name)
        failed_field = "name"
        if message is None:
            message = _validate_email(email)
            failed_field = "email"
        if message is None:
            message = _validate_extra_metadata(extra)
            failed_field = "extra_metadata"

        if message is not None:
            errors.append(RowError(index=index, field=failed_field, message=message))
        else:
            valid.append(
                ValidRecipient(
                    name=name.strip(),
                    email=email.strip(),
                    extra_metadata=dict(extra) if extra else {},
                )
            )
    return valid, errors
