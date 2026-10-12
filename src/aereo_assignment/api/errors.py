"""HTTP error taxonomy and exception handlers (single envelope)."""

from __future__ import annotations

from typing import Any

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse


def _envelope(code: str, message: str, details: list[Any] | None = None) -> dict:
    return {"error": {"code": code, "message": message, "details": details or []}}


class ApiError(Exception):
    code: str = "INTERNAL_ERROR"
    message: str = "Internal server error"
    status_code: int = 500

    def __init__(
        self,
        message: str | None = None,
        *,
        details: list[Any] | None = None,
    ) -> None:
        super().__init__(message or self.message)
        if message is not None:
            self.message = message
        self.details: list[Any] = details or []


class ZeroValidRows(ApiError):
    code = "VALIDATION_ERROR"
    status_code = 422

    def __init__(self, *, details: list[Any] | None = None) -> None:
        super().__init__("No valid recipients", details=details)


class IdempotencyKeyReused(ApiError):
    code = "IDEMPOTENCY_KEY_REUSED"
    status_code = 409

    def __init__(self) -> None:
        super().__init__("Idempotency key already used with a different payload")


class JobNotFound(ApiError):
    code = "JOB_NOT_FOUND"
    status_code = 404

    def __init__(self) -> None:
        super().__init__("Job not found")


class JobAlreadyFinal(ApiError):
    code = "JOB_ALREADY_FINAL"
    status_code = 409

    def __init__(self) -> None:
        super().__init__("Job is already in a final state")


class CertNotFound(ApiError):
    code = "CERT_NOT_FOUND"
    status_code = 404

    def __init__(self) -> None:
        super().__init__("Certificate not found")


class CertNotReady(ApiError):
    code = "CERT_NOT_READY"
    status_code = 409

    def __init__(self) -> None:
        super().__init__("Certificate is not ready")


class FileMissing(ApiError):
    code = "FILE_MISSING"
    status_code = 409

    def __init__(self) -> None:
        super().__init__("Certificate file is missing")


class NoCertificatesReady(ApiError):
    code = "NO_CERTIFICATES_READY"
    status_code = 409

    def __init__(self) -> None:
        super().__init__("No certificates are ready")


class CertificateNotFound(ApiError):
    code = "CERTIFICATE_NOT_FOUND"
    status_code = 404

    def __init__(self) -> None:
        super().__init__("Certificate not found")


async def api_error_handler(request: Request, exc: ApiError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content=_envelope(exc.code, exc.message, exc.details),
    )


async def validation_error_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    details: list[Any] = []
    for err in exc.errors():
        loc = err.get("loc", ())
        field = ".".join(str(p) for p in loc if p != "body")
        details.append({"field": field, "message": err.get("msg", "")})
    return JSONResponse(
        status_code=422,
        content=_envelope("VALIDATION_ERROR", "Request validation failed", details),
    )


async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content=_envelope("INTERNAL_ERROR", "Internal server error", []),
    )
