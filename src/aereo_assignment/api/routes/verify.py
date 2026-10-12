"""Public certificate verification route (spec section 8.8)."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from aereo_assignment.db.session import get_session
from aereo_assignment.schemas.certificates import VerifyView
from aereo_assignment.services.certificate_service import verify_certificate

router = APIRouter(tags=["verify"])


@router.get("/verify/{certificate_number}")
async def verify(
    certificate_number: str,
    session: AsyncSession = Depends(get_session),  # noqa: B008 - FastAPI DI idiom
) -> VerifyView:
    return await verify_certificate(session, certificate_number)
