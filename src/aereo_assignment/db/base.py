"""Declarative base for all ORM models."""

from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Shared declarative base; ``alembic/env.py`` uses ``Base.metadata``."""
