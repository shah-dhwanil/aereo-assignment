"""Application settings loaded from the environment."""

from __future__ import annotations

import functools
from pathlib import Path

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Bulk certificate generator configuration.

    Invariant: ``retry_backoff_seconds × max_generation_attempts`` must stay
    below ``reconcile_grace_seconds`` so the worst-case retry backoff never
    outruns the reconcile grace window.
    """

    model_config = SettingsConfigDict(env_prefix="BCG_", env_file=".env")

    database_url: str
    redis_url: str
    storage_dir: Path = Path("./storage")
    base_url: str = "http://localhost:8000"
    max_recipients_per_job: int = 5000
    max_generation_attempts: int = 4
    retry_backoff_seconds: int = 5
    reconcile_grace_seconds: int = 120
    reconcile_interval_seconds: int = 45
    page_size_default: int = 50
    page_size_max: int = 200
    stream_maxlen: int = 10_000
    log_level: str = "INFO"
    app_name: str = "bulk-certificate-generator"
    environment: str = "development"

    @model_validator(mode="after")
    def _check_backoff_within_grace(self) -> Settings:
        if not (
            self.retry_backoff_seconds * self.max_generation_attempts
            < self.reconcile_grace_seconds
        ):
            raise ValueError(
                "retry_backoff_seconds × max_generation_attempts must be "
                "< reconcile_grace_seconds"
            )
        return self


@functools.lru_cache
def get_settings() -> Settings:
    """Return the cached application settings."""
    return Settings()
