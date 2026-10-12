"""Taskiq broker, smart-retry middleware, and scheduler wiring."""

from __future__ import annotations

from pydantic import ValidationError
from taskiq import TaskiqScheduler
from taskiq.middlewares import SmartRetryMiddleware
from taskiq.schedule_sources import LabelScheduleSource
from taskiq_redis import ListRedisScheduleSource, RedisStreamBroker

from aereo_assignment.config import Settings, get_settings


def _load_settings() -> Settings:
    """Settings for broker wiring; falls back when env is unconfigured.

    Import-time wiring must succeed without Redis (or any env) configured:
    constructing the brokers below is lazy (no connection until first use),
    so placeholder URLs are safe here — any real run provides ``BCG_*`` env.
    """
    try:
        return get_settings()
    except ValidationError:
        return Settings(
            database_url="postgresql+asyncpg://localhost:5432/unconfigured",
            redis_url="redis://localhost:6379/0",
        )


settings = _load_settings()

schedule_source = ListRedisScheduleSource(settings.redis_url)
broker = RedisStreamBroker(
    url=settings.redis_url,
    consumer_group_name="bulk-certificate-generator",
    idle_timeout=settings.reconcile_grace_seconds * 1000,  # ms; XAUTOCLAIM redelivers unacked after grace
    unacknowledged_lock_timeout=settings.reconcile_grace_seconds,
    maxlen=settings.stream_maxlen,
).with_middlewares(
    SmartRetryMiddleware(
        default_retry_count=settings.max_generation_attempts,
        default_delay=settings.retry_backoff_seconds,
        use_jitter=True,
        use_delay_exponent=True,
        schedule_source=schedule_source,
    )
)
scheduler = TaskiqScheduler(
    broker=broker, sources=[schedule_source, LabelScheduleSource(broker)]
)
