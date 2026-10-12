"""Taskiq broker, smart-retry middleware, and scheduler wiring."""

from __future__ import annotations

from pydantic import ValidationError
from taskiq import TaskiqScheduler
from taskiq.events import TaskiqEvents
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
    idle_timeout=settings.reconcile_grace_seconds
    * 1000,  # ms; XAUTOCLAIM redelivers unacked after grace
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


# NOTE: installed taskiq 0.13 has no ``@broker.startup()`` decorator; startup
# hooks register via ``on_event``. This is the second trigger for the same
# ``run_reconcile`` implementation the periodic ``reconcile`` task calls (C1).
# Registered on both role events because ``AsyncBroker.startup()`` dispatches
# exactly one of them per process (worker vs. client) — no double-run.
@broker.on_event(TaskiqEvents.WORKER_STARTUP, TaskiqEvents.CLIENT_STARTUP)
async def reconcile_on_startup(state) -> None:
    """Startup recovery: run one reconcile pass over crashed work."""
    import importlib

    from aereo_assignment.worker import deps as deps_module

    # importlib (not `from ...tasks import reconcile`, which binds the task
    # function re-exported by the package) so tests can monkeypatch
    # `run_reconcile` on the module object.
    reconcile_module = importlib.import_module(
        "aereo_assignment.worker.tasks.reconcile"
    )

    await reconcile_module.run_reconcile(deps_module.get_reconcile_deps())
