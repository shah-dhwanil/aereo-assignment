from aereo_assignment.config import Settings


def test_settings_defaults_satisfy_invariant():
    s = Settings(database_url="postgresql+asyncpg://x", redis_url="redis://x")
    assert s.retry_backoff_seconds * s.max_generation_attempts < s.reconcile_grace_seconds


def test_settings_invariant_rejected():
    import pytest
    with pytest.raises(ValueError, match="reconcile_grace_seconds"):
        Settings(database_url="postgresql+asyncpg://x", redis_url="redis://x",
                 retry_backoff_seconds=100, max_generation_attempts=4,
                 reconcile_grace_seconds=120)


def test_env_override(monkeypatch):
    monkeypatch.setenv("BCG_PAGE_SIZE_DEFAULT", "25")
    s = Settings(database_url="postgresql+asyncpg://x", redis_url="redis://x")
    assert s.page_size_default == 25
