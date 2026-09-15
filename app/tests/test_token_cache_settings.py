import importlib
import logging
import os

import pytest


def _reload_settings(monkeypatch, **env):
    for key in ("GUNDI_TOKEN_CACHE_URL", "REDIS_TOKEN_CACHE_DB", "REDIS_HOST", "REDIS_PORT"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    import app.core.settings
    return importlib.reload(app.core.settings)


def test_token_cache_defaults_to_db_2_on_the_dispatcher_redis(monkeypatch):
    settings = _reload_settings(monkeypatch, REDIS_HOST="10.1.2.3", REDIS_PORT="6380")
    assert settings.REDIS_TOKEN_CACHE_DB == 2
    assert settings.GUNDI_TOKEN_CACHE_URL == "redis://10.1.2.3:6380/2"


def test_token_cache_url_is_installed_into_the_client_settings(monkeypatch):
    settings = _reload_settings(monkeypatch, REDIS_HOST="10.1.2.3", REDIS_PORT="6380")
    from gundi_client_v2 import settings as gundi_client_settings
    assert gundi_client_settings.GUNDI_TOKEN_CACHE_URL == settings.GUNDI_TOKEN_CACHE_URL


def test_token_cache_url_override_wins(monkeypatch):
    settings = _reload_settings(
        monkeypatch, REDIS_HOST="10.1.2.3", GUNDI_TOKEN_CACHE_URL="rediss://tokens.internal:6379/5"
    )
    assert settings.GUNDI_TOKEN_CACHE_URL == "rediss://tokens.internal:6379/5"


def test_empty_override_keeps_tokens_in_process_only(monkeypatch):
    settings = _reload_settings(monkeypatch, GUNDI_TOKEN_CACHE_URL="")
    assert settings.GUNDI_TOKEN_CACHE_URL == ""
    from gundi_client_v2 import settings as gundi_client_settings
    assert gundi_client_settings.GUNDI_TOKEN_CACHE_URL == ""


def test_redis_url_without_a_numeric_db_falls_back_to_in_process(monkeypatch):
    # redis-py maps a missing/non-numeric path to db 0; refuse it rather than
    # land tokens in a database another consumer may own.
    settings = _reload_settings(monkeypatch, GUNDI_TOKEN_CACHE_URL="redis://10.1.2.3:6379")
    assert settings.GUNDI_TOKEN_CACHE_URL == ""


def test_redis_url_without_a_numeric_db_logs_a_warning(caplog):
    import app.core.settings as settings
    with caplog.at_level(logging.WARNING):
        assert settings.validated_token_cache_url("redis://10.1.2.3:6379") == ""
    assert "GUNDI_TOKEN_CACHE_URL is unusable" in caplog.text


def test_unknown_scheme_falls_back_to_in_process(monkeypatch):
    settings = _reload_settings(monkeypatch, GUNDI_TOKEN_CACHE_URL="memcached://10.1.2.3:11211/2")
    assert settings.GUNDI_TOKEN_CACHE_URL == ""


def test_unknown_scheme_logs_a_warning(caplog):
    import app.core.settings as settings
    with caplog.at_level(logging.WARNING):
        assert settings.validated_token_cache_url("memcached://10.1.2.3:11211/2") == ""
    assert "GUNDI_TOKEN_CACHE_URL is unusable" in caplog.text


@pytest.fixture(autouse=True)
def _restore_settings(monkeypatch):
    yield
    monkeypatch.undo()  # restore the ambient environment before rebuilding settings from it
    import app.core.settings
    importlib.reload(app.core.settings)


def test_restore_fixture_rebuilds_settings_from_the_ambient_environment():
    # The autouse fixture must leave app.core.settings matching os.environ once
    # monkeypatch has undone each test's changes.
    import app.core.settings as settings
    expected_port = int(os.environ.get("REDIS_PORT", "6379"))
    assert settings.REDIS_PORT == expected_port
    assert settings.GUNDI_TOKEN_CACHE_URL.endswith(f":{expected_port}/{settings.REDIS_TOKEN_CACHE_DB}")
