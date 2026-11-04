from redis import exceptions as redis_exceptions

from app.core import settings
from app.core.utils import get_redis_db


def test_redis_client_checks_idle_connections_before_reuse():
    client = get_redis_db()
    connection = client.connection_pool.make_connection()

    assert connection.health_check_interval == settings.REDIS_HEALTH_CHECK_INTERVAL


def test_redis_client_retries_on_connection_errors():
    client = get_redis_db()
    connection = client.connection_pool.make_connection()

    assert redis_exceptions.ConnectionError in connection.retry_on_error
    assert redis_exceptions.TimeoutError in connection.retry_on_error
    assert connection.retry._retries == settings.REDIS_RETRY_ATTEMPTS
