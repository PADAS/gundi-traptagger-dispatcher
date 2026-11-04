import pytest
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


def test_rate_limiter_redis_client_does_not_replay_commands():
    # INCR/DECR are not idempotent: replaying one after a dropped connection
    # could count a request twice or release it twice
    client = get_redis_db(retry_on_connection_errors=False)
    connection = client.connection_pool.make_connection()

    assert connection.retry._retries == 0
    assert connection.health_check_interval == settings.REDIS_HEALTH_CHECK_INTERVAL


class _DroppedReplyConnection:
    """
    Fakes the socket layer of redis.asyncio.Connection: every command is "sent"
    and the first reply is lost to a dropped connection, as when Redis applies
    a command but the connection resets before the reply arrives.
    """

    def __init__(self, mocker, reply):
        from redis.asyncio.connection import Connection

        self.sent = []
        self.disconnects = 0
        replies = iter([redis_exceptions.ConnectionError("Connection reset by peer")])

        async def read_response(*args, **kwargs):
            error = next(replies, None)
            if error:
                raise error
            return reply

        async def send_packed_command(conn, command, *args, **kwargs):
            self.sent.append(command)

        async def disconnect(*args, **kwargs):
            self.disconnects += 1

        async def noop(*args, **kwargs):
            return None

        async def can_read_destructive(*args, **kwargs):
            return False

        mocker.patch.object(Connection, "connect", noop)
        mocker.patch.object(Connection, "disconnect", disconnect)
        mocker.patch.object(Connection, "can_read_destructive", can_read_destructive)
        mocker.patch.object(Connection, "send_packed_command", send_packed_command)
        mocker.patch.object(Connection, "read_response", read_response)


@pytest.mark.asyncio
async def test_redis_client_reconnects_and_retries_after_dropped_connection(mocker):
    fake = _DroppedReplyConnection(mocker, reply="cached-value")
    client = get_redis_db()

    assert await client.get("some-key") == "cached-value"
    assert len(fake.sent) == 2  # Sent again after the dropped connection
    assert fake.disconnects >= 1  # The dropped connection was discarded


@pytest.mark.asyncio
async def test_rate_limiter_redis_client_does_not_resend_after_dropped_connection(
    mocker,
):
    fake = _DroppedReplyConnection(mocker, reply=4)
    client = get_redis_db(retry_on_connection_errors=False)

    with pytest.raises(redis_exceptions.ConnectionError):
        await client.decr("traptagger-url")
    assert len(fake.sent) == 1  # DECR was sent once, never replayed
