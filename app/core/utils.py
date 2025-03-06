# ToDo: Move base classes or utils into some common package?
import base64
import json
import logging
import aioredis
from enum import Enum
from redis import exceptions as redis_exceptions
from . import settings, errors


logger = logging.getLogger(__name__)


def get_redis_db():
    logger.debug(
        f"Connecting to REDIS DB :{settings.REDIS_DB} at {settings.REDIS_HOST}:{settings.REDIS_PORT}"
    )
    return aioredis.from_url(
        f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}",
        encoding="utf-8",
        decode_responses=True,
    )


_cache_ttl = settings.PORTAL_CONFIG_OBJECT_CACHE_TTL
redis_client = get_redis_db()
connect_timeout, read_timeout = settings.DEFAULT_REQUESTS_TIMEOUT


async def read_config_from_cache_safe(cache_key, extra_dict):
    try:
        config = await redis_client.get(cache_key)
    except redis_exceptions.ConnectionError as e:
        logger.warning(
            f"ConnectionError while reading integration configuration from Cache: {e}",
            extra={**extra_dict},
        )
        config = None
    except Exception as e:
        logger.warning(
            f"Unknown Error while reading integration configuration from Cache: {e}",
            extra={**extra_dict},
        )
        config = None
    finally:
        return config


async def write_config_in_cache_safe(key, ttl, config, extra_dict):
    try:
        await redis_client.setex(key, ttl, config.json())
    except redis_exceptions.ConnectionError as e:
        logger.warning(
            f"ConnectionError while writing integration configuration to Cache: {e}",
            extra={**extra_dict},
        )
    except Exception as e:
        logger.warning(
            f"Unknown Error while writing integration configuration to Cache: {e}",
            extra={**extra_dict},
        )


def extract_fields_from_message(message):
    if message:
        data = base64.b64decode(message.get("data", "").encode("utf-8"))
        observation = json.loads(data)
        attributes = message.get("attributes")
        if not observation:
            logger.warning(f"No observation was obtained from {message}")
        if not attributes:
            logger.debug(f"No attributes were obtained from {message}")
    else:
        logger.warning(f"message contained no payload", extra={"message": message})
        return None, None
    return observation, attributes


class ExtraKeys(str, Enum):
    def __str__(self):
        return str(self.value)

    DeviceId = "device_id"
    InboundIntId = "inbound_integration_id"
    OutboundIntId = "outbound_integration_id"
    AttentionNeeded = "attention_needed"
    StreamType = "stream_type"
    Provider = "provider"
    Error = "error"
    Url = "url"
    Observation = "observation"
    RetryTopic = "retry_topic"
    RetryAt = "retry_at"
    RetryAttempt = "retry_attempt"
    StatusCode = "status_code"
    DeadLetter = "dead_letter"
    GundiId = "gundi_id"
    RelatedTo = "related_to"


def is_null(value):
    return value in {None, "", "None", "null"}


def find_config_for_action(configurations, action_value):
    return next(
        (config for config in configurations if config.action.value == action_value),
        None,
    )


class RateLimiterSemaphore:
    def __init__(self, redis_client, url, **kwargs):
        self.url = url
        self.max_requests = kwargs.get("max_requests", settings.MAX_REQUESTS)
        self.max_requests_time_window_sec = kwargs.get(
            "max_requests_time_window_sec", settings.MAX_REQUESTS_TIME_WINDOW_SEC
        )
        self.redis_client = redis_client

    # Support using this as an async context manager.
    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.release()

    async def acquire(self, auto_release=True):
        """
        Try to acquire the counter semaphore:
            - Increase the number of requests made in the last time window
            - If the number of requests is greater than the limit, raise an exception
        """
        async with self.redis_client.pipeline(transaction=True) as pipe:
            operations = pipe.incr(self.url)
            if auto_release:
                operations = operations.expire(
                    self.url, self.max_requests_time_window_sec
                )
            res = await operations.execute()
        logger.debug(
            f"RateLimiterSemaphore<{self.url}>: {res[0]}/{self.max_requests} requests"
        )
        if res[0] > self.max_requests:
            raise errors.TooManyRequests(
                f"Too many requests in the last {self.max_requests_time_window_sec} seconds: {res[0]}"
            )

    async def release(self):
        """
        Release the semaphore:
            - Decrease the number of requests made in the last time window
        """
        await self.redis_client.decr(self.url)

    async def get_requests_count(self) -> int:
        """
        Get the number of requests made in the last time window
        """
        count = await self.redis_client.get(self.url)
        if count:
            return int(count)
        else:
            return 0

    def __str__(self):
        return f"RateLimiterSemaphore<{self.url}>: {self.max_requests}/{self.max_requests_time_window_sec} sec"

    def __repr__(self):
        return self.__str__()
