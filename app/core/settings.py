import logging.config
import sys

from environs import Env

env = Env()
env.read_env()

LOGGING_LEVEL = env.str("LOGGING_LEVEL", "INFO")

DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {
            "level": LOGGING_LEVEL,
            "class": "logging.StreamHandler",
            "stream": sys.stdout,
        },
    },
    "loggers": {
        "": {
            "handlers": ["console"],
            "level": LOGGING_LEVEL,
        },
    },
}
logging.config.dictConfig(DEFAULT_LOGGING)

DEFAULT_REQUESTS_TIMEOUT = (10, 20)  # Connect, Read

CDIP_API_ENDPOINT = env.str("CDIP_API_ENDPOINT", None)
CDIP_ADMIN_ENDPOINT = env.str("CDIP_ADMIN_ENDPOINT", None)
PORTAL_API_ENDPOINT = f"{CDIP_ADMIN_ENDPOINT}/api/v1.0"
PORTAL_OUTBOUND_INTEGRATIONS_ENDPOINT = (
    f"{PORTAL_API_ENDPOINT}/integrations/outbound/configurations"
)
PORTAL_INBOUND_INTEGRATIONS_ENDPOINT = (
    f"{PORTAL_API_ENDPOINT}/integrations/inbound/configurations"
)

# Settings for caching admin portal request/responses
REDIS_HOST = env.str("REDIS_HOST", "localhost")
REDIS_PORT = env.int("REDIS_PORT", 6379)
REDIS_DB = env.int("REDIS_DB", 3)

REDIS_TOKEN_CACHE_DB = env.int("REDIS_TOKEN_CACHE_DB", 2)


def _require_explicit_redis_db(url: str) -> None:
    """A redis:// URL must name its database as a number: redis-py maps a
    missing or non-numeric path (redis://host:6379, redis://host/tokens) to
    db 0, which another consumer may own."""
    from redis.connection import parse_url as _parse_redis_url

    if not url.startswith(("redis://", "rediss://")):
        return
    if _parse_redis_url(url).get("db") is None:
        raise ValueError(
            "a redis:// token cache URL must name a numeric database index "
            "(e.g. redis://host:6379/2)"
        )


def validated_token_cache_url(url: str) -> str:
    """Return ``url`` if gundi-client-v2 can build a token cache backend from
    it, else "" (tokens shared within the process only) after one warning.
    A bad cache URL must not take the dispatcher down; redis connects lazily,
    so an unreachable Redis is handled by the client at request time."""
    from gundi_client_v2.errors import TokenCacheConfigError
    from gundi_client_v2.token_cache import token_cache_from_url

    if not url:
        return ""
    try:
        _require_explicit_redis_db(url)
        token_cache_from_url(url)
    except (TokenCacheConfigError, ValueError) as e:
        logging.getLogger(__name__).warning(
            "GUNDI_TOKEN_CACHE_URL is unusable (%s: %s); Gundi OAuth tokens will be "
            "shared within this process only. Check REDIS_HOST/REDIS_PORT/"
            "REDIS_TOKEN_CACHE_DB or the GUNDI_TOKEN_CACHE_URL override.",
            type(e).__name__,
            e,
        )
        return ""
    return url


# Shared OAuth token cache (gundi-client-v2 >= 3.7). Every GundiClient this
# dispatcher builds (one per portal cache miss) shares one Keycloak token per
# set of credentials, in process memory and in this Redis database, so the
# dispatcher services stop minting a token per miss. DB 2 is the same
# database the action runners use, so a runner and a dispatcher that share a
# Redis and credentials share tokens. Set GUNDI_TOKEN_CACHE_URL="" to keep
# tokens in-process only. The cache holds access and refresh tokens as
# plaintext JSON; protect the database like the config cache (db 3).
GUNDI_TOKEN_CACHE_URL = validated_token_cache_url(
    env.str(
        "GUNDI_TOKEN_CACHE_URL",
        f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_TOKEN_CACHE_DB}",
    )
)
# gundi-client-v2 reads its own settings module for the constructor default at
# construction time, so installing the URL there covers every GundiClient()
# in this process without touching the call sites.
from gundi_client_v2 import settings as gundi_client_settings  # noqa: E402

gundi_client_settings.GUNDI_TOKEN_CACHE_URL = GUNDI_TOKEN_CACHE_URL

# N-seconds to cache portal responses for configuration objects.
PORTAL_CONFIG_OBJECT_CACHE_TTL = env.int("PORTAL_CONFIG_OBJECT_CACHE_TTL", 60)
DISPATCHED_OBSERVATIONS_CACHE_TTL = env.int(
    "PORTAL_CONFIG_OBJECT_CACHE_TTL", 60 * 60
)  # 1 Hour

# Used in OTel traces/spans to set the 'environment' attribute, used on metrics calculation
TRACING_ENABLED = env.bool("TRACING_ENABLED", True)
TRACE_ENVIRONMENT = env.str("TRACE_ENVIRONMENT", "dev")

# Retries and dead-letter settings
GCP_PROJECT_ID = env.str("GCP_PROJECT_ID", "cdip-78ca")
GCP_ENVIRONMENT_ENABLED = env.bool("GCP_ENVIRONMENT_ENABLED", True)
LEGACY_DEAD_LETTER_TOPIC = env.str("DEAD_LETTER_TOPIC", "dispatchers-dead-letter-prod")
OBSERVATIONS_DEAD_LETTER_TOPIC = env.str(
    "OBSERVATIONS_DEAD_LETTER_TOPIC", "observations-dead-letter"
)
EVENTS_DEAD_LETTER_TOPIC = env.str("EVENTS_DEAD_LETTER_TOPIC", "events-dead-letter")
EVENTS_UPDATES_DEAD_LETTER_TOPIC = env.str(
    "EVENTS_UPDATES_DEAD_LETTER_TOPIC", "events-updates-dead-letter"
)
ATTACHMENTS_DEAD_LETTER_TOPIC = env.str(
    "ATTACHMENTS_DEAD_LETTER_TOPIC", "attachments-dead-letter"
)
TEXT_MESSAGES_DEAD_LETTER_TOPIC = env.str(
    "TEXT_MESSAGES_DEAD_LETTER_TOPIC", "text-messages-dead-letter"
)
DISPATCHER_EVENTS_TOPIC = env.str("DISPATCHER_EVENTS_TOPIC", "dispatcher-events-dev")
MAX_EVENT_AGE_SECONDS = env.int("MAX_EVENT_AGE_SECONDS", 86400)  # 24hrs
BUCKET_NAME = env.str("BUCKET_NAME", "cdip-files-dev")
DELETE_FILES_AFTER_DELIVERY = env.bool("DELETE_FILES_AFTER_DELIVERY", False)
IMAGE_METADATA_CACHE_TTL = env.int("IMAGE_METADATA_CACHE_TTL", 3600)  # 1 Hour

# Requests rate limitting
MAX_REQUESTS = env.int("MAX_REQUESTS", 3)
MAX_REQUESTS_TIME_WINDOW_SEC = env.int("MAX_REQUESTS_TIME_WINDOW_SEC", 1)
