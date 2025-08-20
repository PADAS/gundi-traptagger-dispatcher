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
