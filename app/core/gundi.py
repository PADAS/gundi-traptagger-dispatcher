import logging
import backoff
import httpx
from app.core import settings
from app.core.utils import (
    ExtraKeys,
    read_config_from_cache_safe,
    write_config_in_cache_safe,
)
from gundi_core.schemas import v2 as gundi_schemas_v2
from gundi_client_v2 import GundiClient

logger = logging.getLogger(__name__)


GUNDI_V1 = "v1"
GUNDI_V2 = "v2"

connect_timeout, read_timeout = settings.DEFAULT_REQUESTS_TIMEOUT
_cache_ttl = settings.PORTAL_CONFIG_OBJECT_CACHE_TTL


@backoff.on_exception(backoff.expo, (httpx.HTTPError,), max_tries=5)
async def get_integration_details(integration_id: str) -> gundi_schemas_v2.Integration:
    """
    Helper function to retrieve integration configurations from Gundi API v2
    """

    if not integration_id:
        raise ValueError("integration_id must not be None")

    extra_dict = {
        ExtraKeys.AttentionNeeded: True,
        ExtraKeys.OutboundIntId: str(integration_id),
    }

    # Retrieve from cache if possible
    cache_key = f"integration_details.{integration_id}"
    cached = await read_config_from_cache_safe(
        cache_key=cache_key, extra_dict=extra_dict
    )

    if cached:
        config = gundi_schemas_v2.Integration.parse_raw(cached)
        logger.debug(
            "Using cached integration details",
            extra={
                **extra_dict,
                ExtraKeys.AttentionNeeded: False,
                "integration_detail": config,
            },
        )
        return config

    # Retrieve details from the portal
    logger.debug(f"Cache miss for integration details.", extra={**extra_dict})
    connect_timeout, read_timeout = settings.DEFAULT_REQUESTS_TIMEOUT
    async with GundiClient(
        connect_timeout=connect_timeout, data_timeout=read_timeout
    ) as portal_v2:
        try:
            integration = await portal_v2.get_integration_details(
                integration_id=integration_id
            )
        # ToDo: Catch more specific exceptions once the gundi client supports them
        except Exception as e:
            error_msg = f"Error retrieving integration details from the portal (v2) for integration {integration_id}: {type}: {e}"
            logger.exception(
                error_msg,
                extra=extra_dict,
            )
            raise e
        else:
            if integration:  # don't cache empty response
                await write_config_in_cache_safe(
                    key=cache_key,
                    ttl=_cache_ttl,
                    config=integration,
                    extra_dict=extra_dict,
                )
            return integration
