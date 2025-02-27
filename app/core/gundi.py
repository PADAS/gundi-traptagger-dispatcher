import logging
from uuid import UUID
import backoff
import httpx
from gundi_core import schemas
from app.core import settings
from app.core.utils import (
    ExtraKeys,
    read_config_from_cache_safe,
    write_config_in_cache_safe,
)
from app.core.errors import ReferenceDataError
from gundi_client import PortalApi
from gundi_core.schemas import v2 as gundi_schemas_v2
from gundi_client_v2 import GundiClient
from .utils import redis_client

logger = logging.getLogger(__name__)


DEFAULT_LOCATION = schemas.Location(x=0.0, y=0.0)
GUNDI_V1 = "v1"
GUNDI_V2 = "v2"

connect_timeout, read_timeout = settings.DEFAULT_REQUESTS_TIMEOUT
portal_client = PortalApi(connect_timeout=connect_timeout, data_timeout=read_timeout)
_cache_ttl = settings.PORTAL_CONFIG_OBJECT_CACHE_TTL


async def get_outbound_config_detail(
    outbound_id: UUID,
) -> schemas.OutboundConfiguration:
    if not outbound_id:
        raise ValueError("integration_id must not be None")

    extra_dict = {
        ExtraKeys.AttentionNeeded: True,
        ExtraKeys.OutboundIntId: str(outbound_id),
    }

    cache_key = f"outbound_detail.{outbound_id}"
    cached = await redis_client.get(cache_key)

    if cached:
        config = schemas.OutboundConfiguration.parse_raw(cached)
        logger.debug(
            "Using cached outbound integration detail",
            extra={
                **extra_dict,
                ExtraKeys.AttentionNeeded: False,
                "outbound_detail": config,
            },
        )
        return config

    logger.debug(f"Cache miss for outbound integration detail", extra={**extra_dict})

    try:
        response = await portal_client.get_outbound_integration(
            integration_id=str(outbound_id)
        )
    except httpx.HTTPStatusError as e:
        error = f"HTTPStatusError: {e.response.status_code}, {e.response.text}"
        message = (
            f"Failed to get outbound details for outbound_id {outbound_id}: {error}"
        )
        target_url = str(e.request.url)
        logger.exception(
            message,
            extra={
                **extra_dict,
                ExtraKeys.AttentionNeeded: True,
                ExtraKeys.Url: target_url,
            },
        )
        # Raise again so it's retried later
        raise ReferenceDataError(message)
    except httpx.HTTPError as e:
        error = f"HTTPError: {e}"
        message = (
            f"Failed to get outbound details for outbound_id {outbound_id}: {error}"
        )
        target_url = str(e.request.url)
        logger.exception(
            message,
            extra={
                **extra_dict,
                ExtraKeys.AttentionNeeded: True,
                ExtraKeys.Url: target_url,
            },
        )
        # Raise again so it's retried later
        raise ReferenceDataError(message)
    else:
        try:
            config = schemas.OutboundConfiguration.parse_obj(response)
        except Exception:
            logger.error(
                f"Failed decoding response for Outbound Integration Detail",
                extra={**extra_dict, "resp_text": response},
            )
            raise ReferenceDataError(
                "Failed decoding response for Outbound Integration Detail"
            )
        else:
            if config:  # don't cache empty response
                await redis_client.setex(cache_key, _cache_ttl, config.json())
            return config


async def get_inbound_integration_detail(
    integration_id: UUID,
) -> schemas.IntegrationInformation:
    if not integration_id:
        raise ValueError("integration_id must not be None")

    extra_dict = {
        ExtraKeys.AttentionNeeded: True,
        ExtraKeys.InboundIntId: str(integration_id),
    }

    cache_key = f"inbound_detail.{integration_id}"
    cached = await redis_client.get(cache_key)

    if cached:
        config = schemas.IntegrationInformation.parse_raw(cached)
        logger.debug(
            "Using cached inbound integration detail",
            extra={**extra_dict, "integration_detail": config},
        )
        return config

    logger.debug(f"Cache miss for inbound integration detai", extra={**extra_dict})

    try:
        response = await portal_client.get_inbound_integration(
            integration_id=str(integration_id)
        )
    except httpx.HTTPStatusError as e:
        error = f"HTTPStatusError: {e.response.status_code}, {e.response.text}"
        message = f"Failed to get inbound details for integration_id {integration_id}: {error}"
        target_url = str(e.request.url)
        logger.exception(
            message,
            extra={
                **extra_dict,
                ExtraKeys.AttentionNeeded: True,
                ExtraKeys.Url: target_url,
            },
        )
        # Raise again so it's retried later
        raise ReferenceDataError(message)
    except httpx.HTTPError as e:
        error = f"HTTPError: {e}"
        message = f"Failed to get inbound details for integration_id {integration_id}: {error}"
        target_url = str(e.request.url)
        logger.exception(
            message,
            extra={
                **extra_dict,
                ExtraKeys.AttentionNeeded: True,
                ExtraKeys.Url: target_url,
            },
        )
        # Raise again so it's retried later
        raise ReferenceDataError(message)
    else:
        try:
            config = schemas.IntegrationInformation.parse_obj(response)
        except Exception:
            logger.error(
                f"Failed decoding response for InboundIntegration Detail",
                extra={**extra_dict, "resp_text": response},
            )
            raise ReferenceDataError(
                "Failed decoding response for InboundIntegration Detail"
            )
        else:
            if config:  # don't cache empty response
                await redis_client.setex(cache_key, _cache_ttl, config.json())
            return config


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
