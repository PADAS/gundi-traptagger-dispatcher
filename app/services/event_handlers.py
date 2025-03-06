import logging
import backoff
import httpx
import traceback
from datetime import datetime, timezone
from redis import exceptions as redis_exceptions
from gundi_core.events.transformers import (
    EventTransformedTrapTagger,
    AttachmentTransformedTrapTagger,
)
from app.core import tracing, settings
from app.core.errors import ReferenceDataError
from app.core.utils import (
    is_null,
    get_redis_db,
)
from app.core.system_events import publish_event
from app.core.gundi import get_integration_details
from gundi_core.schemas import v2 as gundi_schemas_v2
from gundi_core import events as system_events
from opentelemetry.trace import SpanKind
from .dispatchers import TrapTaggerImageDispatcher


_cache_db = get_redis_db()


logger = logging.getLogger(__name__)


async def get_destination_integration(destination_id):
    # Get details about the destination
    destination_integration = await get_integration_details(
        integration_id=destination_id
    )
    if not destination_integration:
        error_msg = (
            f"No destination config details found for destination_id {destination_id}"
        )
        logger.error(error_msg)
        raise ReferenceDataError(error_msg)

    return destination_integration


@backoff.on_exception(backoff.expo, (redis_exceptions.RedisError,), max_tries=5)
async def get_image_metadata_from_cache(
    gundi_id, destination_id
) -> gundi_schemas_v2.TrapTaggerImageMetadata:
    # Retrieve the image metadata from the cache
    try:
        if not gundi_id or not destination_id:
            raise ValueError("gundi_id and destination_id must be valid")
        key = f"traptagger_image_metadata.{gundi_id}.{destination_id}"
        cached_data = await _cache_db.get(name=key)
        if not cached_data:
            return None
        return gundi_schemas_v2.TrapTaggerImageMetadata.parse_raw(cached_data)
    except redis_exceptions.RedisError as e:
        logger.warning(
            f"ConnectionError while getting image metadata from cache for {gundi_id} and {destination_id}: {type(e)}: {e}",
        )
        raise e
    except Exception as e:
        logger.warning(
            f"Error while getting image metadata from cache for {gundi_id} and {destination_id}: {type(e)}: {e}",
        )
        raise e


@backoff.on_exception(backoff.expo, (redis_exceptions.RedisError,), max_tries=5)
async def cache_image_metadata(
    data: gundi_schemas_v2.TrapTaggerImageMetadata, gundi_id, destination_id: str
):
    try:
        if not gundi_id or not destination_id:
            raise ValueError("gundi_id and destination_id must be valid")

        key = f"traptagger_image_metadata.{gundi_id}.{destination_id}"
        await _cache_db.setex(
            name=key,
            time=settings.IMAGE_METADATA_CACHE_TTL,
            value=data.json(),
        )
    except redis_exceptions.RedisError as e:
        logger.warning(
            f"ConnectionError while caching image metadata for {gundi_id} and {destination_id}: {type(e)}: {e}",
        )
        raise e
    except Exception as e:
        logger.warning(
            f"Error while caching image metadata for {gundi_id} and {destination_id}: {type(e)}: {e}",
        )
        raise e


async def get_related_event(event_id, destination_id):
    # Check for related observations
    if is_null(event_id):
        return None
    # Check if the related object was cached
    related_observation = await get_image_metadata_from_cache(
        gundi_id=event_id, destination_id=destination_id
    )
    if not related_observation:
        error_msg = (
            f"Error getting related observation {event_id}. Will retry later.",
        )
        logger.error(error_msg)
        raise ReferenceDataError(error_msg)
    return related_observation


async def dispatch_image(
    integration: gundi_schemas_v2.Integration,
    image: gundi_schemas_v2.TrapTaggerImage,
    related_event: gundi_schemas_v2.TrapTaggerImageMetadata,
    attributes: dict,
):
    gundi_id = attributes.get("gundi_id")
    related_to = attributes.get("related_to")
    data_provider_id = attributes.get("data_provider_id")
    destination_id = attributes.get("destination_id")
    with tracing.tracer.start_as_current_span(
        "traptagger_dispatcher.dispatch_image", kind=SpanKind.CLIENT
    ) as current_span:
        try:
            dispatcher = TrapTaggerImageDispatcher(integration=integration)
            result = await dispatcher.send(image=image, related_event=related_event)
        except Exception as e:
            with tracing.tracer.start_as_current_span(
                "traptagger_dispatcher.error_dispatching_observation",
                kind=SpanKind.CLIENT,
            ) as error_span:
                error_msg = f"Error dispatching observation {gundi_id} to destination {destination_id}: {type(e).__name__}: {e}"
                logger.exception(error_msg)
                error_span.set_attribute("error", error_msg)
                # Extract additional response details if available
                if (
                    response := getattr(e, "response", None)
                ) is not None:  # bool(response) on status errors returns False
                    server_response_status = (getattr(response, "status_code", None),)
                    server_response_body = str(
                        getattr(response, "text", getattr(response, "content", None))
                        or ""
                    )
                # Emit events for the portal and other interested services (EDA)
                await publish_event(
                    event=system_events.ObservationDeliveryFailed(
                        payload=system_events.DeliveryErrorDetails(
                            error=error_msg,
                            error_traceback=traceback.format_exc(),
                            server_response_status=server_response_status,
                            server_response_body=server_response_body,
                            observation=gundi_schemas_v2.DispatchedObservation(
                                gundi_id=gundi_id,
                                related_to=related_to,
                                external_id=None,
                                data_provider_id=data_provider_id,
                                destination_id=destination_id,
                                delivered_at=datetime.now(timezone.utc),  # UTC
                            ),
                        )
                    ),
                    topic_name=settings.DISPATCHER_EVENTS_TOPIC,
                )
                # Raise so it can be retried by GCP
                raise e
        else:
            logger.debug(
                f"Observation {gundi_id} delivered with success. TrapTagger response: {result}"
            )
            current_span.set_attribute("is_dispatched_successfully", True)
            current_span.set_attribute("destination_id", str(destination_id))
            current_span.add_event(
                name="traptagger_dispatcher.observation_dispatched_successfully"
            )
            # Emit events for the portal and other interested services (EDA)
            dispatched_observation = gundi_schemas_v2.DispatchedObservation(
                gundi_id=gundi_id,
                related_to=related_to,
                external_id=result.get("image_id"),  # ID in the destination system
                data_provider_id=data_provider_id,
                destination_id=destination_id,
                delivered_at=datetime.now(timezone.utc),  # UTC
            )
            await publish_event(
                event=system_events.ObservationDelivered(
                    payload=dispatched_observation
                ),
                topic_name=settings.DISPATCHER_EVENTS_TOPIC,
            )
            return result


async def handle_traptagger_event(event: EventTransformedTrapTagger, attributes: dict):
    # Trace observations with Open Telemetry
    with tracing.tracer.start_as_current_span(
        "traptagger_dispatcher.handle_traptagger_event", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("payload", repr(event.payload))
        gundi_id = attributes.get("gundi_id")
        related_to = attributes.get("related_to")
        data_provider_id = attributes.get("data_provider_id")
        destination_id = attributes.get("destination_id")
        try:
            await cache_image_metadata(
                data=event.payload, gundi_id=gundi_id, destination_id=destination_id
            )
        except Exception as e:
            error_msg = f"Error caching image metadata for {gundi_id} and {destination_id}: {type(e).__name__}: {e}"
            logger.exception(error_msg)
            # Extract additional response details if available
            if (
                response := getattr(e, "response", None)
            ) is not None:  # bool(response) on status errors returns False
                server_response_status = (getattr(response, "status_code", None),)
                server_response_body = str(
                    getattr(response, "text", getattr(response, "content", None)) or ""
                )
            await publish_event(
                event=system_events.ObservationDeliveryFailed(
                    payload=system_events.DeliveryErrorDetails(
                        error=error_msg,
                        error_traceback=traceback.format_exc(),
                        server_response_status=server_response_status,
                        server_response_body=server_response_body,
                        observation=gundi_schemas_v2.DispatchedObservation(
                            gundi_id=gundi_id,
                            related_to=related_to,
                            external_id=None,
                            data_provider_id=data_provider_id,
                            destination_id=destination_id,
                            delivered_at=datetime.now(timezone.utc),  # UTC
                        ),
                    )
                ),
                topic_name=settings.DISPATCHER_EVENTS_TOPIC,
            )
            raise e
        else:
            current_span.set_attribute("is_buffered", True)
            current_span.add_event(
                name="traptagger_dispatcher.transformed_observation_buffered"
            )
            await publish_event(
                event=system_events.DispatcherCustomLog(
                    payload=gundi_schemas_v2.CustomDispatcherLog(
                        gundi_id=gundi_id,
                        related_to=related_to,
                        data_provider_id=data_provider_id,
                        destination_id=destination_id,
                        title=f"Observation {gundi_id} buffered in wait for attachment",
                        level=gundi_schemas_v2.LogLevel.INFO,
                    )
                ),
                topic_name=settings.DISPATCHER_EVENTS_TOPIC,
            )
            return {"status": "buffered"}


async def handle_traptagger_attachment(
    event: AttachmentTransformedTrapTagger, attributes: dict
):
    # Trace observations with Open Telemetry
    with tracing.tracer.start_as_current_span(
        "traptagger_dispatcher.handle_traptagger_attachment", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("payload", repr(event.payload))
        destination_id = attributes.get("destination_id")
        current_span.set_attribute("destination_id", destination_id)
        destination_integration = await get_destination_integration(
            destination_id=destination_id
        )
        # Look for the related event which contains the camera ID
        related_to = attributes.get("related_to")
        current_span.set_attribute("destination_id", destination_id)
        related_event = await get_related_event(
            event_id=related_to, destination_id=destination_id
        )
        # Send image plus metadata to WPS Watch
        return await dispatch_image(
            integration=destination_integration,
            image=event.payload,
            related_event=related_event,
            attributes=attributes,
        )


event_schemas = {
    "EventTransformedTrapTagger": EventTransformedTrapTagger,
    "AttachmentTransformedTrapTagger": AttachmentTransformedTrapTagger,
}

event_handlers = {
    "EventTransformedTrapTagger": handle_traptagger_event,
    "AttachmentTransformedTrapTagger": handle_traptagger_attachment,
}
