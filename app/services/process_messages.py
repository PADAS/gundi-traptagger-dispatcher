import json
import logging
import aiohttp
from datetime import datetime, timezone
from gcloud.aio import pubsub
from gundi_core.schemas.v2 import StreamPrefixEnum
from opentelemetry.trace import SpanKind
from app.core import settings
from app.core.utils import (
    extract_fields_from_message,
)

from app.core import tracing
from . import dispatchers
from .event_handlers import event_handlers, event_schemas


logger = logging.getLogger(__name__)


def get_dlq_topic_for_data_type(data_type: StreamPrefixEnum) -> str:
    if data_type == StreamPrefixEnum.observation:
        return settings.OBSERVATIONS_DEAD_LETTER_TOPIC
    elif data_type == StreamPrefixEnum.event:
        return settings.EVENTS_DEAD_LETTER_TOPIC
    elif data_type == StreamPrefixEnum.event_update:
        return settings.EVENTS_UPDATES_DEAD_LETTER_TOPIC
    elif data_type == StreamPrefixEnum.attachment:
        return settings.ATTACHMENTS_DEAD_LETTER_TOPIC
    elif data_type == StreamPrefixEnum.text_message:
        return settings.TEXT_MESSAGES_DEAD_LETTER_TOPIC
    else:
        return settings.LEGACY_DEAD_LETTER_TOPIC


async def send_observation_to_dead_letter_topic(transformed_observation, attributes):
    with tracing.tracer.start_as_current_span(
        "send_message_to_dead_letter_topic", kind=SpanKind.CLIENT
    ) as current_span:

        print(f"Forwarding observation to dead letter topic: {transformed_observation}")
        # Publish to another PubSub topic
        connect_timeout, read_timeout = settings.DEFAULT_REQUESTS_TIMEOUT
        timeout_settings = aiohttp.ClientTimeout(
            sock_connect=connect_timeout, sock_read=read_timeout
        )
        async with aiohttp.ClientSession(
            raise_for_status=True, timeout=timeout_settings
        ) as session:
            client = pubsub.PublisherClient(session=session)
            # Get the topic
            if attributes.get("gundi_version", "v1") == "v2":
                topic_name = get_dlq_topic_for_data_type(
                    data_type=attributes.get("stream_type")
                )
            else:
                topic_name = settings.LEGACY_DEAD_LETTER_TOPIC
            current_span.set_attribute("topic", topic_name)
            topic = client.topic_path(settings.GCP_PROJECT_ID, topic_name)
            # Prepare the payload
            binary_payload = json.dumps(transformed_observation, default=str).encode(
                "utf-8"
            )
            messages = [pubsub.PubsubMessage(binary_payload, **attributes)]
            logger.info(f"Sending observation to PubSub topic {topic_name}..")
            try:  # Send to pubsub
                response = await client.publish(topic, messages)
            except Exception as e:
                logger.exception(
                    f"Error sending observation to dead letter topic {topic_name}: {e}. Please check if the topic exists or review settings."
                )
                raise e
            else:
                logger.info(f"Observation sent to the dead letter topic successfully.")
                logger.debug(f"GCP PubSub response: {response}")

        current_span.set_attribute("is_sent_to_dead_letter_queue", True)
        current_span.add_event(
            name="routing_service.observation_sent_to_dead_letter_queue"
        )


def is_too_old(timestamp):
    if not timestamp:
        return False
    try:  # The timestamp does not always include the microseconds part
        event_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        event_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    event_time = event_time.replace(tzinfo=timezone.utc)
    current_time = datetime.now(timezone.utc)
    # Notice: We have seen cloud events with future timestamps. Don't use .seconds
    event_age_seconds = (current_time - event_time).total_seconds()
    return event_age_seconds > settings.MAX_EVENT_AGE_SECONDS


async def process_transformer_event_v2(raw_event, attributes):
    with tracing.tracer.start_as_current_span(
        "traptagger_dispatcher.process_transformer_event_v2", kind=SpanKind.CLIENT
    ) as current_span:
        current_span.add_event(
            name="traptagger_dispatcher.transformed_observation_received_at_dispatcher"
        )
        current_span.set_attribute("transformed_message", str(raw_event))
        current_span.set_attribute("environment", settings.TRACE_ENVIRONMENT)
        current_span.set_attribute("service", "tt-dispatcher")
        logger.debug(
            f"Message received: \npayload: {raw_event} \nattributes: {attributes}"
        )
        if schema_version := raw_event.get("schema_version") != "v1":
            logger.warning(
                f"Schema version '{schema_version}' not supported. Message discarded."
            )
            return
        event_type = raw_event.get("event_type")
        current_span.set_attribute("event_type", str(event_type))
        try:
            handler = event_handlers[event_type]
        except KeyError:
            logger.warning(f"Event of type '{event_type}' unknown. Ignored.")
            current_span.add_event(
                name="traptagger_dispatcher.discarded_transformer_event_with_invalid_type"
            )
            return
        try:
            schema = event_schemas[event_type]
        except KeyError:
            logger.warning(
                f"Event Schema for '{event_type}' not found. Message discarded."
            )
            return {}
        parsed_event = schema.parse_obj(raw_event)
        return await handler(event=parsed_event, attributes=attributes)


async def process_request(request):
    # Extract the observation and attributes from the CloudEvent
    json_data = await request.json()
    pubsub_message = json_data["message"]
    transformed_observation, attributes = extract_fields_from_message(pubsub_message)
    # Load tracing context
    tracing.pubsub_instrumentation.load_context_from_attributes(attributes)
    with tracing.tracer.start_as_current_span(
        "traptagger_dispatcher.process_request", kind=SpanKind.CLIENT
    ) as current_span:
        timestamp = request.headers.get("ce-time") or pubsub_message.get("publish_time")
        if is_too_old(timestamp=timestamp):
            logger.warning(
                f"Message discarded (timestamp = {timestamp}). The message is too old or the retry time limit has been reached."
            )
            current_span.set_attribute("is_too_old", True)
            await send_observation_to_dead_letter_topic(
                transformed_observation, attributes
            )
            return {
                "status": "discarded",
                "reason": "Message is too old or the retry time limit has been reach",
            }
        if version := attributes.get("gundi_version", "v1") == "v2":
            await process_transformer_event_v2(transformed_observation, attributes)
        else:
            logger.warning(
                f"Message discarded. Version '{version}' is not supported by this dispatcher."
            )
            await send_observation_to_dead_letter_topic(
                transformed_observation, attributes
            )
            return {
                "status": "discarded",
                "reason": f"Gundi '{version}' messages are not supported",
            }
        return {"status": "processed"}
