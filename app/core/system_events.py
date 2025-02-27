import asyncio
import json
import aiohttp
import logging
import backoff
from gundi_core.events import SystemEventBaseModel
from gcloud.aio import pubsub
from . import settings


logger = logging.getLogger(__name__)


# Events for other services or system components
@backoff.on_exception(
    backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_tries=5
)
async def publish_event(event: SystemEventBaseModel, topic_name: str):
    timeout_settings = aiohttp.ClientTimeout(total=10.0)
    async with aiohttp.ClientSession(
        raise_for_status=True, timeout=timeout_settings
    ) as session:
        client = pubsub.PublisherClient(session=session)
        # Get the topic
        topic = client.topic_path(settings.GCP_PROJECT_ID, topic_name)
        # Prepare the payload
        binary_payload = json.dumps(event.dict(), default=str).encode("utf-8")
        messages = [pubsub.PubsubMessage(binary_payload)]
        logger.debug(f"Sending event {event} to PubSub topic {topic_name}..")
        try:  # Send to pubsub
            response = await client.publish(topic, messages)
        except Exception as e:
            logger.exception(
                f"Error publishing system event topic {topic_name}: {e}. This will be retried."
            )
            raise e
        else:
            logger.debug(f"System event {event} published successfully.")
            logger.debug(f"GCP PubSub response: {response}")
