import mimetypes
import os
import httpx
import logging
from app.core import settings
from urllib.parse import urlparse
from gundi_core import schemas
from gcloud.aio.storage import Storage
from app.core.utils import RateLimiterSemaphore, redis_client, find_config_for_action


logger = logging.getLogger(__name__)

gcp_storage = Storage()


class TrapTaggerImageDispatcher:
    def __init__(self, integration):
        self.integration = integration

    async def _traptagger_post(self, request_data, file_name, file_data):
        # Look for the configuration of the authentication action
        configurations = self.integration.configurations
        integration_action_config = find_config_for_action(
            configurations=configurations,
            action_value=schemas.v2.TrapTaggerActions.AUTHENTICATE.value,
        )
        if not integration_action_config:
            raise ValueError(
                f"Authentication settings for integration {str(self.integration.id)} are missing. Please fix the integration setup in the portal."
            )
        api_key = integration_action_config.data.get("api_key")
        if not api_key:
            raise ValueError(
                f"Token for integration {str(self.integration.id)} is missing. Please fix the integration setup in the portal."
            )
        headers = {"apikey": api_key}
        files = {"image": (file_name, file_data)}
        parsed_url = urlparse(self.integration.base_url)
        sanitized_endpoint = (
            f"{parsed_url.scheme}://{parsed_url.hostname}/api/v1/addImage"
        )
        try:
            logger.debug(
                f"Posting to TrapTagger endpoint {sanitized_endpoint} with data {request_data}..."
            )
            connect_timeout, read_timeout = settings.DEFAULT_REQUESTS_TIMEOUT
            timeout_settings = httpx.Timeout(read_timeout, connect=connect_timeout)
            async with httpx.AsyncClient(timeout=timeout_settings) as client:
                response = await client.post(
                    sanitized_endpoint,
                    data=request_data,
                    headers=headers,
                    files=files,
                )
            response.raise_for_status()
        except httpx.HTTPError as e:
            logger.exception(
                f"Error occurred posting to TrapTagger endpoint {sanitized_endpoint} \n {type(e).__name__}: {e}"
            )
            raise e  # Raise so it's retried
        return response.json()

    async def send(self, image: schemas.v2.TrapTaggerImage, **kwargs):
        related_event = kwargs.get("related_event")
        if not related_event:
            raise ValueError("related_observation is required")

        try:  # Download the Image from GCP
            file_path = image.file_path
            downloaded_file = await gcp_storage.download(
                bucket=settings.BUCKET_NAME, object_name=file_path
            )
        except Exception as e:
            logger.exception(
                f"Error downloading file '{file_path}' from cloud storage: {type(e).__name__}: {e}"
            )
            raise e

        try:  # Send the image to TrapTagger
            async with RateLimiterSemaphore(
                redis_client=redis_client, url=str(self.integration.base_url)
            ):
                image_metadata = related_event.dict()  # Cam ID, lat/lon, etc...
                file_name = os.path.basename(file_path)
                result = await self._traptagger_post(
                    request_data=image_metadata,
                    file_name=file_name,
                    file_data=downloaded_file,
                )
        except Exception as e:
            logger.exception(
                f"Error sending data to TrapTagger {type(e).__name__}: {e}"
            )
            raise e
        else:
            logger.info(f"File {file_path} delivered to TrapTagger with success.")
            # Remove the file from GCP after delivering it to TrapTagger
            if settings.DELETE_FILES_AFTER_DELIVERY:
                await gcp_storage.delete(
                    bucket=settings.BUCKET_NAME, object_name=file_path
                )
                logger.debug(f"File {file_path} deleted from GCP.")
            return result
