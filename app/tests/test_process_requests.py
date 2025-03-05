import base64
import json
import pytest
import respx
import httpx
from fastapi.testclient import TestClient
from gundi_core import schemas
from app.core import settings
from app.core.utils import find_config_for_action


@pytest.mark.asyncio
async def test_process_event_v2_successfully(
    mocker,
    mock_redis,
    mock_gundi_client_v2_class,
    mock_cloud_storage_client,
    mock_pubsub_client,
    event_v2_as_pubsub_request,
    destination_integration_v2_traptagger,
    trap_tagger_api_success_response,
):

    # Mock external dependencies
    mocker.patch("app.core.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.core.utils.redis_client", mock_redis)
    mocker.patch("app.core.system_events.pubsub", mock_pubsub_client)
    mocker.patch("app.services.event_handlers._cache_db", mock_redis)
    mocker.patch("app.services.dispatchers.redis_client", mock_redis)
    mocker.patch("app.services.dispatchers.gcp_storage", mock_cloud_storage_client)
    mocker.patch("app.services.process_messages.pubsub", mock_pubsub_client)
    async with respx.mock(
        base_url=destination_integration_v2_traptagger.base_url,
        assert_all_called=False,
        assert_all_mocked=True,
    ) as respx_mock:
        # Mock the TrapTagger API response
        route = respx_mock.post(f"api/v1/addImage", name="upload_file").respond(
            httpx.codes.OK,
            json=trap_tagger_api_success_response,
        )
        from app.main import app

        with TestClient(
            app
        ) as api_client:  # Use as context manager to trigger lifespan hooks
            response = api_client.post(
                "/",
                headers=event_v2_as_pubsub_request.headers,
                json=event_v2_as_pubsub_request.get_json(),
            )
            assert response.status_code == 200

        # Check that the event was cached until receiving the attachment
        event_json = event_v2_as_pubsub_request.get_json()
        gundi_id = event_json["message"]["attributes"]["gundi_id"]
        destination_id = event_json["message"]["attributes"]["destination_id"]
        event_data = event_json["message"]["data"]
        decoded_event_data = json.loads(base64.b64decode(event_data))
        serialized_payload = json.dumps(decoded_event_data["payload"], default=str)
        mock_redis.setex.assert_called_with(
            name=f"traptagger_image_metadata.{gundi_id}.{destination_id}",
            time=settings.IMAGE_METADATA_CACHE_TTL,
            value=serialized_payload,
        )
        # Check that the traptagger api was Not called
        assert not route.called


@pytest.mark.asyncio
async def test_process_attachment_v2_successfully(
    mocker,
    mock_redis,
    mock_redis_with_cached_event,
    mock_gundi_client_v2_class,
    mock_cloud_storage_client,
    mock_pubsub_client,
    event_v2_as_pubsub_request,
    attachment_v2_as_pubsub_request,
    attachment_file_blob,
    destination_integration_v2_traptagger,
    trap_tagger_api_success_response,
):
    # Mock external dependencies
    mocker.patch("app.core.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.core.utils.redis_client", mock_redis)
    mocker.patch("app.core.system_events.pubsub", mock_pubsub_client)
    mocker.patch("app.services.event_handlers._cache_db", mock_redis_with_cached_event)
    mocker.patch("app.services.dispatchers.redis_client", mock_redis)
    mocker.patch("app.services.dispatchers.gcp_storage", mock_cloud_storage_client)
    mocker.patch("app.services.process_messages.pubsub", mock_pubsub_client)
    # Mock the TrapTagger API
    async with respx.mock(
        base_url=destination_integration_v2_traptagger.base_url,
        assert_all_called=True,
        assert_all_mocked=True,
    ) as respx_mock:
        # Define the expected request:

        # Camera ID is taken from the related event source id
        event_json = event_v2_as_pubsub_request.get_json()
        event_data = event_json["message"]["data"]
        decoded_event_data = json.loads(base64.b64decode(event_data))

        # Upload domain is stored in a config
        configurations = destination_integration_v2_traptagger.configurations
        expected_data = decoded_event_data["payload"]
        # API Key is stored in a config
        auth_config = find_config_for_action(
            configurations=configurations,
            action_value=schemas.v2.TrapTaggerActions.AUTHENTICATE.value,
        )
        api_key = auth_config.data.get("api_key")
        expected_headers = {"apikey": api_key}

        expected_files = {
            "image": (
                "259a2767-d7a0-462e-a881-57bf746f53d1_elephant_single_20250305.jpg",
                attachment_file_blob,
            )
        }

        # Patch the upload endpoint
        route = respx_mock.post(  # Check that the TrapTagger endpoint was called with the right parameters
            "api/v1/addImage",
            headers=expected_headers,
            data=expected_data,
            files=expected_files,
        ).respond(
            httpx.codes.OK,
            json=trap_tagger_api_success_response,
        )
        from app.main import app

        with TestClient(
            app
        ) as api_client:  # Use as context manager to trigger lifespan hooks
            response = api_client.post(
                "/",
                headers=attachment_v2_as_pubsub_request.headers,
                json=attachment_v2_as_pubsub_request.get_json(),
            )
            assert response.status_code == 200
            assert route.called
