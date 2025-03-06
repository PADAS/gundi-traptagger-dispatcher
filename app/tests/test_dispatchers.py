import respx
import httpx
import pytest
from gundi_core.events import dispatchers as dispatcher_events
from app.core import settings


@pytest.mark.parametrize(
    "mock_traptagger_successful_response",
    [
        "image_added",
        "image_duplicate",
    ],
    indirect=["mock_traptagger_successful_response"],
)
@pytest.mark.asyncio
async def test_dispatch_image_successfully(
    mocker,
    mock_redis,
    mock_cloud_storage_client,
    mock_publish_event,
    mock_traptagger_successful_response,
    event_v2_transformed_traptagger,
    attachment_v2_transformed_traptagger,
    attachment_v2_transformed_traptagger_attributes,
    destination_integration_v2_traptagger,
):
    from app.services.event_handlers import dispatch_image

    # Mock external dependencies
    mocker.patch("app.core.utils.redis_client", mock_redis)
    mocker.patch("app.services.event_handlers.publish_event", mock_publish_event)
    mocker.patch("app.services.dispatchers.redis_client", mock_redis)
    mocker.patch("app.services.dispatchers.gcp_storage", mock_cloud_storage_client)

    # Mock TrapTagger API response
    async with respx.mock(
        base_url=destination_integration_v2_traptagger.base_url,
        assert_all_called=True,
        assert_all_mocked=True,
    ) as respx_mock:
        respx_mock.post("/api/v1/addImage").respond(
            status_code=mock_traptagger_successful_response.status_code,
            content=mock_traptagger_successful_response.text,
        )
        await dispatch_image(
            integration=destination_integration_v2_traptagger,
            image=attachment_v2_transformed_traptagger.payload,
            related_event=event_v2_transformed_traptagger.payload,
            attributes=attachment_v2_transformed_traptagger_attributes,
        )

    # Check that the right event was published to the right pubsub topic to inform other services about the success
    assert mock_publish_event.called
    assert mock_publish_event.call_count == 1
    call = mock_publish_event.call_args_list[0]
    assert call.kwargs["topic_name"] == settings.DISPATCHER_EVENTS_TOPIC
    published_event = call.kwargs["event"]
    assert isinstance(published_event, dispatcher_events.ObservationDelivered)
    payload = published_event.payload
    assert isinstance(payload, dispatcher_events.DispatchedObservation)
    # The id returned by the TrapTagger API should be saved in the external_id field
    response_data = mock_traptagger_successful_response.json()
    assert payload.external_id == str(response_data.get("image_id"))


@pytest.mark.parametrize(
    "mock_traptagger_error_response",
    [
        "bad_credentials",
        "bad_request",
        "service_unavailable",
        "internal_error",
    ],
    indirect=["mock_traptagger_error_response"],
)
@pytest.mark.asyncio
async def test_dispatch_image_on_errors(
    mock_traptagger_error_response,
    mocker,
    mock_redis,
    mock_cloud_storage_client,
    mock_publish_event,
    event_v2_transformed_traptagger,
    attachment_v2_transformed_traptagger,
    attachment_v2_transformed_traptagger_attributes,
    destination_integration_v2_traptagger,
):
    from app.services.event_handlers import dispatch_image

    # Mock external dependencies
    mocker.patch("app.core.utils.redis_client", mock_redis)
    mocker.patch("app.services.event_handlers.publish_event", mock_publish_event)
    mocker.patch("app.services.dispatchers.redis_client", mock_redis)
    mocker.patch("app.services.dispatchers.gcp_storage", mock_cloud_storage_client)

    # Mock TrapTagger API response
    async with respx.mock(
        base_url=destination_integration_v2_traptagger.base_url,
        assert_all_called=True,
        assert_all_mocked=True,
    ) as respx_mock:
        respx_mock.post("/api/v1/addImage").respond(
            status_code=mock_traptagger_error_response.status_code,
            content=mock_traptagger_error_response.text,
        )
        with pytest.raises(Exception):
            await dispatch_image(
                integration=destination_integration_v2_traptagger,
                image=attachment_v2_transformed_traptagger.payload,
                related_event=event_v2_transformed_traptagger.payload,
                attributes=attachment_v2_transformed_traptagger_attributes,
            )

    # Check that the right event was published to the right pubsub topic to inform other services about the error
    assert mock_publish_event.called
    assert mock_publish_event.call_count == 1
    call = mock_publish_event.call_args_list[0]
    assert call.kwargs["topic_name"] == settings.DISPATCHER_EVENTS_TOPIC
    published_event = call.kwargs["event"]
    assert isinstance(published_event, dispatcher_events.ObservationDeliveryFailed)
    assert published_event.event_type == "ObservationDeliveryFailed"
    assert published_event.schema_version == "v2"
    payload = published_event.payload
    assert isinstance(payload, dispatcher_events.DeliveryErrorDetails)
    assert payload.error_traceback
    assert payload.error
    assert payload.server_response_status == mock_traptagger_error_response.status_code
    assert payload.server_response_body == mock_traptagger_error_response.text
