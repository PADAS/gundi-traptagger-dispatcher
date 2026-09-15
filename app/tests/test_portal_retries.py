import pytest
from unittest.mock import AsyncMock, MagicMock

from gundi_client_v2.errors import GundiAPIError

from app.core import gundi


def _mock_gundi_client(side_effect):
    mock_client = MagicMock()
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None
    mock_client.get_integration_details = AsyncMock(side_effect=side_effect)
    return MagicMock(return_value=mock_client), mock_client


@pytest.mark.asyncio
async def test_get_integration_details_retries_on_portal_5xx(
    mocker, mock_redis, destination_integration_v2_traptagger
):
    mocker.patch("app.core.utils.redis_client", mock_redis)
    mocker.patch("asyncio.sleep", new=AsyncMock())
    mock_client_class, mock_client = _mock_gundi_client(
        side_effect=[
            GundiAPIError(503, "upstream"),
            GundiAPIError(503, "upstream"),
            destination_integration_v2_traptagger,
        ]
    )
    mocker.patch("app.core.gundi.GundiClient", mock_client_class)

    result = await gundi.get_integration_details(
        str(destination_integration_v2_traptagger.id)
    )

    assert result == destination_integration_v2_traptagger
    assert mock_client.get_integration_details.call_count == 3


@pytest.mark.asyncio
async def test_get_integration_details_does_not_retry_on_portal_4xx(
    mocker, mock_redis, destination_integration_v2_traptagger
):
    mocker.patch("app.core.utils.redis_client", mock_redis)
    mocker.patch("asyncio.sleep", new=AsyncMock())
    mock_client_class, mock_client = _mock_gundi_client(
        side_effect=GundiAPIError(404, "missing"),
    )
    mocker.patch("app.core.gundi.GundiClient", mock_client_class)

    with pytest.raises(GundiAPIError):
        await gundi.get_integration_details(
            str(destination_integration_v2_traptagger.id)
        )

    assert mock_client.get_integration_details.call_count == 1
