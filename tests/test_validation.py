import pytest
from databricks_cli.sdk.api_client import ApiClient

from pushcart.validation.common import validate_databricks_api_client


class TestValidateDatabricksApiClient:
    def test_happy_path(self, mocker):
        """
        Tests that the function returns the input client if it passes validation.
        """
        client = mocker.Mock(ApiClient)
        client.url = "https://example.com"
        client.default_headers = {"Authorization": "Bearer token"}

        result = validate_databricks_api_client(client)

        assert result == client

    def test_edge_case_client_is_none(self):
        """
        Tests that the function raises a ValueError if the input client is None.
        """
        client = None

        with pytest.raises(ValueError):
            validate_databricks_api_client(client)

    def test_edge_case_invalid_url_or_default_headers(self, mocker):
        """
        Tests that the function raises a ValueError if the input client has an invalid
        url or default_headers.
        """
        client = mocker.Mock(ApiClient)
        client.url = ""
        client.default_headers = {}

        with pytest.raises(ValueError):
            validate_databricks_api_client(client)

    def test_edge_case_client_not_instance_of_ApiClient(self, mocker):
        """
        Tests that the function raises a TypeError if the input client is not an
        instance of ApiClient.
        """
        client = mocker.Mock()

        with pytest.raises(TypeError):
            validate_databricks_api_client(client)
