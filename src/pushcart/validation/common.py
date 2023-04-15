from databricks_cli.sdk.api_client import ApiClient


def validate_databricks_api_client(client: ApiClient = None) -> ApiClient:
    """
    Validate the input parameter 'client' of type 'ApiClient' and ensure that it has
    been properly initialized before returning it.
    """
    if not client:
        raise ValueError("ApiClient must have a value")

    if not isinstance(client, ApiClient):
        raise TypeError(
            "Client must be of type databricks_cli.sdk.api_client.ApiClient"
        )

    if not client.url or not client.default_headers:
        raise ValueError("ApiClient has not been properly initialized")

    return client


class PydanticArbitraryTypesConfig:
    arbitrary_types_allowed = True
