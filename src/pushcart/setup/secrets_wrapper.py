import logging
from typing import Dict

from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.secrets.api import SecretApi
from pydantic import Field, constr, dataclasses, validate_arguments, validator

from pushcart.validation.common import (
    PydanticArbitraryTypesConfig,
    validate_databricks_api_client,
)


@dataclasses.dataclass(config=PydanticArbitraryTypesConfig)
class SecretsWrapper:
    client: ApiClient

    @validator("client")
    @classmethod
    def check_api_client(cls, value):
        """
        Validates that the ApiClient object is properly initialized
        """
        return validate_databricks_api_client(value)

    def __post_init_post_parse__(self):
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.INFO)

        self.secrets_api = SecretApi(self.client)

    @validate_arguments
    def create_scope_if_not_exists(
        self,
        secret_scope_name: constr(
            strip_whitespace=True,
            to_lower=True,
            strict=True,
            min_length=1,
            regex=r"^[A-Za-z0-9\-_.]{1,128}$",
        ) = "pushcart",
    ) -> None:
        scopes = self.secrets_api.list_scopes()["scopes"]
        if secret_scope_name not in [scope["name"] for scope in scopes]:
            self.secrets_api.create_scope(
                initial_manage_principal="users",
                scope=secret_scope_name,
                scope_backend_type="DATABRICKS",
                backend_azure_keyvault=None,
            )
            self.log.info(f"Created secret scope {secret_scope_name}")

    @validate_arguments
    def push_secrets(
        self,
        secret_scope_name: constr(
            strip_whitespace=True,
            to_lower=True,
            strict=True,
            min_length=1,
            regex=r"^[A-Za-z0-9\-_.]{1,128}$",
        ) = "pushcart",
        secrets_dict: Dict[
            constr(
                strip_whitespace=True,
                to_lower=True,
                strict=True,
                min_length=1,
                regex=r"^[A-Za-z0-9\-_.]{1,128}$",
            ),
            str,
        ] = Field(default_factory=dict),
    ) -> None:
        if not secrets_dict:
            self.log.warning("No secrets to push to secret scope")
            return None

        self.create_scope_if_not_exists(secret_scope_name)

        for key, value in secrets_dict.items():
            self.secrets_api.put_secret(secret_scope_name, key, value, bytes_value=None)
            self.log.info(f"Put secret '{key}' in '{secret_scope_name}' secret scope.")
