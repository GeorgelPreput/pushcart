import logging
from typing import Optional

import keyring
from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import DatabricksConfig
from databricks_cli.dbfs.api import DbfsApi
from databricks_cli.dbfs.dbfs_path import DbfsPath
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.secrets.api import SecretApi
from pydantic import HttpUrl, constr, dataclasses

from pushcart.validation.common import PydanticArbitraryTypesConfig


@dataclasses.dataclass(config=PydanticArbitraryTypesConfig)
class DBUtilsSecrets:
    client: ApiClient

    def __post_init_post_parse__(self):
        self.sa = SecretApi(self.client)

    def get(self, scope, key):
        return keyring.get_password(service_name=scope, username=key)

    def listScopes(self) -> list:
        return self.sa.list_scopes()

    def list_secrets(self, scope: str) -> list:
        return self.sa.list_secrets(scope)


@dataclasses.dataclass(config=PydanticArbitraryTypesConfig)
class DBUtilsFileSystem:
    client: ApiClient

    def __post_init_post_parse__(self):
        self.da = DbfsApi(self.client)

    def ls(self, path: str) -> list:
        if not path.startswith("dbfs:/"):
            path = "dbfs:/" + path.lstrip("/")

        dbfs_path = DbfsPath(path)

        file_list = self.da.list_files(dbfs_path)

        return [f.dbfs_path for f in file_list]


@dataclasses.dataclass
class DBUtils:
    workspace: Optional[HttpUrl] = keyring.get_password("pushcart", "host")
    token: Optional[
        constr(min_length=1, strict=True, regex=r"^[^'\"]*$")
    ] = keyring.get_password("pushcart", "token")

    def __post_init_post_parse__(self):
        self.log = logging.getLogger(__name__)
        self.log.warning(f"Running locally, secrets will default to the system keyring")

        self.config = DatabricksConfig.from_token(self.workspace, self.token, False)
        self.client = _get_api_client(self.config)

        self.secrets = DBUtilsSecrets(client=self.client)
        self.fs = DBUtilsFileSystem(client=self.client)
