import keyring
from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import DatabricksConfig
from databricks_cli.dbfs.api import DbfsApi
from databricks_cli.dbfs.dbfs_path import DbfsPath
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.secrets.api import SecretApi
from traitlets import HasTraits, Instance, Unicode
from traitlets.config import Application


class DBUtilsSecrets(HasTraits):
    client = Instance(klass=ApiClient)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.sa = SecretApi(self.client)

    def get(self, scope, key):
        return keyring.get_password(service_name=scope, username=key)

    def listScopes(self) -> list:
        return self.sa.list_scopes()

    def list_secrets(self, scope: str) -> list:
        return self.sa.list_secrets(scope)


class DBUtilsFileSystem(HasTraits):
    client = Instance(klass=ApiClient)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.da = DbfsApi(self.client)

    def ls(self, path: str) -> list:
        if not path.startswith("dbfs:/"):
            path = "dbfs:/" + path.lstrip("/")

        dbfs_path = DbfsPath(path)

        file_list = self.da.list_files(dbfs_path)

        return [f.dbfs_path for f in file_list]


class DBUtils(HasTraits):
    host = Unicode(default_value=keyring.get_password("pushcart", "host"))
    token = Unicode(default_value=keyring.get_password("pushcart", "token"))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.log = Application.instance().log
        self.log.warning(f"Running locally, secrets will default to the system keyring")

        self.config = DatabricksConfig.from_token(self.host, self.token, False)
        self.client = _get_api_client(self.config)

        self.secrets = DBUtilsSecrets(client=self.client)
        self.fs = DBUtilsFileSystem(client=self.client)
