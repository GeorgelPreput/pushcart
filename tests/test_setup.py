import json
from pathlib import Path

import pytest
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.jobs.api import JobsApi
from databricks_cli.repos.api import ReposApi
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.secrets.api import SecretApi
from pydantic.error_wrappers import ValidationError

from pushcart.setup.job_settings import (
    JobSettings,
    _get_existing_cluster_id,
    _get_newest_spark_version,
    _get_smallest_cluster_node_type,
)
from pushcart.setup.jobs_wrapper import JobsWrapper
from pushcart.setup.repos_wrapper import ReposWrapper
from pushcart.setup.secrets_wrapper import SecretsWrapper
from pushcart.validation.common import validate_databricks_api_client


@pytest.fixture(autouse=True)
def mock_api_client():
    api_client = ApiClient(host="https://databricks.sample.com")

    yield api_client


class TestReposWrapper:
    def test_create_repo_success(self, mocker, mock_api_client):
        """
        Tests that the get_or_create_repo method successfully creates a new repository.
        """
        mocker.patch.object(ReposApi, "__init__", return_value=None)
        mocker.patch.object(ReposApi, "get_repo_id", return_value=None)
        mocker.patch.object(ReposApi, "create", return_value={"id": "456"})

        repo_id = ReposWrapper(mock_api_client).get_or_create_repo(
            repo_user="test_user",
            git_url="https://github.com/test_user/test_repo.git",
        )
        assert repo_id == "456"

    def test_get_existing_repo_success(self, mocker, mock_api_client):
        """
        Tests that the get_or_create_repo method successfully retrieves an existing repository.
        """
        mocker.patch.object(ReposApi, "__init__", return_value=None)
        mocker.patch.object(ReposApi, "get_repo_id", return_value="123")

        repo_id = ReposWrapper(mock_api_client).get_or_create_repo(
            repo_user="test_user",
            git_url="https://github.com/test_user/test_repo.git",
        )
        assert repo_id == "123"

    def test_update_invalid_repo(self, mock_api_client):
        """
        Tests that the update method raises a ValueError if attempting to update
        repository before initializing with get_or_create_repo.
        """
        with pytest.raises(ValueError):
            ReposWrapper(mock_api_client).update(git_branch="main")

    def test_detect_git_provider_warning(self, mock_api_client):
        """
        Tests that the detect_git_provider method raises ValueError if git provider
        is not specified and cannot be detected from url.
        """
        with pytest.raises(ValueError) as e:
            ReposWrapper(mock_api_client)._detect_git_provider("https://example.com")

        assert (
            "Could not detect Git provider from URL. Please specify git_provider explicitly."
            in str(e.value)
        )

    def test_detect_git_provider_success(self, mock_api_client):
        """
        Tests that the detect_git_provider method successfully detects the correct git
        provider from a given url.
        """
        repos_wrapper = ReposWrapper(mock_api_client)
        git_provider = repos_wrapper._detect_git_provider(
            "https://github.com/user/repo.git"
        )
        assert git_provider == "gitHub"

    def test_get_or_create_repo_invalid_input(self, mock_api_client):
        """
        Tests that the get_or_create_repo method raises a ValueError if invalid input
        parameters are passed.
        """
        repos_wrapper = ReposWrapper(mock_api_client)
        with pytest.raises(ValueError):
            repos_wrapper.get_or_create_repo("", "https://github.com/user/repo.git")

    def test_update_invalid_input(self, mock_api_client):
        """
        Tests that the update method raises a ValueError if invalid input parameters
        are passed.
        """
        repos_wrapper = ReposWrapper(mock_api_client)
        with pytest.raises(ValueError):
            repos_wrapper.update("")


class TestGetSmallestClusterNodeType:
    #
    def test_get_smallest_cluster_node_type(self, mocker, mock_api_client):
        """
        Tests that the function returns the smallest cluster node type when given
        multiple node types with different specifications, including some that
        should be filtered out
        """
        node_types = [
            {
                "node_type_id": "1",
                "num_cores": 4,
                "memory_mb": 4096,
                "num_gpus": 1,
                "is_deprecated": False,
                "is_hidden": False,
                "photon_driver_capable": True,
                "photon_worker_capable": True,
            },
            {
                "node_type_id": "2",
                "num_cores": 8,
                "memory_mb": 8192,
                "num_gpus": 2,
                "is_deprecated": False,
                "is_hidden": False,
                "photon_driver_capable": True,
                "photon_worker_capable": True,
            },
            {
                "node_type_id": "3",
                "num_cores": 2,
                "memory_mb": 2048,
                "num_gpus": 1,
                "is_deprecated": False,
                "is_hidden": False,
                "photon_driver_capable": True,
                "photon_worker_capable": True,
            },
            {
                "node_type_id": "4",
                "num_cores": 2,
                "memory_mb": 2048,
                "num_gpus": 1,
                "is_deprecated": True,
                "is_hidden": False,
                "photon_driver_capable": True,
                "photon_worker_capable": True,
            },
            {
                "node_type_id": "5",
                "num_cores": 1,
                "memory_mb": 1024,
                "num_gpus": 0,
                "is_deprecated": False,
                "is_hidden": True,
                "photon_driver_capable": True,
                "photon_worker_capable": True,
            },
            {
                "node_type_id": "6",
                "num_cores": 1,
                "memory_mb": 1024,
                "num_gpus": 0,
                "is_deprecated": False,
                "is_hidden": False,
                "photon_driver_capable": False,
                "photon_worker_capable": True,
            },
        ]

        mocker.patch.object(ClusterApi, "__init__", return_value=None)
        mocker.patch.object(
            ClusterApi, "list_node_types", return_value={"node_types": node_types}
        )

        result = _get_smallest_cluster_node_type(mock_api_client)

        assert result == "3"

    #
    def test_get_smallest_cluster_node_type_with_multiple_same_specs(
        self, mocker, mock_api_client
    ):
        """
        Tests the behavior of the function when multiple node types have the same
        specifications.
        """
        node_types = [
            {
                "node_type_id": "1",
                "num_cores": 2,
                "memory_mb": 4096,
                "num_gpus": 0,
                "is_deprecated": False,
                "is_hidden": False,
                "photon_driver_capable": True,
                "photon_worker_capable": True,
            },
            {
                "node_type_id": "2",
                "num_cores": 4,
                "memory_mb": 8192,
                "num_gpus": 1,
                "is_deprecated": False,
                "is_hidden": False,
                "photon_driver_capable": True,
                "photon_worker_capable": True,
            },
            {
                "node_type_id": "3",
                "num_cores": 1,
                "memory_mb": 2048,
                "num_gpus": 0,
                "is_deprecated": False,
                "is_hidden": False,
                "photon_driver_capable": True,
                "photon_worker_capable": True,
            },
            {
                "node_type_id": "4",
                "num_cores": 1,
                "memory_mb": 2048,
                "num_gpus": 0,
                "is_deprecated": False,
                "is_hidden": False,
                "photon_driver_capable": True,
                "photon_worker_capable": True,
            },
        ]
        mocker.patch.object(ClusterApi, "__init__", return_value=None)
        mocker.patch.object(
            ClusterApi, "list_node_types", return_value={"node_types": node_types}
        )

        result = _get_smallest_cluster_node_type(mock_api_client)

        assert result == "3"

    def test_caching_behavior_get_smallest_cluster_node_type(
        self, mocker, mock_api_client
    ):
        """
        Tests that the function has caching behavior.
        """
        mocker.patch.object(ClusterApi, "__init__", return_value=None)

        node_types = [
            {
                "node_type_id": "type1",
                "num_cores": 2,
                "memory_mb": 8192,
                "num_gpus": 0,
                "is_deprecated": False,
                "is_hidden": False,
                "photon_driver_capable": True,
                "photon_worker_capable": True,
            }
        ]
        mocker.patch.object(
            ClusterApi, "list_node_types", return_value={"node_types": node_types}
        )

        first_result = _get_smallest_cluster_node_type(mock_api_client)

        node_types = [
            {
                "node_type_id": "type1",
                "num_cores": 4,
                "memory_mb": 8192,
                "num_gpus": 1,
                "is_deprecated": False,
                "is_hidden": False,
                "photon_driver_capable": True,
                "photon_worker_capable": True,
            }
        ]
        mocker.patch.object(
            ClusterApi, "list_node_types", return_value={"node_types": node_types}
        )

        second_result = _get_smallest_cluster_node_type(mock_api_client)
        assert first_result == second_result


class TestGetNewestSparkVersion:
    def test_get_newest_spark_version_when_only_one_version_available(
        self, mocker, mock_api_client
    ):
        """
        Tests that the function returns the newest Spark version when there are
        multiple versions available, some not containing "LTS", or containing "ML".
        """
        mocker.patch.object(ClusterApi, "__init__", return_value=None)
        mocker.patch.object(
            ClusterApi,
            "spark_versions",
            return_value={
                "versions": [
                    {
                        "key": "10.4.x-aarch64-scala2.12",
                        "name": "10.4 LTS aarch64 (includes Apache Spark 3.2.1, Scala 2.12)",
                    },
                    {
                        "key": "12.2.x-aarch64-scala2.12",
                        "name": "12.2 LTS aarch64 (includes Apache Spark 3.3.2, Scala 2.12)",
                    },
                    {
                        "key": "12.2.x-gpu-ml-scala2.12",
                        "name": "12.2 LTS ML (includes Apache Spark 3.3.2, GPU, Scala 2.12)",
                    },
                    {
                        "key": "13.0.x-scala2.12",
                        "name": "13.0 (includes Apache Spark 3.4.0, Scala 2.12)",
                    },
                ]
            },
        )
        assert _get_newest_spark_version(mock_api_client) == "12.2.x-aarch64-scala2.12"

    def test_get_newest_spark_version_returns_empty_string_when_no_versions_available(
        self, mocker, mock_api_client
    ):
        """
        Tests that the function returns None when there are no Spark versions available
        """
        mocker.patch.object(ClusterApi, "__init__", return_value=None)
        mocker.patch.object(
            ClusterApi,
            "spark_versions",
            return_value={"versions": []},
        )
        assert _get_newest_spark_version(mock_api_client) == None

    def test_caching_behavior_get_newest_spark_version(self, mocker, mock_api_client):
        """
        Tests that the function has caching behavior.
        """
        mocker.patch.object(ClusterApi, "__init__", return_value=None)

        versions = [
            {
                "key": "12.2.x-aarch64-scala2.12",
                "name": "12.2 LTS aarch64 (includes Apache Spark 3.3.2, Scala 2.12)",
            },
        ]
        mocker.patch.object(
            ClusterApi, "spark_versions", return_value={"versions": versions}
        )

        first_result = _get_newest_spark_version(mock_api_client)

        versions = [
            {
                "key": "10.4.x-aarch64-scala2.12",
                "name": "10.4 LTS aarch64 (includes Apache Spark 3.2.1, Scala 2.12)",
            },
        ]
        mocker.patch.object(
            ClusterApi, "spark_versions", return_value={"versions": versions}
        )

        second_result = _get_newest_spark_version(mock_api_client)
        assert first_result == second_result


class TestGetExistingClusterId:
    def test_valid_input_returns_cluster_id(self, mocker, mock_api_client):
        """
        Tests that providing a valid client and cluster name returns the cluster ID.
        """
        mocker.patch.object(ClusterApi, "__init__", return_value=None)
        mocker.patch.object(
            ClusterApi,
            "list_clusters",
            return_value={
                "clusters": [{"cluster_name": "test_cluster", "cluster_id": "12345"}]
            },
        )

        result = _get_existing_cluster_id(mock_api_client, "test_cluster")

        assert result == "12345"

    def test_invalid_input_raises_exception(self, mocker, mock_api_client):
        """
        Tests that providing an invalid cluster name raises an exception.
        """
        mocker.patch.object(ClusterApi, "__init__", return_value=None)
        mocker.patch.object(
            ClusterApi,
            "list_clusters",
            return_value={
                "clusters": [{"cluster_name": "test_cluster", "cluster_id": "12345"}]
            },
        )

        with pytest.raises(ValidationError):
            _get_existing_cluster_id(client=mock_api_client, cluster_name="")

        with pytest.raises(ValidationError):
            _get_existing_cluster_id(client=mock_api_client, cluster_name=123)

    def test_nonexistent_cluster_returns_none(self, mocker, mock_api_client):
        """
        Tests that providing a non-existent cluster name returns None.
        """
        mocker.patch.object(ClusterApi, "__init__", return_value=None)
        mocker.patch.object(
            ClusterApi,
            "list_clusters",
            return_value={
                "clusters": [{"cluster_name": "test_cluster", "cluster_id": "12345"}]
            },
        )

        result = _get_existing_cluster_id(mock_api_client, "nonexistent_cluster")

        assert result is None

    def test_caching_behavior_get_existing_cluster_id(self, mocker, mock_api_client):
        """
        Tests that the function has caching behavior.
        """
        mocker.patch.object(ClusterApi, "__init__", return_value=None)

        clusters = [{"cluster_name": "first_cluster", "cluster_id": "12345"}]
        mocker.patch.object(
            ClusterApi, "list_clusters", return_value={"versions": clusters}
        )

        first_result = _get_existing_cluster_id(mock_api_client, "first_cluster")

        clusters = [{"cluster_name": "second_cluster", "cluster_id": "67890"}]
        mocker.patch.object(
            ClusterApi, "list_clusters", return_value={"versions": clusters}
        )

        second_result = _get_existing_cluster_id(mock_api_client, "first_cluster")
        assert first_result == second_result


class TestJobSettings:
    def test_load_job_settings_from_valid_file(self, mocker, mock_api_client):
        """
        Tests that job settings can be loaded from a valid JSON file.
        """
        mocker.patch.object(ClusterApi, "__init__", return_value=None)

        job_settings = {"name": "test_job", "timeout_seconds": 60}
        job_settings_path = "tests/data/job_settings.json"
        with open(job_settings_path, "w") as f:
            json.dump(job_settings, f)

        job = JobSettings(mock_api_client)
        loaded_settings = job.load_job_settings(settings_path=Path(job_settings_path))

        assert loaded_settings == job_settings

    def test_load_job_settings_default_when_file_invalid(self, mocker, mock_api_client):
        """
        Tests that the default settings are returned if the given file could not be
        loaded
        """
        default_settings = {"name": "default_job", "timeout_seconds": 30}

        mocker.patch.object(ClusterApi, "__init__", return_value=None)
        mocker.patch.object(
            JobSettings, "_get_default_job_settings", return_value=default_settings
        )
        mocker.patch(
            "pushcart.setup.job_settings.get_config_from_file", return_value=None
        )

        job_settings = JobSettings(mock_api_client)

        result = job_settings.load_job_settings(
            settings_path="tests/data/job_settings.json", default_settings="pipeline"
        )
        assert result == default_settings

    def test_load_job_settings_when_file_invalid_and_no_default(
        self, mocker, mock_api_client
    ):
        """
        Tests that the job settings cannot be loaded when the given file cannot be read
        and no default job settings provided
        """
        mocker.patch.object(ClusterApi, "__init__", return_value=None)
        mocker.patch(
            "pushcart.setup.job_settings.get_config_from_file", return_value=None
        )

        job_settings_path = "tests/data/invalid_job_settings.json"

        job = JobSettings(mock_api_client)

        with pytest.raises(RuntimeError):
            _ = job.load_job_settings(settings_path=job_settings_path)

    def test_load_default_job_settings_for_invalid_job_type(
        self, mocker, mock_api_client
    ):
        """
        Tests that default job settings cannot be loaded for an invalid job type.
        """
        mocker.patch.object(ClusterApi, "__init__", return_value=None)
        job = JobSettings(mock_api_client)

        with pytest.raises(ValueError):
            job.load_job_settings(default_settings="invalid_job_type")

    def test_validate_databricks_api_client_with_invalid_client(self):
        """
        Tests that the ApiClient input is properly validated with an invalid client.
        """
        client = "invalid client"

        with pytest.raises(TypeError):
            validate_databricks_api_client(client)


class TestJobsWrapper:
    def test_get_or_create_release_job_new(self, mocker, mock_api_client):
        """
        Tests that get_or_create_release_job creates a new release job when one does
        not exist.
        """
        job_settings = {"name": "test_job", "job_type": "python"}

        jobs_wrapper = JobsWrapper(mock_api_client)
        jobs_wrapper.get_or_create_job = mocker.Mock(return_value="12345")
        mocker.patch.object(JobSettings, "load_job_settings", return_value=job_settings)

        job_id = jobs_wrapper.get_or_create_release_job()

        assert job_id == "12345"
        jobs_wrapper.get_or_create_job.assert_called_once_with(job_settings)

    def test_get_or_create_job_existing(self, mocker, mock_api_client):
        """
        Tests that get_or_create_job retrieves an existing job when one exists and
        resets it to current settings.
        """
        mocker.patch.object(JobsApi, "reset_job", return_value=None)

        job_settings = {"name": "test_job", "job_type": "python"}

        jobs_wrapper = JobsWrapper(mock_api_client)
        jobs_wrapper._get_job = mocker.Mock(return_value="12345")

        job_id = jobs_wrapper.get_or_create_job(job_settings)

        assert job_id == "12345"
        jobs_wrapper._get_job.assert_called_once_with("test_job")
        jobs_wrapper.jobs_api.reset_job.assert_called_once_with(
            {"job_id": "12345", "new_settings": job_settings}
        )

    def test_run_job(self, mocker, mock_api_client):
        """
        Tests that run_job runs a job and returns its status and URL.
        """
        jobs_wrapper = JobsWrapper(mock_api_client)

        jobs_wrapper.jobs_api.run_now = mocker.Mock(return_value={"run_id": "67890"})
        jobs_wrapper.runs_api.get_run = mocker.Mock(
            side_effect=[
                {
                    "state": {"life_cycle_state": "RUNNING"},
                    "run_page_url": "http://test.com",
                },
                {
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "SUCCESS",
                    },
                    "run_page_url": "http://test.com",
                },
            ]
        )

        status, url = jobs_wrapper.run_job("12345")

        assert status == "SUCCESS"
        assert url == "http://test.com"
        jobs_wrapper.jobs_api.run_now.assert_called_once_with(
            job_id="12345",
            jar_params=None,
            notebook_params=None,
            python_params=None,
            spark_submit_params=None,
        )
        jobs_wrapper.runs_api.get_run.assert_has_calls(
            [mocker.call("67890"), mocker.call("67890")]
        )

    def test_get_or_create_job_new(self, mocker, mock_api_client):
        """
        Tests that get_or_create_job creates a new job when one does not exist.
        """
        mocker.patch.object(JobsApi, "list_jobs", return_value={"jobs": []})
        mocker.patch.object(JobsApi, "create_job", return_value={"job_id": 1234})

        job_settings = {"name": "test_job"}

        jobs_wrapper = JobsWrapper(mock_api_client)
        job_id = jobs_wrapper.get_or_create_job(job_settings)

        assert job_id == 1234
        jobs_wrapper.jobs_api.create_job.assert_called_once_with(job_settings)

    def test_delete_job(self, mocker, mock_api_client):
        """
        Tests that delete_job deletes a job.
        """
        mocker.patch.object(
            JobsApi, "get_job", return_value={"settings": {"name": "test_job"}}
        )
        mocker.patch.object(JobsApi, "delete_job", return_value=None)

        job_id = "test_job_id"

        jobs_wrapper = JobsWrapper(mock_api_client)
        jobs_wrapper.delete_job(job_id)

        jobs_wrapper.jobs_api.delete_job.assert_called_once_with(job_id=job_id)

    def test_get_or_create_job_invalid_settings(self, mock_api_client):
        """
        Tests that an error is raised when invalid job settings are provided to
        get_or_create_job.
        """
        job_settings = {"invalid_field": "invalid_value"}

        with pytest.raises(ValueError):
            JobsWrapper(mock_api_client).get_or_create_job(job_settings)


class TestSecretsWrapper:
    def test_create_scope_if_not_exists_success(self, mocker, mock_api_client):
        """
        Tests that create_scope_if_not_exists creates a new scope if it does not exist.
        """
        mock_list_scopes = mocker.patch.object(SecretApi, "list_scopes")
        mock_list_scopes.return_value = {"scopes": []}
        mock_create_scope = mocker.patch.object(SecretApi, "create_scope")
        secrets_wrapper = SecretsWrapper(mock_api_client)

        secrets_wrapper.create_scope_if_not_exists("test_scope")

        mock_list_scopes.assert_called_once()
        mock_create_scope.assert_called_once_with(
            initial_manage_principal="users",
            scope="test_scope",
            scope_backend_type="DATABRICKS",
            backend_azure_keyvault=None,
        )

    def test_create_scope_if_not_exists_already_exists(self, mocker, mock_api_client):
        """
        Tests that create_scope_if_not_exists does not create a new scope if it already
        exists.
        """
        mock_list_scopes = mocker.patch.object(SecretApi, "list_scopes")
        mock_list_scopes.return_value = {"scopes": [{"name": "test_scope"}]}
        mock_create_scope = mocker.patch.object(SecretApi, "create_scope")
        secrets_wrapper = SecretsWrapper(mock_api_client)

        secrets_wrapper.create_scope_if_not_exists("test_scope")

        mock_list_scopes.assert_called_once()
        mock_create_scope.assert_not_called()

    def test_push_secrets_empty_dict(self, mocker, mock_api_client):
        """
        Tests that push_secrets does not push secrets if secrets_dict is empty.
        """
        mock_create_scope_if_not_exists = mocker.patch.object(
            SecretsWrapper, "create_scope_if_not_exists"
        )
        secrets_wrapper = SecretsWrapper(mock_api_client)

        secrets_wrapper.push_secrets()

        mock_create_scope_if_not_exists.assert_not_called()

    def test_push_secrets_success(self, mocker, mock_api_client):
        """
        Tests that push_secrets pushes secrets to an existing scope.
        """
        mock_secrets_api_put_secret = mocker.patch.object(SecretApi, "put_secret")
        mocker.patch.object(
            SecretApi, "list_scopes", return_value={"scopes": [{"name": "pushcart"}]}
        )

        mock_api_client.default_headers = {"Authorization": "Bearer test_token"}
        secrets_wrapper = SecretsWrapper(mock_api_client)

        secrets_dict = {"test_key": "test_value"}
        secret_scope_name = "pushcart"

        secrets_wrapper.push_secrets(
            secret_scope_name=secret_scope_name, secrets_dict=secrets_dict
        )

        mock_secrets_api_put_secret.assert_called_once_with(
            secret_scope_name, "test_key", "test_value", bytes_value=None
        )

    def test_invalid_secret_scope_name(self, mocker, mock_api_client):
        """
        Tests that an invalid secret_scope_name raises an error.
        """
        mocker.patch.object(
            SecretApi, "list_scopes", return_value={"scopes": [{"name": "pushcart"}]}
        )
        mock_api_client.default_headers = {"Authorization": "Bearer test_token"}
        secrets_wrapper = SecretsWrapper(mock_api_client)

        secrets_dict = {"test_key": "test_value"}
        secret_scope_name = "#invalid_scope_name"

        with pytest.raises(ValueError):
            secrets_wrapper.push_secrets(
                secret_scope_name=secret_scope_name, secrets_dict=secrets_dict
            )

    def test_invalid_key_or_value(self, mocker, mock_api_client):
        """
        Test that an invalid key or value in secrets_dict raises an error
        """
        mocker.patch.object(
            SecretApi, "list_scopes", return_value={"scopes": [{"name": "pushcart"}]}
        )
        mock_api_client.default_headers = {"Authorization": "Bearer test_token"}
        secrets_wrapper = SecretsWrapper(mock_api_client)

        secrets_dict = {"#invalid_key": "test_value"}
        secret_scope_name = "pushcart"

        with pytest.raises(ValueError):
            secrets_wrapper.push_secrets(
                secret_scope_name=secret_scope_name, secrets_dict=secrets_dict
            )
