import pytest
from databricks_cli.repos.api import ReposApi
from databricks_cli.sdk.api_client import ApiClient

from pushcart.setup.repos_wrapper import ReposWrapper


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
            git_url="https://github.com/test_user/test_repo",
            git_repo="test_repo",
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
            git_url="https://github.com/test_user/test_repo",
            git_repo="test_repo",
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
            ReposWrapper(mock_api_client).detect_git_provider("https://example.com")

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
        git_provider = repos_wrapper.detect_git_provider(
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
            repos_wrapper.get_or_create_repo(
                "", "https://github.com/user/repo.git", "repo"
            )

    def test_update_invalid_input(self, mock_api_client):
        """
        Tests that the update method raises a ValueError if invalid input parameters
        are passed.
        """
        repos_wrapper = ReposWrapper(mock_api_client)
        with pytest.raises(ValueError):
            repos_wrapper.update("")
