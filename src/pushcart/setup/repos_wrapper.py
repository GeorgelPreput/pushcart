import logging
import os
import re
from pathlib import Path
from typing import Optional

from databricks_cli.repos.api import ReposApi
from databricks_cli.sdk.api_client import ApiClient
from pydantic import HttpUrl, constr, dataclasses, validator

from pushcart.validation.common import (
    PydanticArbitraryTypesConfig,
    validate_databricks_api_client,
)


@dataclasses.dataclass(config=PydanticArbitraryTypesConfig)
class ReposWrapper:
    """
    The ReposWrapper class provides a wrapper around the Databricks Repos API. It
    allows users to get or create a repository, update the repository with a new
    branch, and detect the Git provider from a given URL.

    Fields:
    - client: an ApiClient object used to interact with the Databricks Repos API
    - log: a logger object used for logging messages
    - repos_api: a ReposApi object used to interact with the Databricks Repos API
    - repo_id: the ID of the repository (initialized to None)
    """

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

        self.repos_api = ReposApi(self.client)
        self.repo_id = None

    @staticmethod
    def detect_git_provider(repo_url):
        """
        Detects the Git provider from a given URL
        """

        providers = {
            "gitHub": r"(?:https?://|git@)github\.com[:/]",
            "bitbucketCloud": r"(?:https?://|git@)bitbucket\.org[:/]",
            "gitLab": r"(?:https?://|git@)gitlab\.com[:/]",
            "azureDevOpsServices": r"(?:https?://|git@ssh?\.?)([\w-]+@)?\.?dev\.azure\.com[:/]",
            "gitHubEnterprise": r"(?:https?://|git@)([\w-]+)\.github(?:usercontent)?\.com[:/]",
            "bitbucketServer": r"(?:https?://|git@)([\w-]+)\.bitbucket(?:usercontent)?\.com[:/]",
            "gitLabEnterpriseEdition": r"(?:https?://|git@)([\w-]+)\.gitlab(?:usercontent)?\.com[:/]",
            "awsCodeCommit": r"(?:https?://|git@)git-codecommit\.[^/]+\.amazonaws\.com[:/]",
        }

        for provider, regex in providers.items():
            if re.match(regex, repo_url):
                return provider

        raise ValueError(
            "Could not detect Git provider from URL. Please specify git_provider explicitly."
        )

    def get_or_create_repo(
        self,
        repo_user: constr(min_length=1, strict=True, regex=r"^[^'\"]*$"),
        git_url: HttpUrl,
        git_repo: constr(min_length=1, strict=True, regex=r"^[^'\"]*$"),
        git_provider: Optional[
            constr(
                min_length=1,
                strict=True,
                regex=r"^(gitHub|bitbucketCloud|gitLab|azureDevOpsServices|gitHubEnterprise|bitbucketServer|gitLabEnterpriseEdition|awsCodeCommit)$",
            )
        ] = None,
    ) -> str:
        """
        Gets or creates a repository with a given user, Git URL, Git repository name,
        and Git provider (if not detected from URL)
        """

        if not git_provider:
            self.log.warning(
                "No Git provider specified. Attempting to guess based on URL."
            )
            git_provider = self.detect_git_provider(git_url)

        repo_path = Path(os.path.join("/", "Repos", repo_user, git_repo)).as_posix()
        try:
            self.repo_id = self.repos_api.get_repo_id(path=repo_path)
        except Exception:
            self.log.warning("Failed to get repo ID")

        if not self.repo_id:
            self.log.warning(f"Repo not found, cloning from URL: {git_url}")

            repo = self.repos_api.create(git_url, git_provider, repo_path)
            self.repo_id = repo["id"]

        self.log.info(f"Repository ID: {self.repo_id}")

        return self.repo_id

    def update(self, git_branch: constr(min_length=1, strict=True, regex=r"^[^'\"]*$")):
        """
        Updates the Databricks repository with a new branch
        """

        if not self.repo_id:
            raise ValueError(
                "Repo not initialized. Please first run get_or_create_repo()"
            )

        # TODO: Support Git tags as well
        self.repos_api.update(self.repo_id, branch=git_branch, tag=None)
