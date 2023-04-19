import logging
from textwrap import dedent
from typing import Optional

import click
from databricks_cli.configure.config import provide_api_client
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.workspace.api import WorkspaceApi
from pydantic import FilePath, HttpUrl, constr, dataclasses, validator

from pushcart.configuration import get_config_from_file
from pushcart.setup.jobs_wrapper import JobsWrapper
from pushcart.setup.repos_wrapper import ReposWrapper
from pushcart.setup.secrets_wrapper import SecretsWrapper
from pushcart.validation.common import HttpAuthToken


@dataclasses.dataclass
class Deployment:
    """
    The Deployment class is responsible for deploying the pushcart release job from a
    Git repository. Using an existing Databricks CLI configuration, it gets or creates
    a repository in Databricks Repos, updating it with a new branch, creates a release
    job, runs it, and logs the job status.

    Fields:
    - git_url: the URL of the Git repository
    - git_branch: the name of the Git branch
    - dbr_repo_user: the name of the repository user in Databricks (default is
      "pushcart")
    - git_provider: the name of the Git provider (optional)
    - job_settings_file: the release job settings file (optional)
    - secret_scope: Databricks secret scope to use on target environment (default is
      "pushcart")
    """

    git_url: HttpUrl
    git_branch: constr(min_length=1, strict=True, regex=r"^[^'\"]*$") = "main"
    repos_user: Optional[
        constr(min_length=1, strict=True, regex=r"^[^'\"]*$")
    ] = "pushcart"
    git_provider: Optional[
        constr(
            min_length=1,
            strict=True,
            regex=r"^(gitHub|bitbucketCloud|gitLab|azureDevOpsServices|gitHubEnterprise|bitbucketServer|gitLabEnterpriseEdition|awsCodeCommit)$",
        )
    ] = None
    job_settings_file: Optional[FilePath] = None
    secret_scope: Optional[
        constr(
            strip_whitespace=True,
            to_lower=True,
            strict=True,
            min_length=1,
            regex=r"^[A-Za-z0-9\-_.]{1,128}$",
        )
    ] = "pushcart"

    @validator("git_branch")
    @classmethod
    def clean_branch_name(cls, value):
        """
        Validator method that removes the "refs/heads/" prefix from the Git branch name
        """
        return value.replace("refs/heads/", "")

    @provide_api_client
    def __post_init_post_parse__(self, api_client: ApiClient):
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.INFO)

        init_log = f"""
          Using deployment parameters:
          - Databricks Workspace: {api_client.url}

          - Git Provider: {self.git_provider}
          - Git URL: {self.git_url}
          - Git Branch: {self.git_branch}
          - Release Job Settings JSON: {self.job_settings_file}
        """
        self.log.info(dedent(init_log))

        self.client = api_client
        self.repos_api = ReposWrapper(self.client)
        self.jobs_api = JobsWrapper(self.client)
        self.workspace_api = WorkspaceApi(self.client)
        self.secrets_api = SecretsWrapper(self.client)

    def _save_token_to_secret_scope(self, secret_scope_name: str, auth: dict) -> None:
        """
        Saves the Databricks token used in the deployment into a secret scope on the
        target Databricks environment, in order to be reused by the release process
        """
        validated_auth = HttpAuthToken(**auth)
        self.secrets_api.push_secrets(
            secret_scope_name,
            {"token": validated_auth.Authorization.removeprefix("Bearer ")},
        )

    def deploy(self) -> None:
        """
        Deploys the release job by getting or creating the repository, updating the
        repository with a new branch, creating a release job, running the job, and
        logging the job status. Also saves the Databricks authentication token used
        for deployment in a secret scope on the environment for reuse during release
        """
        self._save_token_to_secret_scope(self.secret_scope, self.client.default_headers)

        self.workspace_api.mkdirs(f"/Repos/{self.repos_user}")

        self.repos_api.get_or_create_repo(
            self.repos_user, self.git_url, self.git_provider
        )
        self.repos_api.update(self.git_branch)

        job_id = self.jobs_api.get_or_create_release_job(self.job_settings_file)
        job_status, run_url = self.jobs_api.run_job(job_id)

        log_message = f"Run {job_status} for {job_id}: {run_url}"
        if job_status == "SUCCESS":
            self.log.info(log_message)
        elif job_status == "FAILED":
            self.log.error(log_message)
        else:
            self.log.warning(log_message)


@click.command()
@click.option("--from-file", "-f", help="Deployment configuration file path")
@click.option("--profile", "-p", help="Databricks CLI profile to use (optional)")
def deploy(
    from_file: str,
    profile: str = None,  # Derived from context by @provide_api_client
):
    kwargs = get_config_from_file(from_file)

    d = Deployment(**kwargs)
    d.deploy()


if __name__ == "__main__":
    deploy(auto_envvar_prefix="PUSHCART")
