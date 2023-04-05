import logging
from textwrap import dedent
from typing import Any, Optional, Union

import click
from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import DatabricksConfig
from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi
from pydantic import FilePath, HttpUrl, Json, constr, dataclasses, validator

from pushcart.setup.repos_wrapper import ReposWrapper


@dataclasses.dataclass
class Deployment:
    workspace: HttpUrl
    token: constr(min_length=1, strict=True, regex=r"^[^'\"]*$")
    repo_user: Optional[constr(min_length=1, strict=True, regex=r"^[^'\"]*$")] = "main"
    git_provider: Optional[
        constr(
            min_length=1,
            strict=True,
            regex=r"^(gitHub|bitbucketCloud|gitLab|azureDevOpsServices|gitHubEnterprise|bitbucketServer|gitLabEnterpriseEdition|awsCodeCommit)$",
        )
    ] = None
    git_url: HttpUrl
    git_repo: constr(min_length=1, strict=True, regex=r"^[^'\"]*$")
    git_branch: constr(min_length=1, strict=True, regex=r"^[^'\"]*$")
    settings_json: Optional[Union[FilePath, Json[Any]]] = None

    @validator("git_branch")
    @classmethod
    def clean_branch_name(cls, value):
        return value.replace("refs/heads/", "")

    def __post_init_post_parse__(self):
        self.log = logging.getLogger(__name__)

        init_log = f"""
          Using deployment parameters:
          - Databricks Workspace: {self.workspace}
          - Token: {self.token[:5]}***** [REDACTED]
          - Git Provider: {self.git_provider}
          - Git URL: {self.git_url}
          - Git Repository: {self.git_repo}
          - Git Branch: {self.git_branch}
          - Settings JSON: {self.settings_json}
        """
        self.log.info(dedent(init_log))

        config = DatabricksConfig.from_token(self.workspace, self.token, False)
        client = _get_api_client(config)
        self.repos_api = ReposWrapper(client)
        self.jobs_api = JobsApi(client)
        self.runs_api = RunsApi(client)

    def deploy(self):
        pass


@click.command()
@click.option("--workspace", "-w")
@click.option("--token", "-t")
@click.option("--dbr-repos-user", "-n")
@click.option("--git-provider", "-p")
@click.option("--git-url", "-u")
@click.option("--git-repo", "-r")
@click.option("--git-branch", "-b")
@click.option("--release-settings-json", "-j")
def deploy(
    workspace: str,
    token: str,
    dbr_repos_user: str,
    git_provider: str,
    git_url: str,
    git_repo: str,
    git_branch: str,
    release_settings_json: str,
):
    d = Deployment(
        workspace=workspace,
        token=token,
        repo_user=dbr_repos_user,
        git_provider=git_provider,
        git_url=git_url,
        git_repo=git_repo,
        git_branch=git_branch,
        settings_json=release_settings_json,
    )
    d.deploy()


if __name__ == "__main__":
    deploy(auto_envvar_prefix="PUSHCART")
