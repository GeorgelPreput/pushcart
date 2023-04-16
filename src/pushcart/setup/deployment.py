import logging
from textwrap import dedent
from typing import Optional

import click
from databricks_cli.configure.config import provide_api_client
from databricks_cli.sdk.api_client import ApiClient
from pydantic import FilePath, HttpUrl, constr, dataclasses, validator

from pushcart.setup.jobs_wrapper import JobsWrapper
from pushcart.setup.repos_wrapper import ReposWrapper


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
    - dbr_repo_user: the name of the repository user in Databricks (default is "pushcart")
    - git_provider: the name of the Git provider (optional)
    - job_settings_file: the release job settings file (optional)
    """

    git_url: HttpUrl
    git_branch: constr(min_length=1, strict=True, regex=r"^[^'\"]*$")
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

        client = api_client
        self.repos_api = ReposWrapper(client)
        self.jobs_api = JobsWrapper(client)

    def deploy(self):
        """
        Deploys the release job by getting or creating the repository, updating the
        repository with a new branch, creating a release job, running the job, and
        logging the job status
        """
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
@click.option(
    "--repos-user",
    "-u",
    help="Databricks Repos user name (default is 'pushcart')",
)
@click.option("--git-provider", "-p", help="Name of the Git provider (optional)")
@click.option("--git-url", "-g", help="Git URL holding pushcart configurations")
@click.option("--git-branch", "-b", help="Name of the Git branch to deploy from")
@click.option("--job-settings-file", "-j", help="Release job settings file (optional)")
@click.option("--profile", "-p", help="Databricks CLI profile to use (optional)")
def deploy(
    repos_user: str,
    git_url: str,
    git_branch: str,
    git_provider: str = None,
    job_settings_file: str = None,
    profile: str = None,  # Derived from context by @provide_api_client
):
    d = Deployment(
        repos_user=repos_user,
        git_provider=git_provider,
        git_url=git_url,
        git_branch=git_branch,
        job_settings_file=job_settings_file,
    )
    d.deploy()


if __name__ == "__main__":
    deploy(auto_envvar_prefix="PUSHCART")
