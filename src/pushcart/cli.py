import click


@click.command()
@click.argument("command", required=True)
@click.option("--workspace", "-w", default=None, help="")
@click.option("--token", "-t", default=None, help="")
@click.option("--dbr-repos-user", "-n", default=None, help="")
@click.option("--git-provider", "-p", default=None, help="")
@click.option("--git-url", "-u", default=None, help="")
@click.option("--git-repo", "-r", default=None, help="")
@click.option("--git-branch", "-b", default=None, help="")
@click.option("--release-settings-json", "-j", default=None, help="")
def cli(
    command: str,
    workspace: str = None,
    token: str = None,
    dbr_repos_user: str = None,
    git_provider: str = None,
    git_url: str = None,
    git_repo: str = None,
    git_branch: str = None,
    release_settings_json: str = None,
):
    print(f"Running {command} with params:")
    print(
        workspace,
        token,
        dbr_repos_user,
        git_provider,
        git_url,
        git_repo,
        git_branch,
        release_settings_json,
    )


if __name__ == "__main__":
    cli(auto_envvar_prefix="PUSHCART")
