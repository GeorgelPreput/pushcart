import click


@click.command()
@click.argument("command", required=True)
@click.option("--workspace", "-w")
@click.option("--token", "-t")
@click.option("--dbr-repos-user", "-n")
@click.option("--git-url", "-u")
@click.option("--git-repo", "-r")
@click.option("--git-branch", "-b")
@click.option("--git-provider", "-p")
@click.option("--release-settings-json", "-j")
def cli():
    pass


if __name__ == "__main__":
    cli(auto_envvar_prefix="PUSHCART")
