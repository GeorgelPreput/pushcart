import click


@click.command()
@click.argument("command", required=True)
@click.option("--workspace", "-w")
@click.option("--token", "-t")
@click.option("--git-url", "-u")
@click.option("--git-repo", "-r")
@click.option("--git-branch", "-b")
def cli():
    pass


if __name__ == "__main__":
    cli(auto_envvar_prefix="PUSHCART")
