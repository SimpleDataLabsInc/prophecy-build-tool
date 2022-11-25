"""
DATABRICKS_HOST, DATABRICKS_TOKEN
"""
import click
from rich import print

from .prophecy_build_tool import ProphecyBuildTool


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--path",
    help="Path to the directory containing the pbt_project.yml file",
    required=True,
)
def build(path):
    pbt = ProphecyBuildTool(path)
    pbt.build(dict())


@cli.command()
@click.option(
    "--path",
    help="Path to the directory containing the pbt_project.yml file",
    required=True,
)
@click.option("--dependent-projects-path", help="Dependent projects path", default="")
@click.option(
    "--release-version",
    help="Release version to be used during deployments",
    default="",
)
@click.option(
    "--project-id",
    help="Project Id placeholder to be used during deployments",
    default="",
)
@click.option(
    "--prophecy-url",
    help="Prophecy URL placeholder to be used during deployments",
    default="",
)
def deploy(path, dependent_projects_path, release_version, project_id, prophecy_url):
    pbt = ProphecyBuildTool(
        path, dependent_projects_path, release_version, project_id, prophecy_url
    )
    pbt.deploy()


@cli.command()
@click.option(
    "--path",
    help="Path to the directory containing the pbt_project.yml file",
    required=True,
)
def test(path):
    pbt = ProphecyBuildTool(path)
    pbt.test()


if __name__ == "pbt":
    print(
        "[bold purple]Prophecy-build-tool[/bold purple] [bold black]v1.0.3[/bold black]\n"
    )
    cli()
