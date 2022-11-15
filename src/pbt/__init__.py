"""
DATABRICKS_HOST, DATABRICKS_TOKEN
"""


import click
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
@click.option(
    "--mvn-params", help="Additional params to be passed to mvn commands", default=""
)
def build(path, mvn_params):
    pbt = ProphecyBuildTool(path, mvn_params)
    pbt.build()


@cli.command()
@click.option(
    "--path",
    help="Path to the directory containing the pbt_project.yml file",
    required=True,
)
@click.option(
    "--mvn-params", help="Additional params to be passed to mvn commands", default=""
)
def deploy(path, mvn_params):
    pbt = ProphecyBuildTool(path, mvn_params)
    pbt.deploy()


@cli.command()
@click.option(
    "--path",
    help="Path to the directory containing the pbt_project.yml file",
    required=True,
)
@click.option(
    "--mvn-params", help="Additional params to be passed to mvn commands", default=""
)
def test(path, mvn_params):
    pbt = ProphecyBuildTool(path, mvn_params)
    pbt.test()


if __name__ == "pbt":
    cli()
