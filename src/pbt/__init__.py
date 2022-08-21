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
    "--path", help="Path to the directory containing the pbt_project.yml file"
)
def build(path):
    pbt = ProphecyBuildTool(path)
    pbt.build()


@cli.command()
@click.option(
    "--path", help="Path to the directory containing the pbt_project.yml file"
)
def deploy(path):
    pbt = ProphecyBuildTool(path)
    pbt.deploy()


@cli.command()
@click.option(
    "--path", help="Path to the directory containing the pbt_project.yml file"
)
def test(path):
    pbt = ProphecyBuildTool(path)
    pbt.test()


if __name__ == "pbt":
    cli()
