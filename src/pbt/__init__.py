"""
DATABRICKS_HOST, DATABRICKS_TOKEN
"""
from typing import Optional

import click
import pkg_resources
from rich import print

from .prophecy_build_tool import ProphecyBuildTool
from .v2.pbt_cli import PBTCli


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
    "--pipelines",
    help="Pipeline names(comma separated) which can be used to filter pipelines to be build",
    default="",
)
@click.option(
    "--ignore-build-errors",
    help="Flag to ignore any build errors in pipelines and return success (EXIT_CODE = 0), please refer logs for any "
         "errors.",
    default=False,
    is_flag=True,
    required=False,
)
@click.option(
    "--ignore-parse-errors",
    help="Flag to ignore any parsing errors in pipelines and return success (EXIT_CODE = 0), please refer logs for "
         "any errors.",
    default=False,
    is_flag=True,
    required=False,
)
def build(path, pipelines, ignore_build_errors, ignore_parse_errors):
    pbt = ProphecyBuildTool(path, ignore_parse_errors=ignore_parse_errors)
    pbt.build(pipelines, exit_on_build_failure=(not ignore_build_errors))


@cli.command()
@click.option(
    "--path",
    help="Path to the directory containing the pbt_project.yml file",
    required=True,
)
@click.option(
    "--treat-warnings-as-errors",
    help="Specifies whether to treat warnings as errors.",
    is_flag=True,
    required=False,
    default=False,
)
def validate(path, treat_warnings_as_errors):
    pbt = ProphecyBuildTool(path)
    pbt.validate(treat_warnings_as_errors)


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
@click.option(
    "--fabric-ids",
    help="Fabric IDs(comma separated) which can be used to filter jobs for deployments",
    default="",
)
@click.option(
    "--job-ids",
    help="Job IDs(comma separated) which can be used to filter jobs for deployment",
    default="",
)
@click.option("--skip-builds", is_flag=True, default=False, help="Flag to skip building Pipelines")
def deploy(
        path,
        dependent_projects_path,
        release_version,
        project_id,
        prophecy_url,
        fabric_ids,
        job_ids,
        skip_builds,
):
    pbt = ProphecyBuildTool(path, dependent_projects_path, release_version, project_id, prophecy_url)
    pbt.deploy(fabric_ids=fabric_ids, skip_builds=skip_builds, job_ids=job_ids)


@cli.command()
@click.option(
    "--project-dir",
    help="Path to the directory containing the pbt_project.yml file",
    required=True,
)
@click.option(
    "--project-id",
    help="Path to the directory containing the pbt_project.yml file",
    required=True,
)
@click.option(
    "--conf-dir",
    help="Path to the configuration file folders",
    required=False,
)
@click.option(
    "--release-tag",
    help="Release tag",
    required=False,
)
@click.option(
    "--release-version",
    help="Release version",
    required=True,
)
def deploy_v2(project_dir, project_id: str, conf_dir: Optional[str],
              release_tag: Optional[str], release_version: str):
    if conf_dir is not None:
        pbt = PBTCli.from_conf_folder(project_dir, project_id, conf_dir, release_tag, release_version)
        pbt.headers()
        pbt.deploy([])
    else:
        raise Exception("Not implemented")


@cli.command()
@click.option(
    "--path",
    help="Path to the directory containing the pbt_project.yml file",
    required=True,
)
@click.option(
    "--driver-library-path",
    help="Jar path of prophecy-python-libs and other required dependencies",
    required=False,
)
def test(path, driver_library_path):
    pbt = ProphecyBuildTool(path)
    pbt.test(driver_library_path)


if __name__ == "pbt":
    print(
        f"[bold purple]Prophecy-build-tool[/bold purple] [bold black]"
        f"v{pkg_resources.require('prophecy-build-tool')[0].version}[/bold black]\n"
    )
    cli()
