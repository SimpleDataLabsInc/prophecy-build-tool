"""
DATABRICKS_HOST, DATABRICKS_TOKEN
"""
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
def build(path, pipelines):
    pbt = ProphecyBuildTool(path)
    pbt.build(pipelines)


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
    default=False
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
    "--path",
    help="Path to the directory containing the pbt_project.yml file",
    required=True,
)
@click.option(
    "--state-config-path",
    help="Path to the yaml file containing the state configuration",
    required=True,
)
@click.option(
    "--project-id",
    help="Prophecy project id to deploy",
    required=True,
)
@click.option(
    "--release-version",
    help="Release version to be used during deployments",
    required=True,
)

@click.option(
    "--pipeline-ids",
    help="pipeline-ids to be build and upload",
    required=False,
)
def build_v2(path, state_config_path, project_id, release_version, pipeline_ids):
    pbt = PBTCli(path, state_config_path, project_id, release_version)
    pbt.build(pipeline_ids)


@cli.command()
@click.option(
    "--path",
    help="Path to the directory containing the pbt_project.yml file",
    required=True,
)
@click.option(
    "--state-config-path",
    help="Path to the yaml file containing the state configuration",
    required=True,
)
@click.option(
    "--project-id",
    help="Prophecy project id to deploy",
    required=True,
)
@click.option(
    "--release-version",
    help="Release version to be used during deployments",
    required=True,
)
def headers_v2(path, project_id: str, release_version: str, state_config_path: str,):
    pbt = PBTCli(path, state_config_path, project_id, release_version)
    pbt.headers()


@cli.command()
@click.option(
    "--path",
    help="Path to the directory containing the pbt_project.yml file",
    required=True,
)
@click.option(
    "--state-config-path",
    help="Path to the yaml file containing the state configuration",
    required=True,
)
@click.option(
    "--project-id",
    help="Prophecy project id to deploy",
    required=True,
)
@click.option(
    "--release-version",
    help="Release version to be used during deployments",
    required=True,
)
@click.option(
    "--job-ids",
    help="jobs to be deployed",
    required=False,
)
def deploy_v2(path, state_config_path, project_id, release_version, jobs_ids):
    pbt = PBTCli(path, state_config_path, project_id, release_version)
    pbt.deploy(jobs_ids.split(","))


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
