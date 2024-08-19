"""
DATABRICKS_HOST, DATABRICKS_TOKEN
"""
from typing import Optional

import click
import pkg_resources
from rich import print

from .pbt_cli import PBTCli
from .prophecy_build_tool import ProphecyBuildTool
from .utility import is_online_mode


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
@click.option("--add-pom-python", default=False, is_flag=True,
              help="adds pom.xml and MAVEN_COORDINATES files to pyspark pipeline WHL files", required=False)
def build_v2(path, pipelines, ignore_build_errors, ignore_parse_errors, add_pom_python):
    pbt = PBTCli.from_conf_folder(path)
    pbt.build(pipelines, ignore_build_errors, ignore_parse_errors, add_pom_python)


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
@click.option(
    "--treat-warnings-as-errors",
    help="Specifies whether to treat warnings as errors.",
    is_flag=True,
    required=False,
    default=False,
)
def validate_v2(path, treat_warnings_as_errors):
    pbt = PBTCli.from_conf_folder(path)
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
    "--project-id",
    help="Path to the directory containing the pbt_project.yml file",
    default="1",
    required=False,
)
@click.option(
    "--conf-dir",
    help="Path to the configuration file folders",
    default="",
    required=False,
)
@click.option(
    "--release-tag",
    help="Release tag",
    default="1.0",
    required=False,
)
@click.option(
    "--release-version",
    help="Release version",
    default="1.0",
    required=False,
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
@click.option("--skip-builds", default=False, is_flag=True, help="Flag to skip building Pipelines", required=False)
@click.option("--dependent-projects-path", default="", help="Dependent projects path", required=False)
@click.option("--migrate", default=False, is_flag=True, help="Migrate v1 to v2 based project", required=False)
def deploy_v2(
    path: str,
    project_id: str,
    conf_dir: Optional[str],
    release_tag: Optional[str],
    release_version: str,
    fabric_ids: str,
    job_ids: str,
    skip_builds: bool,
    dependent_projects_path: str,
    migrate: bool,
):
    pbt = PBTCli.from_conf_folder(
        path,
        project_id,
        conf_dir,
        release_tag,
        release_version,
        fabric_ids,
        job_ids,
        skip_builds,
        dependent_projects_path,
        migrate,
    )
    if is_online_mode():
        pbt.headers()
    pbt.deploy([])


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
def test_v2(path, driver_library_path):
    pbt = PBTCli.from_conf_folder(path)
    pbt.test(driver_library_path)


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
@click.option(
    "--pipelines",
    help="Pipeline names(comma separated) which can be used to filter pipelines to be tested",
    default="",
)
def test(path, driver_library_path, pipelines):
    pbt = ProphecyBuildTool(path)
    pbt.test(driver_library_path, pipelines)


@cli.command()
@click.option(
    "--path",
    help="Path to the directory containing the pbt_project.yml file",
    required=True,
)
@click.option(
    "--bump",
    type=click.Choice(['major', 'minor', 'patch', 'build', 'prerelease'], case_sensitive=False),
    help="bumps one of the semantic version numbers for the project and all pipelines based on the current value. "
         "Only works if existing versions follow semantic versioning https://semver.org/",
    required=False,
)
@click.option(
    "--set",
    type=str,
    help="Explicitly set the exact version",
    required=False,
)
@click.option(
    "--force", "--spike",
    default=False,
    is_flag=True,
    help="bypass errors if the version set is lower than the base branch",
    required=False,
)
@click.option(
    "--sync",
    default=False,
    is_flag=True,
    help="Ensure all files are set to the same version that is defined in pbt_project.yml. (implies --force)",
    required=False,
)
@click.option(
    "--set-prerelease",
    type=str,
    help="Set a prerelease string. example '-SNAPSHOT' or '-rc.4'",
    required=False,
)
def versioning(path, bump, set, force, sync, set_prerelease):
    pbt = PBTCli.from_conf_folder(path)

    if sum([set is not None, bump is not None, sync, set_prerelease is not None]) > 1:
        raise click.UsageError("Options '--set', '--bump', '--sync' are mutually exclusive.")
    elif set:
        pbt.version_set(set, force)
    elif bump:
        pbt.version_bump(bump, force)
    elif sync:
        pbt.version_set(None, force)
    elif set_prerelease:
        pbt.version_set_prerelease(set_prerelease, force)
    else:
        raise click.UsageError("must give ONE of: '--set', '--bump', '--sync', --set-prerelease'")


@cli.command()
@click.option(
    "--path",
    help="Path to the directory containing the pbt_project.yml file",
    required=True,
)
@click.option(
    "--repo-path",
    help="Path to the repository root. If left blank it will use '--path'",
    required=False,
)
@click.option(
    "--no-push",
    default=False,
    is_flag=True,
    help="By default the tag will be pushed to the origin after it is created. Use this flag to skip pushing the tag.",
    required=False,
)
@click.option(
    "--branch",
    default=None,
    help="normally the tag is prefixed with the branch name: <branch_name>/<version>. "
         "This overrides <branch_name>. Provide \"\" to omit the branch name.",
    required=False,
)
@click.option(
    "--custom",
    default=None,
    type=str,
    help="Explicitly set the exact tag using a string. Ignores other options.",
    required=False,
)
def tag(path, repo_path, no_push, branch, custom):
    pbt = PBTCli.from_conf_folder(path)
    if not repo_path:
        repo_path = path
    pbt.tag(repo_path, no_push=no_push, branch=branch, custom=custom)


if __name__ == "pbt":
    print(
        f"[bold purple]Prophecy-build-tool[/bold purple] [bold black]"
        f"v{pkg_resources.require('prophecy-build-tool')[0].version}[/bold black]\n"
    )
    cli()
