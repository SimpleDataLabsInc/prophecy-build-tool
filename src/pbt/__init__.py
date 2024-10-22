"""
DATABRICKS_HOST, DATABRICKS_TOKEN
"""
import sys
from typing import Optional

import click
import pkg_resources
from rich import print

from .utils.versioning import get_bumped_version
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
@click.option(
    "--add-pom-python",
    default=False,
    is_flag=True,
    help="adds pom.xml and MAVEN_COORDINATES files to pyspark pipeline WHL files",
    required=False,
)
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
@click.option(
    "--artifactory",
    default=None,
    is_flag=False,
    help="Use Pypi/Maven packages instead of DBFS files for deployment",
    required=False,
)
@click.option(
    "--skip-artifactory-upload",
    default=False,
    is_flag=True,
    help="Flag to skip uploading to private artifactory, must be used with --artifactory option",
    required=False,
)
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
    artifactory: str,
    skip_artifactory_upload: bool,
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
        artifactory,
        skip_artifactory_upload,
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
    "--path", help="Path to the directory containing the pbt_project.yml file", required=True, metavar="<PATH>"
)
@click.option(
    "--repo-path",
    help="Path to the repository root. If left blank it will use '--path'",
    required=False,
    metavar="<PATH>",
)
@click.option(
    "--bump",
    type=click.Choice(["major", "minor", "patch", "build", "prerelease"], case_sensitive=False),
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
    "--force",
    "--spike",
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
    "--set-suffix",
    type=str,
    help="Set a suffix string. example '-SNAPSHOT' or '-rc.4' . If this is not a valid semVer string an error will be "
    "thrown.",
    required=False,
)
@click.option(
    "--check-sync",
    help="check to see if versions are synced. exit code 0 success, 1 failure.",
    default=False,
    is_flag=True,
    required=False,
)
@click.option(
    "--compare-to-target",
    "--compare",
    metavar="<TARGET_BRANCH>",
    type=str,
    help="Checks to see if current branch has a greater version number than the <TARGET_BRANCH> branch name provided. "
    "Returns 0 true, or 1 false. (Also performs --sync check). NOTE: if you provide '--bump'  with this option then it "
    "will compare the current version with the version in the target branch and use the bump strategy IF the current "
    "version is lower than the target. ",
    required=False,
    default=None,
)
@click.option(
    "--make-unique",
    help="Helper function that makes a version unique for feature branches. Adds build-metadata suffix to differentiate"
    " this feature branch from other dev branches (hash based on branch name). Adds Prerelease candidate to "
    " deprioritize this version from being chosen over other versions (recommended so that it does not "
    " accidentally get chosen over a real release. "
    " \nformat: MAJOR.MINOR.PATCH-PRERELEASE+BUILDMETADATA"
    " \npython example: 3.3.0 -> 3.3.0-dev+sha.j0239ruf0ew"
    " \nscala example: 3.3.0 -> 3.3.0-SNAPSHOT+sha.j0239ruf0ew",
    default=False,
    is_flag=True,
    required=False,
)
def versioning(path, repo_path, bump, set, force, sync, set_suffix, check_sync, compare_to_target, make_unique):
    pbt = PBTCli.from_conf_folder(path)
    if not repo_path:
        repo_path = path
    option_total = sum(
        [
            set is not None,
            bump is not None,
            sync,
            check_sync,
            set_suffix is not None,
            compare_to_target is not None,
            make_unique,
        ]
    )

    if option_total > 1:
        if option_total == 2 and compare_to_target and bump:
            pass  # this is the one combo that is allowed; compare_to_target and bump
        else:
            raise click.UsageError(
                "Options '--set', '--bump', '--sync', '--check-sync', '--set-prerelease', '--check-sync', "
                " '--compare-to-target'"
                "are mutually exclusive."
            )

    if set:
        pbt.version_set(set, force)
    elif bump and not compare_to_target:
        pbt.version_bump(bump, force)
    elif sync:
        pbt.version_set(None, force)
    elif set_suffix:
        pbt.version_set_suffix(set_suffix, force)
    elif check_sync:
        pbt.version_check_sync()
    elif compare_to_target:
        if not pbt.version_compare_to_target(repo_path, compare_to_target):
            if bump:
                # when bump is given then use the strategy to bump over the version found in the target.
                target_version = pbt.version_get_target_branch_version(repo_path, compare_to_target)
                new_version = get_bumped_version(
                    target_version,
                    bump,
                    pbt.project.project.pbt_project_dict["language"],
                )
                pbt.version_set(new_version, force=True)
            else:
                # when bump is not given, then just return 0 or 1 to compare versions
                sys.exit(1)
        else:
            # always if our version is already higher, make sure we are sync'd.
            pbt.version_check_sync()
    elif make_unique:
        pbt.version_make_unique(repo_path, force=True)
    else:
        raise click.UsageError(
            "must give ONE of: '--set', '--bump', '--sync', '--check-sync', '--set-prerelease', "
            " '--compare-to-target' '--make-unique' "
        )


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
    'This overrides <branch_name>. Provide "" to omit the branch name.',
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


def main():
    print(
        f"[bold purple]Prophecy-build-tool[/bold purple] [bold black]"
        f"v{pkg_resources.require('prophecy-build-tool')[0].version}[/bold black]\n"
    )
    cli()


if __name__ == "pbt":
    main()
