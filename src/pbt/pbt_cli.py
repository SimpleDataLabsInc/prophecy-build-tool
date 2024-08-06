import os
from typing import Optional

from .deployment.project import ProjectDeployment
from .entities.project import Project
from .utils.project_config import ProjectConfig
from packaging.version import parse as parse_version
import copy
import git


class PBTCli(object):
    """Command line interface for PBT."""

    def __init__(self, project: Project, project_config: ProjectConfig):
        self.project = ProjectDeployment(project, project_config)

    def headers(self):
        """Print headers."""
        self.project.headers()

    def deploy(self, job_ids):
        """Deploy pipelines."""
        self.project.deploy(job_ids)

    @classmethod
    def from_conf_folder(
        cls,
        project_path: str,
        project_id: str = "",
        conf_folder: str = "",
        release_tag: Optional[str] = "",
        release_version: str = "",
        fabric_ids: str = "",
        job_ids: str = "",
        skip_builds: bool = False,
        dependant_project_paths: str = "",
        migrate: bool = False,
    ):
        """Create PBTCli from conf folder."""
        project = Project(project_path, project_id, release_tag, release_version, dependant_project_paths)
        project_config = ProjectConfig.from_conf_folder(project, conf_folder, fabric_ids, job_ids, skip_builds, migrate)
        return cls(project, project_config)

    def build(self, pipelines, ignore_build_errors, ignore_parse_errors):
        self.project.build(pipelines, ignore_build_errors, ignore_parse_errors)

    def test(self, driver_library_path: str):
        if driver_library_path and str.upper(self.project.project.project_language) == "PYTHON":
            if os.path.isdir(driver_library_path):
                driver_library_path = os.path.abspath(driver_library_path)
                jar_files = ",".join(
                    [os.path.join(driver_library_path, file) for file in os.listdir(driver_library_path) if
                     file.endswith('.jar')])
                os.environ["SPARK_JARS_CONFIG"] = jar_files
            elif os.path.isfile(driver_library_path):
                driver_library_path = os.path.abspath(driver_library_path)
                os.environ["SPARK_JARS_CONFIG"] = driver_library_path
            elif "," in driver_library_path:  # allow comma separated list of files
                for f in driver_library_path.split(","):
                    if not os.path.isfile(f):
                        raise ValueError(f"{f} is not a file")
                jar_files = ",".join([os.path.abspath(f) for f in driver_library_path.split(',')])
                os.environ["SPARK_JARS_CONFIG"] = jar_files

        if "SPARK_JARS_CONFIG" in os.environ:
            print(f"    Using env SPARK_JARS_CONFIG={os.environ['SPARK_JARS_CONFIG']}")
        else:
            os.environ["SPARK_JARS_CONFIG"] = ""
            print("    Using default spark jars locations")
        self.project.test()

    def validate(self, treat_warnings_as_errors: bool):
        self.project.validate(treat_warnings_as_errors)

    def update_all_versions(self, new_version, force):
        orig_version = parse_version(self.project.project.pbt_project_dict['version'])
        # check this version against base branch if not "force". error if it is not greater
        if not force:
            if parse_version(new_version) <= parse_version(orig_version):
                raise ValueError(f"new version {new_version} is not later than {orig_version}")

        # overwrite pbt_project.yml, all setup.py, all pom.xml
        pass #TODO

    def version_bump(self, bump_type, force):
        new_version = parse_version(self.project.project.pbt_project_dict['version'])

        if bump_type == 'major':
            new_version.major += 1
        elif bump_type == 'minor':
            new_version.minor += 1
        elif bump_type == 'patch':
            new_version.micro += 1
        else:
            raise ValueError("bad choice for bump type: ", bump_type)

        self.update_all_versions(new_version, force)

    def version_set(self, version, force):
        new_version = parse_version(version)

        self.update_all_versions(new_version, force)

    def tag(self, repo_path, no_push=False, custom=None, no_branch=False, no_project=False):
        repo = git.Repo(repo_path)

        if custom:
            tag = custom
        else:
            tag = self.project.project.pbt_project_dict['version']
            if not no_branch:
                tag = repo.active_branch + "/" + tag
            if not no_project:
                tag = self.project.project.pbt_project_dict['name'] + "/" + tag

        repo.create_tag(tag)

        if not no_push:
            # Pushing the tag to the remote repository
            origin = repo.remote(name='origin')
            origin.push(tag)

