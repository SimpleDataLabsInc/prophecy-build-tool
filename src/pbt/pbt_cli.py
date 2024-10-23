import os
from typing import Optional

import yaml

from .deployment.project import ProjectDeployment
from .entities.project import Project
from .utils.project_config import ProjectConfig
from .utility import custom_print as log
import git
from .utils.versioning import get_bumped_version, version_check_sync
import semver
from .utils.constants import SCALA_LANGUAGE
import hashlib
import sys


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
        artifactory: str = "",
        skip_artifactory_upload: bool = False,
    ):
        """Create PBTCli from conf folder."""
        project = Project(project_path, project_id, release_tag, release_version, dependant_project_paths)
        project_config = ProjectConfig.from_conf_folder(
            project, conf_folder, fabric_ids, job_ids, skip_builds, migrate, artifactory, skip_artifactory_upload
        )
        return cls(project, project_config)

    def build(self, pipelines, ignore_build_errors, ignore_parse_errors, add_pom_python):
        self.project.build(pipelines, ignore_build_errors, ignore_parse_errors, add_pom_python)

    def test(self, driver_library_path: str):
        if driver_library_path and str.upper(self.project.project.project_language) == "PYTHON":
            if os.path.isdir(driver_library_path):
                driver_library_path = os.path.abspath(driver_library_path)
                jar_files = ",".join(
                    [
                        os.path.join(driver_library_path, file)
                        for file in os.listdir(driver_library_path)
                        if file.endswith(".jar")
                    ]
                )
                os.environ["SPARK_JARS_CONFIG"] = jar_files
            elif os.path.isfile(driver_library_path):
                driver_library_path = os.path.abspath(driver_library_path)
                os.environ["SPARK_JARS_CONFIG"] = driver_library_path
            elif "," in driver_library_path:  # allow comma separated list of files
                for f in driver_library_path.split(","):
                    if not os.path.isfile(f):
                        raise ValueError(f"{f} is not a file")
                jar_files = ",".join([os.path.abspath(f) for f in driver_library_path.split(",")])
                os.environ["SPARK_JARS_CONFIG"] = jar_files

        if "SPARK_JARS_CONFIG" in os.environ:
            print(f"    Using env SPARK_JARS_CONFIG={os.environ['SPARK_JARS_CONFIG']}")
        else:
            os.environ["SPARK_JARS_CONFIG"] = ""
            print("    Using default spark jars locations")
        self.project.test()

    def validate(self, treat_warnings_as_errors: bool):
        self.project.validate(treat_warnings_as_errors)

    def version_bump(self, bump_type, force):
        new_version = get_bumped_version(
            self.project.project.pbt_project_dict["version"],
            bump_type,
            self.project.project.pbt_project_dict["language"],
        )
        log(f"Bumping {bump_type}. New version: {new_version}")
        self.project.project.update_version(new_version=new_version, force=force)
        log("Success.")

    def version_set(self, version, force):
        if version is None:
            # sync option will send None, so take existing version.
            version = self.project.project.pbt_project_dict["version"]
            force = True
        self.project.project.update_version(new_version=version, force=force)

    def version_set_suffix(self, suffix, force):
        if not semver.Version.is_valid(self.project.project.pbt_project_dict["version"]):
            print("ERROR: current version is not in semVer syntax. cannot proceed.")
            sys.exit(1)
        current_version = semver.parse_version_info(self.project.project.pbt_project_dict["version"])
        new_version_str = f"{current_version.major}.{current_version.minor}.{current_version.patch}{suffix}"
        if not semver.Version.is_valid(new_version_str) and not force:
            print("ERROR: suffix provided is not valid semVer syntax. You can use --force to ignore this.")
            sys.exit(1)
        self.version_set(new_version_str, force)

    def version_check_sync(self):
        version_check_sync(
            self.project.project.project_path,
            self.project.project.pbt_project_dict["language"],
            self.project.project.pbt_project_dict["version"],
        )

    def version_make_unique(self, repo_path, force):
        repo = git.Repo(repo_path)
        branch_name = repo.active_branch.name
        branch_hash = hashlib.sha256(branch_name.encode()).hexdigest()[:8]
        if self.project.project.project_language == SCALA_LANGUAGE:
            self.version_set_suffix(f"-SNAPSHOT+sha.{branch_hash}", force)
        else:
            self.version_set_suffix(f"-dev+sha.{branch_hash}", force)

    def version_get_target_branch_version(self, repo_path, target_branch):
        repo = git.Repo(repo_path)
        subpath = os.path.relpath(
            os.path.join(self.project.project.project_path, "pbt_project.yml"), os.path.abspath(repo_path)
        )
        branch_content = repo.git.show(f"{target_branch}:{subpath}")
        branch_pbt_dict = yaml.safe_load(branch_content)
        branch_pbt_version = branch_pbt_dict["version"]
        return branch_pbt_version

    def version_compare_to_target(self, repo_path, target_branch):
        current_pbt_version = self.project.project.pbt_project_dict["version"]
        branch_pbt_version = self.version_get_target_branch_version(repo_path, target_branch)
        try:
            if semver.parse_version_info(current_pbt_version) <= semver.parse_version_info(branch_pbt_version):
                log(f"Current version is not higher than target version: {current_pbt_version} <= {branch_pbt_version}")
                return False
        except ValueError as e:
            log(f"failed to parse one or more versions. marking invalid: {e}")
            return False
        log(f"Comparison success: current {current_pbt_version} > target {branch_pbt_version}")
        return True

    def tag(self, repo_path, no_push=False, branch=None, custom=None):
        repo = git.Repo(repo_path)
        if custom:
            tag = custom
        else:
            tag = self.project.project.pbt_project_dict["version"]
            if branch is None:
                tag = repo.active_branch.name + "/" + tag
            elif branch != "":
                tag = branch + "/" + tag
        log(f"Setting tag to: {tag}")
        repo.create_tag(tag)
        if not no_push:
            # Pushing the tag to the remote repository
            origin = repo.remote(name="origin")
            origin.push(tag)
            log("Pushing tag to remote")
