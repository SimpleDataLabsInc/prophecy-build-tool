import os
from typing import Optional

from .deployment.project import ProjectDeployment
from .entities.project import Project
from .utils.project_config import ProjectConfig
from packaging.version import InvalidVersion, parse as parse_version
from .utility import custom_print as log
import git
import re
import glob


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
        # check this version against base branch if not "force". error if it is not greater
        if not force:
            orig_version = self.project.project.pbt_project_dict['version']
            if parse_version(new_version) <= parse_version(orig_version):
                raise ValueError(f"new version {new_version} is not later than {orig_version}")

        def _replace_in_files(matching_regex: str, replacement_string: str, files: list):
            # NOTE: use this pattern matching rather than opening/rewriting files as yaml parsing may shuffle
            #  line order and cause unnecessary changes.
            for file in files:
                pattern = re.compile(matching_regex, re.MULTILINE)
                with open(file, 'r') as fd:
                    content = fd.read()
                # only replace the first instance of the version encountered. otherwise we risk
                # replacing other versions (especially found in pom.xml)
                new_content = pattern.sub(replacement_string, content, count=1)
                with open(file, 'w') as fd:
                    fd.write(new_content)

        # PBT project
        pbt_project_file = os.path.join(self.project.project.project_path, "pbt_project.yml")
        _replace_in_files(r"^version: .*$", f"version: {new_version}", [pbt_project_file])

        # replace version in language specific files:
        if self.project.project.project_language == 'python':
            matching_regex = r"^\s*version\s*=\s*.*$"
            replacement_string = f"    version = '{new_version}',"
            filename_to_find = "setup.py"
        elif self.project.project.project_language == 'scala':
            matching_regex = r"^\s*<version>.*</version>"
            replacement_string = f"    <version>{new_version}</version>"
            filename_to_find = "pom.xml"
        elif self.project.project.project_language == 'sql':
            matching_regex = r"^version: .*$"
            replacement_string = f"version: \"{new_version}\""
            filename_to_find = "dbt_project.yml"
        else:
            raise ValueError("bad project language: ", self.project.project.project_language)

        files_to_fix = glob.glob(os.path.join(self.project.project.project_path, '**', filename_to_find),
                                 recursive=True)
        _replace_in_files(matching_regex, replacement_string, files_to_fix)

    def version_bump(self, bump_type, force):
        # TODO need to be able to parse valid maven syntax and dbt syntax for versioning in addition to pep440
        try:
            v = parse_version(self.project.project.pbt_project_dict['version'])
        except InvalidVersion:
            log(f"Error bumping: Unable to parse version {self.project.project.pbt_project_dict['version']}. "
                f"Use PEP440")
            exit(1)

        if bump_type == 'major':
            major = v.major + 1
            minor = 0
            micro = 0
        elif bump_type == 'minor':
            major = v.major
            minor = v.minor + 1
            micro = 0
        elif bump_type == 'patch':
            major = v.major
            minor = v.minor
            micro = v.micro + 1
        # Version is a final class and we cannot inherit/extend so we steal the string building logic from its
        #  __str__() implementation. this follows pep440:
        parts = []
        if v.epoch != 0:
            parts.append(f"{v.epoch}!")
        parts.append(f"{major}.{minor}.{micro}")
        if v.pre is not None:
            parts.append("".join(str(x) for x in v.pre))
        if v.post is not None:
            parts.append(f".post{v.post}")
        if v.dev is not None:
            parts.append(f".dev{v.dev}")
        if v.local is not None:
            parts.append(f"+{v.local}")
        new_version = "".join(parts)

        self.update_all_versions(new_version, force)

    def version_set(self, version, force):
        if version is None:
            # sync option will send None, so take existing version.
            version = self.project.project.pbt_project_dict['version']
            force = True
        self.update_all_versions(version, force)

    def tag(self, repo_path, no_push=False, branch=None, custom=None):
        repo = git.Repo(repo_path)

        if custom:
            tag = custom
        else:
            tag = self.project.project.pbt_project_dict['version']
            if branch is None:
                tag = repo.active_branch.name + "/" + tag
            elif branch != "":
                tag = branch + "/" + tag

        log(f"Setting tag to: {tag}")

        repo.create_tag(tag)

        if not no_push:
            # Pushing the tag to the remote repository
            origin = repo.remote(name='origin')
            origin.push(tag)
            log("Pushing tag to remote")
