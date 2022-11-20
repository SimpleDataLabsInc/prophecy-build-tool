import collections
import json
import os
import re
import subprocess
import sys
from glob import glob
from os import listdir
from os.path import normpath, basename, isfile, join, dirname
from typing import Dict

import yaml
from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import EnvironmentVariableConfigProvider
from databricks_cli.sdk import DbfsService, JobsService
from requests import HTTPError
from rich import print

from .process import Process


class ProphecyBuildTool:
    def __init__(
        self,
        path_root: str,
        dependent_projects_path: str = "",
        release_version: str = "",
        project_id: str = "",
        prophecy_url: str = "",
    ):
        if not path_root:
            self._error("Path of project not passed as argument using --path.")
        self.operating_system = sys.platform
        self.path_root = path_root
        self.path_project = os.path.join(self.path_root, "pbt_project.yml")

        self._verify_project()
        self._verify_databricks_configs()
        self._parse_project()
        self.dependent_projects = {}
        if dependent_projects_path:
            print("\nParsing dependent projects")
            other_pbt_projects = glob(
                f"{dependent_projects_path}**/pbt_project.yml", recursive=True
            )
            for dependent_project in other_pbt_projects:
                self.dependent_projects[dirname(dependent_project)] = ProphecyBuildTool(
                    dirname(dependent_project)
                )
        config = EnvironmentVariableConfigProvider().get_config()

        self.api_client = _get_api_client(config)

        self.dbfs_service = DbfsService(self.api_client)
        self.jobs_service = JobsService(self.api_client)
        self.python_cmd, self.pip_cmd = self.get_python_commands(path_root)
        self.pipelines_build_path = {}
        self.dependent_pipelines_build_path = {}
        self.project_release = (
            release_version
            if release_version
            else os.getenv(
                "PROJECT_RELEASE_VERSION_PLACEHOLDER",
                "__PROJECT_RELEASE_VERSION_PLACEHOLDER__",
            )
        )
        self.project_id = (
            project_id
            if project_id
            else os.getenv("PROJECT_ID_PLACEHOLDER", "__PROJECT_ID_PLACEHOLDER__")
        )
        self.prophecy_url = (
            prophecy_url
            if prophecy_url
            else os.getenv("PROPHECY_URL_PLACEHOLDER", "__PROPHECY_URL_PLACEHOLDER__")
        )

    def get_python_commands(self, cwd):
        if (
            Process.process_sequential(
                [
                    Process(
                        ["python3", "--version"],
                        cwd,
                        subprocess.DEVNULL,
                        subprocess.DEVNULL,
                        (self.operating_system == "win32"),
                    )
                ]
            )
            == 0
        ):
            return "python3", "pip3"
        elif (
            Process.process_sequential(
                [
                    Process(
                        ["python", "--version"],
                        cwd,
                        subprocess.DEVNULL,
                        subprocess.DEVNULL,
                        (self.operating_system == "win32"),
                    )
                ]
            )
            == 0
        ):
            return "python", "pip"
        else:
            print("ERROR: python not found")
            sys.exit(1)

    def build(self, pipelines, exit_on_build_failure=True):
        if not pipelines:
            pipelines = self.pipelines
        print("\n[bold blue]Building %s pipelines [/bold blue]" % len(pipelines))
        overall_build_status = True
        for pipeline_i, (path_pipeline, pipeline) in enumerate(pipelines.items()):
            print(
                "\n  Building pipeline %s [%s/%s]"
                % (path_pipeline, pipeline_i + 1, len(pipelines))
            )

            path_pipeline_absolute = os.path.join(
                os.path.join(self.path_root, path_pipeline), "code"
            )
            return_code = (
                self.build_python(path_pipeline_absolute, path_pipeline)
                if self.project_language == "python"
                else self.build_scala(path_pipeline_absolute)
            )

            self.pipelines_build_path[path_pipeline] = None
            build_file_found = False
            build_file_paths = []

            if self.project_language == "python":
                path_pipeline_dist = path_pipeline_absolute + "/dist"
                build_file_paths = list(
                    filter(
                        lambda x: (x.endswith("py3-none-any.whl")),
                        glob(f"{path_pipeline_dist}/*.whl"),
                    )
                )
            elif self.project_language == "scala":
                path_pipeline_dist = path_pipeline_absolute + "/target"
                build_file_paths = list(
                    filter(
                        lambda x: ("jar-with-dependencies" not in x),
                        glob(f"{path_pipeline_dist}/*.jar"),
                    )
                )

            if len(build_file_paths) > 0:
                build_file_found = True
                self.pipelines_build_path[path_pipeline] = {
                    "source_absolute": build_file_paths[0],
                    "source": os.path.basename(build_file_paths[0]),
                    "uploaded": False,
                }

            if return_code == 0:
                if build_file_found:
                    print("\n[bold blue] Build complete![/bold blue]")
                else:
                    print(
                        f"\n[bold red] Build completed but built target not found for pipeline: {pipeline['name']}![/bold red]"
                    )
                    overall_build_status = False
            else:
                print(
                    f"\n[bold red] Build failed for pipeline: {pipeline['name']}![/bold red]"
                )
                overall_build_status = False

        if not overall_build_status and exit_on_build_failure:
            sys.exit(1)
        else:
            return overall_build_status, self.pipelines_build_path

    def deploy(self):
        self.build(dict())

        print("\n[bold blue] Deploying %s jobs [/bold blue]" % self.jobs_count)

        pipelines_upload_failures = collections.defaultdict(list)
        job_update_failures = dict()

        for job_idx, (path_job, job) in enumerate(self.jobs.items()):
            pipelines_upload_failures_job = collections.defaultdict(list)
            print(
                "\n  Deploying job %s [%s/%s]"
                % (path_job, job_idx + 1, self.jobs_count)
            )

            path_job_absolute = os.path.join(
                os.path.join(self.path_root, path_job), "code"
            )
            path_job_definition = os.path.join(path_job_absolute, "databricks-job.json")

            with open(path_job_definition, "r") as _in:
                data = _in.read()
                data = (
                    data.replace(
                        "__PROJECT_RELEASE_VERSION_PLACEHOLDER__", self.project_release
                    )
                    .replace("__PROJECT_ID_PLACEHOLDER__", self.project_id)
                    .replace("__PROPHECY_URL_PLACEHOLDER__", self.prophecy_url)
                )
            with open(path_job_definition, "w") as _out:
                _out.write(data)

            with open(path_job_definition, "r") as _in:
                job_definition = json.load(_in)

            components = job_definition["components"]
            generate_pipeline_config_from_pipeline_component = False

            for component in components:
                if "PipelineComponent" in component:
                    pipeline_uri = component["PipelineComponent"]["id"]

                    # Matches project_id/pipelines/pipeline_name or pipelines/pipeline_name
                    # group(2) should return pipelines/pipeline_name
                    uri_pattern = "([0-9]*)(pipelines/[-_.A-Za-z0-9 /]+)"
                    pipeline_id = re.search(uri_pattern, pipeline_uri).group(2)

                    if (
                        "releaseVersion" in component["PipelineComponent"]
                        or "path=" in pipeline_id
                        or pipeline_id not in self.pipelines_build_path
                    ):
                        # Check if this shared pipelineComponent has configs
                        dependent_pipeline_regex_pattern = (
                            "(^[0-9]+?\/pipelines\/[-_.A-Za-z0-9 \/]+)$"
                            "|^.*projectSubscriptionProjectId=([0-9]+).*path=([-_.A-Za-z0-9 \/]+).*$"
                            "|^.*path=([-_.A-Za-z0-9 \/]+).*projectSubscriptionProjectId=([0-9]+).*$"
                        )

                        search_regex_id = re.search(
                            dependent_pipeline_regex_pattern, pipeline_uri
                        )
                        if (
                            bool(search_regex_id)
                            and "configPath" in component["PipelineComponent"]
                        ):
                            generate_pipeline_config_from_pipeline_component = True
                            shared_pipeline_id = ""
                            if bool(search_regex_id.group(1)):
                                shared_pipeline_id = search_regex_id.group(1)
                            elif bool(search_regex_id.group(2)) and bool(
                                search_regex_id.group(3)
                            ):
                                shared_pipeline_id = f"{search_regex_id.group(2)}/{search_regex_id.group(3)}"
                            elif bool(search_regex_id.group(4)) and bool(
                                search_regex_id.group(5)
                            ):
                                shared_pipeline_id = f"{search_regex_id.group(5)}/{search_regex_id.group(4)}"
                            else:
                                generate_pipeline_config_from_pipeline_component = False
                            self.pipeline_to_dbfs_config_path[
                                shared_pipeline_id
                            ] = component["PipelineComponent"]["configPath"]

                        print(
                            f"    Pipeline {pipeline_id} might be shared, checking if it exists in DBFS"
                        )

                        # This jar is from a shared pipeline, check if jar exists in DBFS
                        try:
                            if self.dbfs_service.get_status(
                                component["PipelineComponent"]["path"]
                            ):
                                print(
                                    "    Dependent package exists on DBFS already, continuing with next pipeline"
                                )
                                continue
                        except HTTPError as e:
                            dependent_build_jar_found = False
                            if e.response.status_code == 404:
                                # Pipeline not found on DBFS. Check if it is present in dependency folder
                                # and try to build it ourselves
                                print(
                                    f"    Pipeline {pipeline_id} not found in DFBS, searching in dependent project directory"
                                )
                                for project in self.dependent_projects.values():
                                    if pipeline_id in project.pipelines:
                                        print(
                                            "    Building dependent project's pipeline:"
                                        )
                                        (
                                            dependent_build_status,
                                            dependent_build_paths,
                                        ) = project.build(
                                            {
                                                k: v
                                                for (k, v) in project.pipelines.items()
                                                if pipeline_id in project.pipelines
                                            },
                                            False,
                                        )
                                        if dependent_build_status:
                                            dependent_build_jar_found = True
                                            self.dependent_pipelines_build_path[
                                                pipeline_id
                                            ] = dependent_build_paths[pipeline_id]
                            if not dependent_build_jar_found:
                                pipelines_upload_failures_job[pipeline_id].append(
                                    e.response.text
                                )
                                pipelines_upload_failures[pipeline_id].append(
                                    e.response.text
                                )
                        except Exception as ex:
                            self.pipelines_build_path[pipeline_id]["uploaded"] = False
                            pipelines_upload_failures_job[pipeline_id].append(str(ex))
                            pipelines_upload_failures[pipeline_id].append(str(ex))
                    if (
                        pipeline_id in self.pipelines_build_path
                        or pipeline_id in self.dependent_pipelines_build_path
                    ):
                        pipelines_build_path = (
                            self.pipelines_build_path
                            if pipeline_id in self.pipelines_build_path
                            else self.dependent_pipelines_build_path
                        )
                        source_path = pipelines_build_path[pipeline_id][
                            "source_absolute"
                        ]
                        target_path = component["PipelineComponent"]["path"]
                        if not pipelines_build_path[pipeline_id]["uploaded"]:
                            print(
                                "    Uploading %s to %s"
                                % (
                                    pipelines_build_path[pipeline_id]["source"],
                                    target_path,
                                )
                            )

                            try:
                                self.dbfs_service.put(
                                    target_path, overwrite=True, src_path=source_path
                                )
                                pipelines_build_path[pipeline_id]["uploaded"] = True
                            except HTTPError as e:
                                pipelines_upload_failures_job[pipeline_id].append(
                                    e.response.text
                                )
                                pipelines_upload_failures[pipeline_id].append(
                                    e.response.text
                                )
                            except Exception as ex:
                                pipelines_build_path[pipeline_id]["uploaded"] = False
                                pipelines_upload_failures_job[pipeline_id].append(
                                    str(ex)
                                )
                                pipelines_upload_failures[pipeline_id].append(str(ex))

            job_request = job_definition["request"]
            if "CreateNewJobRequest" in job_request.keys():
                job_request = job_definition["request"]["CreateNewJobRequest"]

            job_request["version"] = "2.1"
            # Upload configuration json files to dbfs

            local_path_dbfs_map = dict()
            if generate_pipeline_config_from_pipeline_component:
                for (
                    base_pipeline_id,
                    local_config_path,
                ) in self.pipeline_to_local_config_path.items():
                    local_path_dbfs_map[
                        local_config_path
                    ] = self.pipeline_to_dbfs_config_path[base_pipeline_id]
            else:
                json_configs = self._get_spark_parameter_files(
                    job_request["tasks"], ".json"
                )

                local_path_dbfs_map = self._construct_local_config_to_dbfs_config_path(
                    json_configs
                )

            for local_json_directory, dbfs_directory in local_path_dbfs_map.items():
                files = [
                    join(local_json_directory, f)
                    for f in listdir(local_json_directory)
                    if isfile(join(local_json_directory, f))
                ]
                for file_path in files:
                    dbfs_file_path = join(dbfs_directory, basename(file_path))
                    try:
                        print(f"    Uploading {file_path} to {dbfs_file_path}")
                        self.dbfs_service.put(
                            dbfs_file_path, overwrite=True, src_path=file_path
                        )
                    except Exception as e:
                        print(
                            f"    Failed to upload {file_path} to {dbfs_file_path}. Exception: {e}"
                        )

            if len(pipelines_upload_failures_job) > 0:
                print(
                    "\n[bold red] Package upload failed or doesn't exist for one or more pipelines [/bold red]"
                )
                for pipeline_id, errors in pipelines_upload_failures_job.items():
                    print(
                        "\n[bold red] %s: Exceptions: %s [/bold red]"
                        % (pipeline_id, "\n".join(errors))
                    )

                # Process next job
                continue

            limit = 25
            current_offset = 0
            found_job = None
            try:
                while found_job is None:
                    print(
                        f"    Querying existing jobs to find current job: Offset: {current_offset}, Pagesize: {limit}"
                    )
                    response = self.jobs_service.list_jobs(
                        limit=limit, offset=current_offset, version="2.1"
                    )
                    current_offset += limit

                    found_jobs = response["jobs"] if "jobs" in response else []
                    for potential_found_job in found_jobs:
                        if (
                            potential_found_job["settings"]["name"]
                            == job_request["name"]
                        ):
                            found_job = potential_found_job
                            break

                    if len(found_jobs) <= 0:
                        break

                job_request = job_definition["request"]
                if "CreateNewJobRequest" in job_request.keys():
                    job_request = job_definition["request"]["CreateNewJobRequest"]

                if found_job is None:
                    print("    Creating a new job: %s" % (job_request["name"]))
                    self.jobs_service.create_job(**job_request)
                else:
                    print("    Updating an existing job: %s" % (job_request["name"]))
                    self.jobs_service.reset_job(
                        found_job["job_id"], new_settings=job_request, version="2.1"
                    )
            except Exception as e:
                print(
                    f"\n[bold red] Create/Update for job: {job_request['name']} failed with exception: {e} [/bold red]"
                )
                job_update_failures[job_request["name"]] = str(e)

        if len(pipelines_upload_failures) == 0 and len(job_update_failures) == 0:
            print("\n[bold blue] Deployment completed successfully![/bold blue]")
        else:
            print("\n[bold red] Deployment failed![/bold red]")
            if len(pipelines_upload_failures) > 0:
                print(
                    "   Pipeline failures: %s"
                    % (" ,".join(pipelines_upload_failures.keys()))
                )
            if len(job_update_failures) > 0:
                print(
                    "   Create/Update failed for jobs: %s"
                    % (" ,".join(job_update_failures.keys()))
                )
            sys.exit(1)

    def test(self):
        if self._verify_unit_test_env():
            unit_test_results = {}

            for pipeline_i, (path_pipeline, pipeline) in enumerate(
                self.pipelines.items()
            ):
                print(
                    "\n  Unit Testing pipeline %s [%s/%s]"
                    % (path_pipeline, pipeline_i + 1, self.pipelines_count)
                )

                path_pipeline_absolute = os.path.join(
                    os.path.join(self.path_root, path_pipeline), "code"
                )
                if self.project_language == "python":
                    if os.path.isfile(
                        os.path.join(path_pipeline_absolute, "test/TestSuite.py")
                    ):
                        unit_test_results[path_pipeline] = self.test_python(
                            path_pipeline_absolute, path_pipeline
                        )
                elif self.project_language == "scala":
                    unit_test_results[path_pipeline] = self.test_scala(
                        path_pipeline_absolute
                    )
            is_any_ut_failed = False
            for path_pipeline, return_code in unit_test_results.items():
                if return_code not in (0, 5):
                    is_any_ut_failed = True
                    print(
                        f"\n[bold red] Unit test for pipeline: {path_pipeline} failed with return code {return_code}![/bold red]"
                    )
                else:
                    print(
                        f"\n[bold blue] Unit test for pipeline: {path_pipeline} succeeded.[/bold blue]"
                    )

            if is_any_ut_failed:
                sys.exit(1)
        else:
            sys.exit(1)

    def get_python_dependencies(self, path_pipeline_absolute, path_pipeline):
        if (
            Process.process_sequential(
                [
                    # Export dependencies to egg_info/requires.txt
                    Process(
                        [self.python_cmd, "setup.py", "-q", "egg_info"],
                        path_pipeline_absolute,
                        is_shell=(self.operating_system == "win32"),
                        running_message="    Getting Python dependencies...",
                    )
                ]
            )
            == 0
        ):
            python_dependencies = []
            egg_info_dir = f"{basename(normpath(path_pipeline))}.egg-info"
            egg_info_requires = os.path.join(
                *[path_pipeline_absolute, egg_info_dir, "requires.txt"]
            )
            for line in open(egg_info_requires, "r"):
                if not line.lstrip().startswith("[") and line.strip():
                    python_dependencies.append(line.strip())
            return python_dependencies
        else:
            print("Failed to get python dependencies")
            sys.exit(1)

    def build_python(self, path_pipeline_absolute, path_pipeline):
        python_dependencies = self.get_python_dependencies(
            path_pipeline_absolute, path_pipeline
        )
        return Process.process_sequential(
            [
                # Extract the install_requires and extra_requires and install them
                Process(
                    [self.pip_cmd, "install", "-q"] + python_dependencies,
                    path_pipeline_absolute,
                    is_shell=(self.operating_system == "win32"),
                ),
                # Check for compilation errors
                Process(
                    [self.python_cmd, "-m", "compileall", ".", "-q"],
                    path_pipeline_absolute,
                    is_shell=(self.operating_system == "win32"),
                ),
                # Generate wheel
                Process(
                    [self.python_cmd, "setup.py", "bdist_wheel"],
                    path_pipeline_absolute,
                    is_shell=(self.operating_system == "win32"),
                ),
            ]
        )

    def build_scala(self, path_pipeline_absolute):
        return Process.process_sequential(
            [
                Process(
                    ["mvn", "clean", "package", "-q", "-DskipTests"],
                    path_pipeline_absolute,
                    is_shell=(self.operating_system == "win32"),
                )
            ]
        )

    def test_scala(self, path_pipeline_absolute):
        return Process.process_sequential(
            [
                Process(
                    ["mvn", "test", "-q", "-Dfabric=" + self.fabric.strip()],
                    path_pipeline_absolute,
                    is_shell=(self.operating_system == "win32"),
                )
            ]
        )

    def test_python(self, path_pipeline_absolute, path_pipeline):
        return Process.process_sequential(
            [
                # Install dependencies of particular pipeline
                # 1. Export dependencies to egg_info/requires.txt
                Process(
                    [self.python_cmd, "setup.py", "-q", "egg_info"],
                    path_pipeline_absolute,
                    is_shell=(self.operating_system == "win32"),
                ),
                # 2. Extract the install_requires and extra_requires and install them
                Process(
                    [self.pip_cmd, "install", "-q"]
                    + self.get_python_dependencies(
                        path_pipeline_absolute, path_pipeline
                    ),
                    path_pipeline_absolute,
                    is_shell=(self.operating_system == "win32"),
                ),
                # Run the unit test
                Process(
                    [self.python_cmd, "-m", "pytest", "-v", "test/TestSuite.py"],
                    path_pipeline_absolute,
                    is_shell=(self.operating_system == "win32"),
                ),
            ]
        )

    def _verify_unit_test_env(self):
        self.fabric = os.environ.get("FABRIC_NAME")

        if self.fabric is None:
            self._error(
                "[i]FABRIC_NAME[/i] environment variable is required to "
                "run your Unit tests"
            )
            return False
        else:
            return True

    def _parse_project(self):
        self.pipelines: Dict = {}
        self.jobs: Dict = {}
        with open(self.path_project, "r") as _in:
            self.project = yaml.safe_load(_in)
            self.jobs = self.project["jobs"]
            self.pipelines = self.project["pipelines"]
            self.project_language = self.project["language"]
            self.pipeline_configurations = dict(
                self.project.get("pipelineConfigurations", [])
            )
            self.pipeline_to_local_config_path = {}
            self.pipeline_to_dbfs_config_path = {}
            for (
                pipeline_config_path,
                pipeline_config_object,
            ) in self.pipeline_configurations.items():
                self.pipeline_to_local_config_path[
                    pipeline_config_object["basePipeline"]
                ] = os.path.join(self.path_root, pipeline_config_path)
        if self.project_language not in ("python", "scala"):
            self._error(
                f"Language: [i]{self.project_language}[/i] not supported by Prophecy-Build-Tool right now."
            )

        self.pipelines_count = len(self.pipelines)
        self.jobs_count = len(self.jobs)

        jobs_str = ", ".join(map(lambda job: job["name"], self.jobs.values()))
        print("Found %s jobs: %s" % (self.jobs_count, jobs_str))

        pipelines_str = ", ".join(
            map(
                lambda pipeline: "%s (%s)" % (pipeline["name"], pipeline["language"]),
                self.pipelines.values(),
            )
        )
        print("Found %s pipelines: %s" % (self.pipelines_count, pipelines_str))
        self._verify_project_structure()

    def _verify_project_structure(self):
        for path_pipeline, pipeline in self.pipelines.items():
            path_pipeline_absolute = os.path.join(
                os.path.join(self.path_root, path_pipeline), "code"
            )
            pipeline_dependencies_file = (
                "setup.py" if self.project_language == "python" else "pom.xml"
            )

            if not os.path.isfile(
                os.path.join(path_pipeline_absolute, pipeline_dependencies_file)
            ):
                print(
                    f"\n[bold red]Pipeline {path_pipeline} does not exist or is corrupted. [/bold red]"
                )
                sys.exit(1)

        for path_job, job in self.jobs.items():
            path_job_definition = os.path.join(
                os.path.join(self.path_root, path_job), "code/databricks-job.json"
            )

            if not os.path.isfile(path_job_definition):
                print(
                    f"\n[bold red]Job {path_job} does not exist or is corrupted. [/bold red]"
                )
                sys.exit(1)

    def _get_spark_parameter_files(self, tasks_list, file_extension):
        result = []
        for task in tasks_list:
            params = [
                file.replace("/dbfs/", "dbfs:/")
                for file in list(task["spark_jar_task"]["parameters"])
                if file.endswith(file_extension)
            ]
            result += params
        return result

    def _construct_local_config_to_dbfs_config_path(self, json_configs):
        local_config_to_dbfs_config_path_map = {}
        for json_config in json_configs:
            for (
                pipeline_id,
                pipeline_config_path,
            ) in self.pipeline_to_local_config_path.items():
                if pipeline_id in json_config:
                    local_config_to_dbfs_config_path_map[
                        pipeline_config_path
                    ] = os.path.dirname(json_config)

        return local_config_to_dbfs_config_path_map

    @classmethod
    def _verify_databricks_configs(cls):
        host = os.environ.get("DATABRICKS_HOST")
        token = os.environ.get("DATABRICKS_TOKEN")

        if host is None or token is None:
            cls._error(
                "[i]DATABRICKS_HOST[/i] & [i]DATABRICKS_TOKEN[/i] environment variables are required to "
                "deploy your Databricks Workflows"
            )

    def _verify_project(self):
        if not os.path.isfile(self.path_project):
            self._error(
                "Missing [i]pbt_project.yml[/i] file. Are you sure you pointed pbt into a Prophecy project? "
                "Current path [u]%s[/u]" % self.path_root
            )

    @classmethod
    def _error(cls, message: str):
        print("[bold red]ERROR[/bold red]:", message)
        sys.exit(1)
