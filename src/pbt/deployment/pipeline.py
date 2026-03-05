import ast
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import threading
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

from . import JobData, get_python_commands
from .utils import get_maven_opts
from .uploader.PipelineUploaderManager import PipelineUploadManager
from ..client.nexus import NexusClient
from ..deployment.jobs.airflow import AirflowJobDeployment
from ..deployment.jobs.databricks import DatabricksJobsDeployment
from ..entities.project import Project
from ..utility import Either, custom_print as log, is_online_mode
from ..utils.constants import PYTHON_LANGUAGE, SCALA_LANGUAGE
from ..utils.exceptions import ProjectBuildFailedException
from ..utils.project_config import DeploymentMode, ProjectConfig
from ..utils.project_models import Colors, Operation, Status, StepMetadata, StepType


_SCALA_JAR_SUFFIX_RE = re.compile(r"_2\.(12|13)\.jar$")


def _extract_scala_version_from_jar_path(jar_path: str) -> Optional[str]:
    m = _SCALA_JAR_SUFFIX_RE.search(jar_path)
    return f"2.{m.group(1)}" if m else None


def get_required_scala_versions(project: Project) -> Dict[str, List[str]]:
    pipeline_versions: Dict[str, set] = {}

    for job_id in project.jobs:
        raw_db = project.load_databricks_job(job_id)
        try:
            db_json = json.loads(raw_db)
        except Exception:
            db_json = {}
            continue

        node_name_to_pipeline = {}
        for comp in db_json.get("components", []):
            pc = comp.get("PipelineComponent")
            if not pc:
                continue
            node_name = pc.get("nodeName", "")
            pipeline_id = pc.get("pipelineId", "")
            if node_name and pipeline_id:
                node_name_to_pipeline[node_name] = pipeline_id

        task_key_to_jars = {}
        for task in db_json.get("request", {}).get("tasks", []):
            tk = task.get("task_key", "")
            jars = [lib["jar"] for lib in task.get("libraries", []) if "jar" in lib and lib["jar"]]
            if jars:
                task_key_to_jars[tk] = jars

        for node_name, pipeline_id in node_name_to_pipeline.items():
            for jp in task_key_to_jars.get(node_name, []):
                sv = _extract_scala_version_from_jar_path(jp)
                if sv:
                    pipeline_versions.setdefault(pipeline_id, set()).add(sv)

    return {pid: sorted(versions) for pid, versions in pipeline_versions.items()}


class PipelineDeployment:
    def __init__(
        self,
        project: Project,
        databricks_jobs: DatabricksJobsDeployment,
        airflow_jobs: AirflowJobDeployment,
        project_config: ProjectConfig,
        job_ids: Optional[List[str]] = None,
        pipelines_to_build: Optional[List[str]] = None,
    ):
        self.job_ids = job_ids
        self.pipelines_to_build = pipelines_to_build

        self.databricks_jobs = databricks_jobs
        self.airflow_jobs = airflow_jobs

        self.project = project
        self.project_config = project_config
        self.deployment_mode = project_config.configs_override.mode
        self.are_tests_enabled = project_config.configs_override.tests_enabled

        self.pipeline_id_to_local_path = {}
        self.has_pipelines = False  # in case deployment doesn't have any pipelines.

        self._scala_versions_per_pipeline: Dict[str, List[str]] = (
            get_required_scala_versions(project) if project.project_language == SCALA_LANGUAGE else {}
        )

    @property
    def _all_jobs(self) -> Dict[str, JobData]:
        return {**self.databricks_jobs.valid_databricks_jobs, **self.airflow_jobs.valid_airflow_jobs}

    def _get_scala_versions_for_pipeline(self, pipeline_id: str) -> List[str]:
        return self._scala_versions_per_pipeline.get(pipeline_id, ["2.12"])

    def summary(self):
        summary = []
        for pipeline_id, pipeline_name in self.pipelines_to_build_and_upload():
            if self.project.project_language == SCALA_LANGUAGE:
                for sv in self._get_scala_versions_for_pipeline(pipeline_id):
                    summary.append(f"Pipeline {pipeline_id} (Scala {sv}) will be build and uploaded.")
            else:
                summary.append(f"Pipeline {pipeline_id} will be build and uploaded.")
        return summary

    def headers(self):
        headers = []
        for pipeline_id, pipeline_name in self.pipelines_to_build_and_upload():
            if self.project.project_language == SCALA_LANGUAGE:
                for sv in self._get_scala_versions_for_pipeline(pipeline_id):
                    step_id = f"{pipeline_id}_scala_{sv}"
                    headers.append(
                        StepMetadata(
                            step_id, f"Build {pipeline_name} pipeline (Scala {sv})", Operation.Build, StepType.Pipeline
                        )
                    )
            else:
                headers.append(
                    StepMetadata(pipeline_id, f"Build {pipeline_name} pipeline", Operation.Build, StepType.Pipeline)
                )
        return headers

    def _build_and_upload_online(self, pipeline_jobs):
        responses = []
        max_workers = os.getenv("PBT_MAX_WORKERS", 2)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.build_and_upload_pipeline, pipeline_id, pipeline_name): (
                    pipeline_id,
                    pipeline_name,
                )
                for pipeline_id, pipeline_name in pipeline_jobs
            }

            for future in as_completed(futures):
                responses.append(future.result())
        return responses

    def _build_and_upload_offline(self, pipeline_jobs):
        responses = []
        for index, (pipeline_id, pipeline_name) in enumerate(pipeline_jobs, start=1):
            try:
                log(
                    f"{Colors.OKBLUE}\nWorking on pipeline {pipeline_id} [{index}/{len(pipeline_jobs)}]{Colors.ENDC}\n\n",
                    indent=1,
                )
                response = self.build_and_upload_pipeline(pipeline_id, pipeline_name)
                responses.append(response)
            except Exception as exc:
                log(
                    f"{Colors.FAIL}Pipeline {pipeline_id} generated an exception: {exc}{Colors.ENDC}",
                    step_id=pipeline_id,
                    indent=1,
                )
        return responses

    def build_and_upload(self):
        use_threads = is_online_mode()

        required_pipelines = self.pipelines_to_build_and_upload()

        if len(required_pipelines) > 0:
            log(f"\n\n{Colors.OKCYAN}Building {len(self._pipeline_to_list_fabrics)} Pipelines {Colors.ENDC}\n")

        if use_threads and len(required_pipelines) > 1:
            return self._build_and_upload_online(required_pipelines)

        else:
            return self._build_and_upload_offline(required_pipelines)

    def build_and_upload_pipeline(self, pipeline_id, pipeline_name) -> Either:
        if self.project.project_language != SCALA_LANGUAGE:
            log(step_id=pipeline_id, step_status=Status.RUNNING)

        all_available_fabrics = set(self.project_config.fabric_config.list_all_fabrics())
        relevant_fabrics_for_pipeline = [
            fabric for fabric in self._pipeline_to_list_fabrics.get(pipeline_id) if fabric in all_available_fabrics
        ]
        scala_versions = (
            self._get_scala_versions_for_pipeline(pipeline_id)
            if self.project.project_language == SCALA_LANGUAGE
            else []
        )
        pipeline_builder = PackageBuilderAndUploader(
            self.project,
            pipeline_id,
            pipeline_name,
            self.project_config,
            are_tests_enabled=self.are_tests_enabled,
            fabrics=relevant_fabrics_for_pipeline,
            scala_versions=scala_versions,
        )
        return pipeline_builder.build_and_upload_pipeline()

    def deploy(self):
        return self.build_and_upload()

    def validate(self, treat_warning_as_errors: bool = False):
        all_pipelines = self._pipeline_components_from_jobs()
        log(f"{Colors.OKBLUE} Validating pipelines {Colors.ENDC}")

        overall_validate_status = True

        for pipeline_id, pipeline_name in all_pipelines.items():
            log(f"\n\nValidating pipeline {pipeline_name} \n")
            rdc = self.project.load_pipeline_folder(pipeline_id)
            workflow = rdc.get(".prophecy/workflow.latest.json", None)

            num_errors = 0
            num_warnings = 0

            if workflow is None:
                log(f"\n{Colors.FAIL}Empty Pipeline Found: {pipeline_name}!{Colors.ENDC}")
            else:
                workflow_json = json.loads(workflow)
                if "diagnostics" in workflow_json:
                    diagnostics = workflow_json["diagnostics"]
                    for diagnostic in diagnostics:
                        if diagnostic.get("severity") == 1:
                            log(f"\n{Colors.FAIL} {pipeline_name}: {diagnostic.get('message')}{Colors.ENDC}")
                            num_errors += 1
                        elif diagnostic.get("severity") == 2:
                            log(f"\n{Colors.WARNING} {pipeline_name}: {diagnostic.get('message')}{Colors.ENDC}")
                            num_warnings += 1
                    log(f"\n{pipeline_name} has {num_errors} errors and {num_warnings} warnings.")
                    if num_errors > 0 or (treat_warning_as_errors and num_warnings > 0):
                        log(f"\n{Colors.FAIL}Pipeline is Broken: {pipeline_name}{Colors.ENDC}")
                        overall_validate_status = False
                else:
                    log(f"\n{Colors.OKBLUE} Pipeline is validated: {pipeline_name}{Colors.ENDC}")

        if not overall_validate_status:
            sys.exit(1)
        else:
            sys.exit(0)

    def test(self):
        log(f"{Colors.OKBLUE}Testing pipelines{Colors.ENDC}")

        responses = {}
        for pipeline_id, pipeline_name in self.pipelines_to_build_and_upload():
            log(f"{Colors.OKGREEN} Testing pipeline `{pipeline_id}` {Colors.ENDC}", step_id=pipeline_id)
            log(step_id=pipeline_id, step_status=Status.RUNNING)

            pipeline_builder = PackageBuilderAndUploader(
                self.project,
                pipeline_id,
                pipeline_name,
                self.project_config,
                are_tests_enabled=True,
                fabrics=self._pipeline_to_list_fabrics.get(pipeline_id),
            )
            return_code = pipeline_builder.test()
            responses[pipeline_id] = return_code
        for pipeline_id, return_code in responses.items():
            if return_code in (0, 5):
                log(f"{Colors.OKGREEN} Pipeline test succeeded : `{pipeline_id}` {Colors.ENDC}")
            else:
                log(f"{Colors.FAIL} Pipeline test failed : `{pipeline_id}`  {Colors.ENDC}")

    def _add_maven_dependency_info_python(self):
        if self.project.pbt_project_dict.get("language", "") == PYTHON_LANGUAGE:

            def _generate_pom_from_dicts(d_list):
                # Create the root element
                project = ET.Element(
                    "project",
                    {
                        "xmlns": "http://maven.apache.org/POM/4.0.0",
                        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
                        "xsi:schemaLocation": "http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd",
                    },
                )

                # project placeholders
                ET.SubElement(project, "modelVersion").text = "4.0.0"
                ET.SubElement(project, "groupId").text = "placeholder"
                ET.SubElement(project, "artifactId").text = "placeholder"
                ET.SubElement(project, "version").text = "0.0.1"

                # Repositories
                repositories_elem = ET.SubElement(project, "repositories")
                for dep in d_list:
                    repo_url = dep.get("repo")
                    if repo_url:
                        repository = ET.SubElement(repositories_elem, "repository")
                        ET.SubElement(repository, "id").text = "custom"
                        ET.SubElement(repository, "url").text = repo_url

                # Dependencies
                dependencies_elem = ET.SubElement(project, "dependencies")
                for dep in d_list:
                    if dep.get("type") == "coordinates":
                        dependency = ET.SubElement(dependencies_elem, "dependency")
                        coords = dep["coordinates"].split(":")
                        if len(coords) == 3:
                            ET.SubElement(dependency, "groupId").text = coords[0]
                            ET.SubElement(dependency, "artifactId").text = coords[1]
                            ET.SubElement(dependency, "version").text = coords[2]
                        if "exclusions" in dep:
                            exclusions_elem = ET.SubElement(dependency, "exclusions")
                            for ex in dep["exclusions"]:
                                exclusion = ET.SubElement(exclusions_elem, "exclusion")
                                group_id, artifact_id = ex.split(":")
                                ET.SubElement(exclusion, "groupId").text = group_id
                                ET.SubElement(exclusion, "artifactId").text = artifact_id

                ET.indent(project, space="    ", level=0)
                return ET.tostring(project, encoding="utf-8", method="xml")

            for pipeline_id in self.project.pipelines:
                maven_dependencies = self.project.get_maven_dependencies_for_python_pipelines(pipeline_id=pipeline_id)
                if maven_dependencies is None:
                    continue

                # include all information in pom.xml because there may be specific repos
                # or exclusions information:
                pom_xml = _generate_pom_from_dicts(maven_dependencies)
                with open(os.path.join(self.project.get_pipeline_absolute_path(pipeline_id), "pom.xml"), "w") as fd:
                    fd.write(pom_xml.decode("utf-8"))

                # write out coordinates to separate file in simple case where user just
                # wants to specify these with --packages option in spark-submit:
                coordinates_only = "\n".join([d["coordinates"] for d in maven_dependencies])
                with open(
                    os.path.join(self.project.get_pipeline_absolute_path(pipeline_id), "MAVEN_COORDINATES"), "w"
                ) as fd:
                    fd.write(coordinates_only)

                # modify setup.py to include both of these files
                setup_py_path = os.path.join(self.project.get_pipeline_absolute_path(pipeline_id), "setup.py")
                with open(setup_py_path, "r") as fd:
                    setup_py_content = fd.read()

                # WARNING: I am very intentionally using these strings in the replacement. some customers
                # are also adding their own files to the WHL which we want to keep without overwriting.
                setup_py_content_modified = setup_py_content.replace(
                    '(".prophecy", [".prophecy/workflow.latest.json"])',
                    '(".prophecy", [".prophecy/workflow.latest.json"]),("./", ["MAVEN_COORDINATES", "pom.xml"])',
                )

                with open(setup_py_path, "w") as fd:
                    fd.write(setup_py_content_modified)

    def build(
        self,
        pipeline_names: str = "",
        ignore_build_errors: bool = False,
        ignore_parse_errors: bool = False,
        add_pom_python: bool = False,
    ):
        # these can be names and ids.
        all_pipeline_ids = self._pipeline_to_list_fabrics_full_deployment.keys()
        pipeline_ids_to_name = {
            pipeline_id: self.project.get_pipeline_name(pipeline_id)
            for pipeline_id in all_pipeline_ids
            if self.project.get_pipeline_name(pipeline_id) is not None
        }

        if pipeline_names is not None and len(pipeline_names) > 0:
            pipelines_set = {pipeline for pipeline in pipeline_names.split(",")}
            pipeline_ids_to_name = {
                pipeline_id: pipeline_name
                for pipeline_id, pipeline_name in pipeline_ids_to_name.items()
                if pipeline_name in pipelines_set
            }

            pipelines_to_skip_build = {
                pipeline_id: pipeline_name
                for pipeline_id, pipeline_name in pipeline_ids_to_name.items()
                if pipeline_name not in pipelines_set
            }

            if len(pipelines_to_skip_build) > 0:
                log(
                    f"{Colors.WARNING}Skipping build for pipelines {pipelines_to_skip_build}, Please check the ids/names of provided pipelines {Colors.ENDC}"
                )

        if add_pom_python and self.project.pbt_project_dict.get("language", "") == PYTHON_LANGUAGE:
            self._add_maven_dependency_info_python()

        log(f"\n\n{Colors.OKBLUE}Building pipelines {len(pipeline_ids_to_name)}{Colors.ENDC}\n")

        build_errors = False
        for index, (pipeline_id, pipeline_name) in enumerate(pipeline_ids_to_name.items(), start=1):
            log(
                f"\n\n{Colors.OKGREEN}Building pipeline `{pipeline_name}`:[{index}/{len(pipeline_ids_to_name)}] {Colors.ENDC}\n",
                indent=1,
            )
            log(step_id=pipeline_id, step_status=Status.RUNNING)

            code = self.project.load_pipeline_folder(pipeline_id)

            if (len(code) == 0) and ignore_parse_errors:
                log(f"{Colors.WARNING}Pipeline `{pipeline_name}` code is not present {Colors.ENDC}", indent=2)
                pass

            pipeline_builder = PackageBuilderAndUploader(
                self.project,
                pipeline_id,
                pipeline_name,
                self.project_config,
                are_tests_enabled=False,
                fabrics=self._pipeline_to_list_fabrics.get(pipeline_id),
            )
            try:
                build_success = pipeline_builder.build(ignore_build_errors) == 0
            except ProjectBuildFailedException:
                build_success = False

            if not build_success:
                build_errors = True

            if build_success:
                log(f"\n{Colors.OKGREEN}Build for pipeline `{pipeline_name}` succeeded {Colors.ENDC}", indent=1)
            elif ignore_build_errors:
                log(f"\n{Colors.WARNING}Build for pipeline `{pipeline_name}` failed {Colors.ENDC}", indent=1)
            else:
                log(f"\n{Colors.FAIL}Build for pipeline `{pipeline_name}` failed {Colors.ENDC}", indent=1)

        if build_errors:
            if ignore_build_errors:
                log(f"\n{Colors.WARNING}Ignoring builds Errors as --ignore-build-errors is passed.{Colors.ENDC}")
            else:
                sys.exit(1)

    @property
    def _pipeline_to_list_fabrics_selective_job(self) -> Dict[str, List[str]]:
        pipeline_id_to_fabrics_dict = {}

        for job_id, job_data in self._all_jobs.items():
            for entity in job_data.pipeline_and_fabric_ids:
                if entity.entity_id not in pipeline_id_to_fabrics_dict:
                    pipeline_id_to_fabrics_dict[entity.entity_id] = set()

                pipeline_id_to_fabrics_dict[entity.entity_id].add(entity.fabric_id)

        return {pipeline_id: list(fabric_ids) for pipeline_id, fabric_ids in pipeline_id_to_fabrics_dict.items()}

    @property
    def _pipeline_to_list_fabrics_full_deployment(self) -> Dict[str, List[str]]:
        pipelines_to_fabrics = {}
        for pipeline_id in self.project.pipelines:
            pipelines_to_fabrics[pipeline_id] = self.project_config.fabric_config.list_all_fabrics()

        return pipelines_to_fabrics

    @property
    def _pipeline_to_list_fabrics(self) -> Dict[str, List[str]]:
        if self.deployment_mode == DeploymentMode.SelectiveJob:
            return self._pipeline_to_list_fabrics_selective_job
        elif not is_online_mode():
            return {**self._pipeline_to_list_fabrics_full_deployment, **self._pipeline_to_list_fabrics_selective_job}
        else:
            return self._pipeline_to_list_fabrics_full_deployment

    def pipelines_to_build_and_upload(self):
        all_pipelines_from_job = self._pipeline_components_from_jobs().items()
        filtered_pipelines = {
            pipeline_id: name for pipeline_id, name in all_pipelines_from_job if not pipeline_id.startswith("gitUri=")
        }
        return filtered_pipelines.items()

    def _pipeline_components_from_jobs(self):
        pipeline_components = {}

        for pipeline_id in list(self._pipeline_to_list_fabrics.keys()):
            pipeline_name = self.project.get_pipeline_name(pipeline_id)
            pipeline_components[pipeline_id] = pipeline_name

        return pipeline_components


# look at the nexus client, download the jar in target folder or dist folder and return or
# if it's not present in nexus, then build the jar upload to nexus and return it back.
class PackageBuilderAndUploader:
    def __init__(
        self,
        project: Project,
        pipeline_id: str,
        pipeline_name: str,
        project_config: ProjectConfig = None,
        are_tests_enabled: bool = False,
        fabrics: List = [],
        scala_versions: Optional[List[str]] = None,
    ):
        self._pipeline_id = pipeline_id
        self._pipeline_name = pipeline_name
        self._are_tests_enabled = are_tests_enabled
        self._project = project
        self._project_language = project.project_language
        self._base_path = project.load_pipeline_base_path(pipeline_id)
        self._project_config = project_config
        self.fabrics = fabrics
        self._scala_versions = scala_versions or ["2.12"]
        self.pipeline_upload_manager = PipelineUploadManager(
            self._project, self._project_config, self._pipeline_id, self._pipeline_name, self.fabrics
        )
        if self._project_language == PYTHON_LANGUAGE:
            self._python_cmd, self._pip_cmd = get_python_commands(self._base_path)

    def _initialize_temp_folder(self):
        rdc = self._project.load_pipeline_folder(self._pipeline_id)

        temp_dir = tempfile.mkdtemp()
        for file_name, file_content in rdc.items():
            file_path = os.path.join(temp_dir, file_name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w") as f:
                f.write(file_content)
        self._base_path = temp_dir

    def test(self):
        if self._project_language == SCALA_LANGUAGE:
            return self.mvn_test()
        else:
            return self.wheel_test()

    def build(self, ignore_build_errors: bool = False):
        if self._project_language == SCALA_LANGUAGE:
            return self.mvn_build(ignore_build_errors)
        else:
            return self.wheel_build(ignore_build_errors)

    def build_and_upload_pipeline(self) -> Either:
        pipeline_from_nexus = self._download_pipeline_from_nexus()

        if pipeline_from_nexus is not None and pipeline_from_nexus.is_right:
            return Either(right=(self._pipeline_id, pipeline_from_nexus.right))

        # trying to build and deploy
        try:
            if not is_online_mode() and self._project_config.skip_builds:
                log(
                    "Artifact File already exists or --skip-builds set, ignoring pipeline build",
                    step_id=self._pipeline_id,
                    indent=2,
                )
            else:
                self._initialize_temp_folder()
                log("Initialized temp folder for building the pipeline package.", step_id=self._pipeline_id, indent=2)

                if self._project_language == SCALA_LANGUAGE:
                    for scala_version in self._scala_versions:
                        step_id = f"{self._pipeline_id}_scala_{scala_version}"
                        profile = f"scala-{scala_version}"
                        log(step_id=step_id, step_status=Status.RUNNING)
                        log(
                            f"Building Scala pipeline with profile -P{profile}",
                            step_id=step_id,
                            indent=2,
                        )
                        self.mvn_build(scala_profile=profile, step_id=step_id)
                        jar_path = Project.get_pipeline_jar_for_scala_version(self._base_path, scala_version)
                        if jar_path:
                            log(
                                f"{Colors.OKGREEN}Built JAR: {jar_path}{Colors.ENDC}",
                                step_id=step_id,
                                indent=2,
                            )
                            self._upload_built_jar(jar_path, step_id=step_id)
                            log(step_id=step_id, step_status=Status.SUCCEEDED)
                        else:
                            log(
                                f"{Colors.FAIL}JAR for Scala {scala_version} not found in target/{Colors.ENDC}",
                                step_id=step_id,
                                indent=2,
                            )
                            log(step_id=step_id, step_status=Status.FAILED)
                            return Either(left=Exception(f"JAR for Scala {scala_version} not found after build"))
                    return Either(right=True)
                else:
                    self.wheel_build()
                log(
                    f"{Colors.OKGREEN}Pipeline package built successfully{Colors.ENDC}\n",
                    step_id=self._pipeline_id,
                    indent=2,
                )

            path = Project.get_pipeline_whl_or_jar(self._base_path)
            log(
                f"{Colors.OKBLUE}Pipeline package path: {path}{Colors.ENDC}\n",
                step_id=self._pipeline_id,
                indent=2,
            )

            if self._project_config.system_config.nexus is not None:
                log("Trying to upload pipeline package to nexus.", self._pipeline_id)
                self._uploading_to_nexus(path)

            if self._project_config.artifactory:
                log(
                    f"Trying to upload pipeline wheel {path} package to artifactory:\n {self._project_config.artifactory}.",
                    self._pipeline_id,
                )
                return self._uploading_to_artifactory(artifact_path=path)
            else:  # upload to DBFS file path
                log(f"Uploading pipeline {self._pipeline_id} from path {path} to DBFS", indent=2)
                return self._upload_pipeline(path)

        except Exception as e:
            log(
                message=f"{Colors.FAIL}Failed to build the pipeline package.{Colors.ENDC}",
                exception=e,
                step_id=self._pipeline_id,
                indent=1,
            )
            log(step_id=self._pipeline_id, step_status=Status.FAILED)
            return Either(left=e)

    def _upload_pipeline(self, path: str) -> Either:
        return self.pipeline_upload_manager.upload_pipeline(path)

    def _upload_built_jar(self, jar_path: str, step_id: Optional[str] = None):
        _step_id = step_id or self._pipeline_id
        if self._project_config.system_config.nexus is not None:
            log("Trying to upload pipeline package to nexus.", _step_id)
            self._uploading_to_nexus(jar_path)
        if self._project_config.artifactory:
            log(
                f"Trying to upload pipeline JAR {jar_path} to artifactory:\n {self._project_config.artifactory}.",
                _step_id,
            )
            self._uploading_to_artifactory(artifact_path=jar_path)
        else:
            log(f"Uploading pipeline {self._pipeline_id} from path {jar_path} to DBFS", step_id=_step_id, indent=2)
            self._upload_pipeline(jar_path)

    def _download_pipeline_from_nexus(self) -> Optional[Either]:
        if self._project_config.system_config.nexus is not None:
            log(
                "Project has nexus configured, trying to download the pipeline package.",
                step_id=self._pipeline_id,
                indent=2,
            )
            return self._download_from_nexus()
        else:
            # log("Project does not have nexus configured, building the pipeline package.", step_id=self._pipeline_id, indent=2)
            return None

    def _uploading_to_artifactory(self, artifact_path) -> Either:
        skip_upload = self._project_config.skip_artifactory_upload
        if skip_upload:
            log(f"Skipping artifactory upload as --skip-artifactory-upload passed")
            return Either(right=True)

        # Upload the wheel file to the internal Artifactory using Twine.
        username = os.getenv("ARTIFACTORY_USERNAME")
        password = os.getenv("ARTIFACTORY_PASSWORD")

        if not username or not password:
            raise EnvironmentError(
                "Artifactory credentials not found in environment variables "
                "ARTIFACTORY_USERNAME and ARTIFACTORY_PASSWORD"
            )

        artifactory_url = self._project_config.artifactory
        if artifact_path.endswith(".whl"):
            wheel_file = artifact_path
            upload_command = [
                "twine",
                "upload",
                "--repository-url",
                artifactory_url,
                "-u",
                username,
                "-p",
                password,
                wheel_file,
            ]
            log(f"Uploading wheel file {wheel_file} to Artifactory at {artifactory_url}")
            response_code = subprocess.run(upload_command)
            if response_code.returncode != 0:
                log(f"Twine upload failed for {wheel_file}")
                raise Exception(f"Twine upload failed for {wheel_file}")
            log("Wheel file uploaded to Artifactory.")
            return Either(right=True)
        elif artifact_path.endswith(".jar"):
            return NotImplementedError("Maven repository support is not added yet")
        else:
            return Exception(f"Invalid file {artifact_path}, supported file types [.jar, .whl]")

    def _uploading_to_nexus(self, upload_path):
        try:
            client = NexusClient.initialize_nexus_client(self._project_config)
            client.upload_file(
                upload_path,
                self._project.project_id,
                self._pipeline_id,
                self._project.release_version,
                self._get_package_name(),
            )
            log("Pipeline uploaded to nexus.", step_id=self._pipeline_id)
        except Exception as e:
            log("Failed to upload pipeline to nexus", e, self._pipeline_id)

    def _download_from_nexus(self):
        try:
            client = NexusClient.initialize_nexus_client(self._project_config)
            package_name = self._get_package_name()
            response = client.download_file(
                package_name, self._project.project_id, self._project.release_version, self._pipeline_id
            )
            log("Pipeline downloaded from nexus.", step_id=self._pipeline_id)
            return Either(right=response)
        except Exception as e:
            log("Failed to download pipeline from nexus", e, self._pipeline_id)
            return Either(left=e)

    def _get_package_name(self):
        if self._project_language == SCALA_LANGUAGE:
            return f"{self._pipeline_name}.jar"
        else:
            # todo combine in a single regex
            regex_match = r"[^\w\d.]+"
            underscore_regex = r"(_)\1+"
            result = re.sub(regex_match, "_", self._pipeline_name)
            result = re.sub(underscore_regex, "_", result)
            return f"{result}-1.0-py3-none-any.whl"

    def mvn_build(
        self, ignore_build_errors: bool = False, scala_profile: Optional[str] = None, step_id: Optional[str] = None
    ):
        mvn = "mvn"
        command = (
            [mvn, "package", "-DskipTests"] if not self._are_tests_enabled else [mvn, "package", "-Dfabric=default"]
        )
        if scala_profile is not None:
            command.extend([f"-P{scala_profile}"])

        _step_id = step_id or self._pipeline_id
        log(f"Running mvn command {command}", step_id=_step_id, indent=2)

        return self._build(command, ignore_build_errors, step_id=_step_id)

    def mvn_test(self):
        mvn = "mvn"
        command = [mvn, "package", "-Dfabric=default"]
        log(f"Running mvn command {command}", step_id=self._pipeline_id, indent=2)

        return self._build(command)

    def get_python_dependencies(self):
        log(f"{Colors.OKBLUE}Getting python dependencies for {self._pipeline_name} {Colors.ENDC}")

        def _extract_install_requires_from_setup(setup_py_path):
            with open(setup_py_path, "r") as file:
                setup_code = file.read()
            tree = ast.parse(setup_code)
            install_requires = []
            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "setup":
                    for keyword in node.keywords:
                        if keyword.arg == "install_requires" and isinstance(keyword.value, ast.List):
                            for elt in keyword.value.elts:
                                s = getattr(elt, "s", None) or (
                                    elt.value if isinstance(elt, ast.Constant) and isinstance(elt.value, str) else None
                                )
                                if s:
                                    install_requires.append(s)
                        elif keyword.arg == "extras_require" and isinstance(keyword.value, ast.Dict):
                            for key, value in zip(keyword.value.keys, keyword.value.values):
                                key_s = getattr(key, "s", None) or (
                                    key.value if isinstance(key, ast.Constant) and isinstance(key.value, str) else None
                                )
                                if key_s == "test" and isinstance(value, ast.List):
                                    for elt in value.elts:
                                        s = getattr(elt, "s", None) or (
                                            elt.value
                                            if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
                                            else None
                                        )
                                        if s:
                                            install_requires.append(s)
            return install_requires

        requirements = _extract_install_requires_from_setup(os.path.join(self._base_path, "setup.py"))
        try:
            log(f"{Colors.OKBLUE}Installing: {requirements} {Colors.ENDC}")
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "--disable-pip-version-check", "-q"] + requirements
            )
        except subprocess.CalledProcessError as e:
            log(f"An error occurred while trying to install requirements: {e}", step_id=self._pipeline_id)

    def get_maven_dependencies_python(self):
        if "SPARK_JARS_CONFIG" in os.environ and len(os.environ.get("SPARK_JARS_CONFIG", "")) != 0:
            # Keep this for backwards compatibility with old code.
            log(
                f"{Colors.OKBLUE}Skipping installing maven dependencies: using {os.environ['SPARK_JARS_CONFIG']}{Colors.ENDC}"
            )
            return
        maven_deps = self._project.get_maven_dependencies_for_python_pipelines(self._pipeline_id)
        if not maven_deps:
            return
        # ONLY import pyspark here if we need it because it is a heavy dependency and we don't want to import it if we don't need it.
        import pyspark

        def _get_spark_version_for_prophecy_libs(version):
            # Only use major.minor version and set patch as 0, e.g. 3.5.2 -> 3.5.0
            parts = version.split(".")
            if len(parts) >= 2:
                return f"{parts[0]}.{parts[1]}.0"
            return version

        maven_deps_patched = []
        for d in maven_deps:
            if d["coordinates"].startswith("io.prophecy:prophecy-libs_"):
                # For prophecy-libs, use major.minor.0 in place of {{REPLACE_ME}}
                patched = d["coordinates"].replace(
                    "{{REPLACE_ME}}", _get_spark_version_for_prophecy_libs(pyspark.__version__)
                )
            maven_deps_patched.append(patched)
        maven_deps = maven_deps_patched
        log(f"{Colors.OKBLUE}Installing: {maven_deps} {Colors.ENDC}")
        for d in maven_deps:
            try:
                subprocess.check_call(["mvn", "dependency:get", f"-Dartifact={d}"])
            except subprocess.CalledProcessError as e:
                log(f"An error occurred while trying to install maven requirements: {e}", step_id=self._pipeline_id)

    def wheel_test(self):
        COVERAGERC_CONTENT = "[run]\n" "omit=test/**,build/**,dist/**,setup.py\n"

        log(f"\n\n{Colors.OKBLUE}Gathering Dependencies{Colors.ENDC}\n")
        self.get_python_dependencies()
        self.get_maven_dependencies_python()

        coveragerc_path = os.path.join(f"{self._base_path}", ".coveragerc")
        if not os.path.exists(coveragerc_path):
            with open(coveragerc_path, "w") as fd:
                fd.write(COVERAGERC_CONTENT)

        separator = os.sep
        test_command = [
            self._python_cmd,
            "-m",
            "pytest",
            "-v",
            "--cov=.",
            "--cov-report=xml",
            "--junitxml=report.xml",
            f"{self._base_path}{separator}test{separator}TestSuite.py",
        ]
        log(f"Running python test {test_command}", step_id=self._pipeline_id, indent=2)
        response_code = self._build(test_command)

        return response_code

    def wheel_build(self, ignore_build_error: bool = False):
        if self._are_tests_enabled:
            response_code = self.wheel_test()

            if response_code not in (0, 5):
                raise Exception(f"Python test failed for pipeline {self._pipeline_id}")

        case_preserved_whl_build = (
            "import sys, runpy, setuptools._normalization as norm;"
            "norm.safer_name = lambda v: norm.filename_component(norm.safe_name(v));"
            "sys.argv=['setup.py','bdist_wheel'];"
            "runpy.run_path('setup.py', run_name='__main__')"
        )

        command = [self._python_cmd, "-c", case_preserved_whl_build]

        log(f"Running python command {command}", step_id=self._pipeline_id, indent=2)

        return self._build(command, ignore_build_error)

    # maybe we can try another iteration with yield ?
    def _build(self, command: list, ignore_build_errors: bool = False, step_id: Optional[str] = None):
        _step_id = step_id or self._pipeline_id
        env = dict(os.environ)

        # Set the MAVEN_OPTS variable with environment overrides
        env["MAVEN_OPTS"] = get_maven_opts()

        if env.get("FABRIC_NAME", None) is None:
            env["FABRIC_NAME"] = "default"  # for python test runs.

        log(f"Running command {command} on path {self._base_path}", step_id=_step_id, indent=2)
        process = subprocess.Popen(
            command, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, cwd=self._base_path
        )

        def log_output(pipe, log_function):
            while True:
                # Read line from stdout or stderr, break if EOF
                output = pipe.readline()
                if process.poll() is not None and not output:
                    break
                # Decode line and print it
                response = output.decode().strip()

                # stripping unnecessary logs
                if not re.search(r"Progress \(\d+\):", response) and len(response) != 0 and response != "\n":
                    log_function(response)

        # Create threads to read and log stdout and stderr simultaneously
        stdout_thread = threading.Thread(
            target=log_output, args=(process.stdout, lambda msg: log(msg, step_id=_step_id, indent=2))
        )
        stderr_thread = threading.Thread(
            target=log_output, args=(process.stderr, lambda msg: log(msg, step_id=_step_id, indent=2))
        )

        # Start threads
        stdout_thread.start()
        stderr_thread.start()

        # Wait for both threads to finish
        stdout_thread.join()
        stderr_thread.join()

        # Get the exit code
        return_code = process.wait()

        if return_code in (0, 5):
            log(f"Build was successful with exit code {return_code}", step_id=_step_id, indent=2)
        elif ignore_build_errors:
            log(f"Build failed with exit code {return_code}", step_id=_step_id, indent=2)
        else:
            log(f"Build failed with exit code {return_code}", step_id=_step_id, indent=2)
            raise ProjectBuildFailedException(f"Build failed with exit code {return_code}")

        return return_code


# Create for Synapse / Azure
