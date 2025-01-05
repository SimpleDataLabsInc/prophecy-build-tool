import os
import re
import subprocess
import threading
from typing import List

from ..utils.constants import SCALA_LANGUAGE, PYTHON_LANGUAGE
from ..entities.project import Project
from ..utils.project_config import ProjectConfig
from ..utils.project_models import StepMetadata, StepType, Operation, Status
from ..utility import custom_print as log, Either
from . import get_python_commands

GEMS = "Gems"


class GemsDeployment:
    def __init__(self, project: Project, project_config: ProjectConfig):
        self.project = project
        self.project_config = project_config

    def _does_gems_exist(self):
        return self.project.non_empty_gems_directory() and self.project.gems

    def summary(self):
        if self._does_gems_exist():
            return ["Gems package will be build and uploaded to nexus"]
        else:
            return []

    def headers(self) -> List[StepMetadata]:
        if self._does_gems_exist():
            return [StepMetadata(GEMS, "Gems will be built and uploaded", Operation.Build, StepType.Pipeline)]
        else:
            return []

    def deploy(self):
        if self._does_gems_exist():
            log(step_status=Status.RUNNING, step_id=GEMS)
            gem_path = os.path.join(self.project.project_path, "gems")
            package_builder = PackageBuilder(gem_path, self.project.project_language)

            try:
                return_code = package_builder.build()

                if return_code == 0:
                    log(step_status=Status.SUCCEEDED, step_id=GEMS)
                    return [Either(right=True)]
                else:
                    log(step_status=Status.FAILED, step_id=GEMS)
                    return [Either(left=Exception(f"Failed to build the gems package with exit code {return_code}."))]

            except Exception as e:
                log(message="Failed to build the pipeline package.", exception=e, step_id=GEMS)
                log(step_id=GEMS, step_status=Status.FAILED)
                return [Either(left=e)]
        else:
            return []


class PackageBuilder:
    def __init__(self, path: str, language: str):
        self.path = path
        self.language = language
        if self.language == PYTHON_LANGUAGE:
            self._python_cmd, self._pip_cmd = get_python_commands(self.path)

    def build(self):
        if self.language == SCALA_LANGUAGE:
            return self.mvn_build()
        else:
            return self.wheel_build()

    def mvn_build(self):
        command = ["mvn", "deploy", "-DskipTests"]

        log(f"Running mvn command {command}", step_id=GEMS)

        return self._build(command)

    def wheel_build(self):
        build_command = [self._python_cmd, "setup.py", "bdist_wheel"]
        log(f"Running python command {build_command}", step_id=GEMS)
        build_status = self._build(build_command)
        if build_status == 0:
            upload_command = ["twine", "upload", "-r", "local", "dist/*"]
            log(f"Running python command {upload_command}", step_id=GEMS)
            return self._build(upload_command)
        else:
            return build_status

    # maybe we can try another iteration with yield ?
    def _build(self, command: list):
        env = dict(os.environ)

        # Set the MAVEN_OPTS variable
        env["MAVEN_OPTS"] = "-Xmx1024m -XX:MaxMetaspaceSize=512m -Xss32m"

        process = subprocess.Popen(
            command, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, cwd=self.path
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
                if not re.search(r"Progress \(\d+\):", response):
                    log_function(response)

        # Create threads to read and log stdout and stderr simultaneously
        stdout_thread = threading.Thread(target=log_output, args=(process.stdout, lambda msg: log(msg, step_id=GEMS)))
        stderr_thread = threading.Thread(target=log_output, args=(process.stderr, lambda msg: log(msg, step_id=GEMS)))

        # Start threads
        stdout_thread.start()
        stderr_thread.start()

        # Wait for both threads to finish
        stdout_thread.join()
        stderr_thread.join()

        # Get the exit code
        return_code = process.wait()

        if return_code == 0:
            log("Build was successful.", step_id=GEMS)
        else:
            log(f"Build failed with exit code {return_code}", step_id=GEMS)

        return return_code
