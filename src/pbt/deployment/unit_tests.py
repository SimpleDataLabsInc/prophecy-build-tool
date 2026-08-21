import glob
import os
import shutil
import subprocess
import tempfile
import threading
from typing import List

from . import get_python_commands
from ..entities.project import Project
from ..utility import Either, custom_print as log
from ..utils.constants import PYPROJECT_FILE_NAME, SQL_LANGUAGE
from ..utils.project_config import ProjectConfig
from ..utils.project_models import Colors, Operation, Status, StepMetadata, StepType

UNIT_TESTS = "run-unit-tests"

# pytest exits 5 when it collected nothing; pbt treats that as a pass everywhere else too.
_PYTEST_NO_TESTS_COLLECTED = 5


class ProjectUnitTests:
    """Runs a SQL project's generated PySpark unit tests as part of a release.

    A SQL project keeps its generated PySpark code under `src/pipelines/<name>/` and the unit
    tests generated for it under `src/pipelines/<name>/tests/`. Those pipelines are declared in
    `pbt_project.yml` under `sqlOrchestratorPipelines:`, never under `pipelines:`, so
    `PipelineDeployment` -- which drives the per-pipeline `mvn`/`wheel` test runs for Spark
    projects -- iterates an empty dict for them. That left the project's "Enable Unit Tests"
    setting a no-op for SQL projects: the tests never ran, nothing said so, and a failing test
    could not fail the release.
    """

    def __init__(self, project: Project, project_config: ProjectConfig):
        self.project = project
        self.project_config = project_config
        self.are_tests_enabled = project_config.configs_override.tests_enabled

    def _is_supported(self) -> bool:
        # SQL projects only. Spark python/scala projects run their unit tests through
        # PackageBuilderAndUploader as part of the wheel/jar build and must not run twice.
        return self.project.project_language == SQL_LANGUAGE and os.path.exists(
            os.path.join(self.project.project_path, PYPROJECT_FILE_NAME)
        )

    def test_directories(self) -> List[str]:
        if not self._is_supported():
            return []

        directories = set()
        for pattern in ("test_*.py", "*_test.py"):
            matches = glob.glob(
                os.path.join(self.project.project_path, "src", "**", "tests", "**", pattern), recursive=True
            )
            directories.update(os.path.dirname(match) for match in matches)

        return sorted(directories)

    def _should_run(self) -> bool:
        return self.are_tests_enabled and len(self.test_directories()) > 0

    def summary(self) -> List[str]:
        if not self._is_supported():
            return []

        if not self.are_tests_enabled:
            return ["Unit tests are disabled for this project, no unit tests will run during this release."]

        directories = self.test_directories()
        if len(directories) == 0:
            return ["Unit tests are enabled but this project has no unit tests, none will run during this release."]

        return [f"Unit tests are enabled, unit tests from {len(directories)} folder(s) will run before deployment."]

    def headers(self) -> List[StepMetadata]:
        if self._should_run():
            return [StepMetadata(UNIT_TESTS, "Run unit tests", Operation.Build, StepType.Pipeline)]
        else:
            return []

    def deploy(self) -> List[Either]:
        if not self._should_run():
            return []

        log(step_status=Status.RUNNING, step_id=UNIT_TESTS)
        package_directory = tempfile.mkdtemp(prefix="prophecy-unit-tests-")

        try:
            return_code = self._run_tests(package_directory)
        except Exception as e:
            log(message="Failed to run the unit tests.", exception=e, step_id=UNIT_TESTS)
            log(step_status=Status.FAILED, step_id=UNIT_TESTS)
            return [Either(left=e)]
        finally:
            shutil.rmtree(package_directory, ignore_errors=True)

        if return_code in (0, _PYTEST_NO_TESTS_COLLECTED):
            log(f"{Colors.OKGREEN}Unit tests passed{Colors.ENDC}", step_id=UNIT_TESTS, indent=2)
            log(step_status=Status.SUCCEEDED, step_id=UNIT_TESTS)
            return [Either(right=True)]
        else:
            log(
                f"{Colors.FAIL}Unit tests failed with exit code {return_code}{Colors.ENDC}",
                step_id=UNIT_TESTS,
                indent=2,
            )
            log(step_status=Status.FAILED, step_id=UNIT_TESTS)
            return [Either(left=Exception(f"Unit tests failed with exit code {return_code}."))]

    def _run_tests(self, package_directory: str) -> int:
        python_cmd, _ = get_python_commands(self.project.project_path)
        env = self._env(package_directory)

        # The generated tests import the project by the package name declared in
        # pyproject.toml (`from <package>.pipelines...`), so the project has to be importable.
        # Install it into a throwaway directory instead of the interpreter's site-packages:
        # several releases can run concurrently in the same pod, so mutating the shared
        # environment would race between them. --no-deps keeps the release image's pinned
        # pyspark / prophecy-libs in place rather than resolving the project's own pins.
        install_command = [
            python_cmd,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-deps",
            "--target",
            package_directory,
            ".",
        ]
        install_code = self._run(install_command, env)
        if install_code != 0:
            raise Exception(f"Failed to install the project for unit tests with exit code {install_code}.")

        test_command = [
            python_cmd,
            "-m",
            "pytest",
            # Prophecy-generated pipelines ship a package literally named `code`, which shadows
            # the stdlib module that pytest's debugging plugin imports at configure time. Same
            # workaround as wheel_test; see pbt.utils.pytest_debugging_stub for the details.
            "-p",
            "no:debugging",
            "-p",
            "pbt.utils.pytest_debugging_stub",
            "-v",
        ] + self.test_directories()

        return self._run(test_command, env)

    def _env(self, package_directory: str) -> dict:
        env = dict(os.environ)

        python_path = env.get("PYTHONPATH", None)
        env["PYTHONPATH"] = f"{package_directory}{os.pathsep}{python_path}" if python_path else package_directory

        if env.get("FABRIC_NAME", None) is None:
            env["FABRIC_NAME"] = "default"  # the generated tests initialise their config from it

        return env

    def _run(self, command: list, env: dict) -> int:
        log(f"Running command {command} on path {self.project.project_path}", step_id=UNIT_TESTS, indent=2)

        process = subprocess.Popen(
            command,
            shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            cwd=self.project.project_path,
        )

        def log_output(pipe):
            while True:
                output = pipe.readline()
                if process.poll() is not None and not output:
                    break

                response = output.decode().strip()
                if len(response) != 0:
                    log(response, step_id=UNIT_TESTS, indent=2)

        stdout_thread = threading.Thread(target=log_output, args=(process.stdout,))
        stderr_thread = threading.Thread(target=log_output, args=(process.stderr,))

        stdout_thread.start()
        stderr_thread.start()

        stdout_thread.join()
        stderr_thread.join()

        return process.wait()
