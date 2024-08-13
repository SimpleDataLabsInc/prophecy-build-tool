
from src.pbt import build, build_v2
from parameterized import parameterized
import unittest
from click.testing import CliRunner
import os
import shutil
import uuid
from git import Repo
import glob

SAMPLE_REPO = "https://github.com/prophecy-samples/HelloProphecy.git"


class BuildingTestCase(unittest.TestCase):

    @staticmethod
    def _get_tmp_sample_repo(repo_url=SAMPLE_REPO):
        new_path = os.path.join("/tmp/", SAMPLE_REPO.split("/")[-1], f"{uuid.uuid4()}")
        repo = Repo.clone_from(repo_url, new_path)
        return repo, new_path

    def setUp(self):
        self.repo, self.repo_path = BuildingTestCase._get_tmp_sample_repo()
        self.python_project_path = os.path.join(self.repo_path, "prophecy")
        self.scala_project_path = os.path.join(self.repo_path, "prophecy_scala")

    def tearDown(self):
        if self.repo_path:
            shutil.rmtree(self.repo_path)

    @staticmethod
    def _check_for_artifacts(project_path, language, n=5):
        artifacts = []
        # Find any .whl files in the current directory
        if language == 'python':
            artifacts += glob.glob(os.path.join(project_path, "**", '*.whl'), recursive=True)

            # Find any .jar files in the current directory
        if language == 'scala':
            artifacts = glob.glob(os.path.join(project_path, "**", '*.jar'), recursive=True)

            # make sure we found correct build artifacts:
        assert len(list(artifacts)) == n

    @parameterized.expand([
        ("python", build_v2),
        ("scala", build_v2),
        ("python", build),
        ("scala", build),
    ])
    def test_build_path_default(self, language, build_command):
        runner = CliRunner()
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        result = runner.invoke(build_command, ["--path", project_path])
        assert result.exit_code == 0
        assert "Found 5 pipelines" in result.output
        BuildingTestCase._check_for_artifacts(project_path, language, n=5)

    @parameterized.expand([
        ("python", "setup.py"),
        ("scala", "pom.xml"),
    ])
    def test_build_v2_path_default_build_errors(self, language, build_file_name):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        a_file_to_mangle = list(glob.glob(os.path.join(project_path, "**", build_file_name), recursive=True))[0]
        with open(a_file_to_mangle, 'w') as fd:
            fd.write("oops")
        runner = CliRunner()
        result = runner.invoke(build_v2, ["--path", project_path])
        assert result.exit_code == 1
        BuildingTestCase._check_for_artifacts(project_path, language, n=4)

    @parameterized.expand([
        ("python", "setup.py"),
        ("scala", "pom.xml"),
    ])
    def test_build_v2_path_default_build_errors_ignore_errors(self, language, build_file_name):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        a_file_to_mangle = list(glob.glob(os.path.join(project_path, "**", build_file_name), recursive=True))[0]
        with open(a_file_to_mangle, 'w') as fd:
            fd.write("oops")
        runner = CliRunner()
        result = runner.invoke(build_v2, ["--path", project_path, "--ignore-build-errors"])
        assert result.exit_code == 0
        BuildingTestCase._check_for_artifacts(project_path, language, n=4)

    @parameterized.expand([
        ("python", build_v2),
        ("scala", build_v2),
        ("python", build),
        ("scala", build),
    ])
    def test_build_path_pipeline_filter(self, language, build_command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(build_command, ["--path", project_path, "--pipelines", "raw_bronze,gold_sales"])
        assert result.exit_code == 0
        BuildingTestCase._check_for_artifacts(project_path, language, n=2)

    @parameterized.expand([
        ("python", build_v2),
        ("scala", build_v2),
        ("python", build),
        ("scala", build),
    ])
    def test_build_path_pipeline_with_invalid_filter(self, language, build_command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(
            build_command,
            [
                "--path",
                project_path,
                "--pipelines",
                "raw_bronze,INVALID_PIPELINE_NAME",
            ],
        )
        assert result.exit_code == 0
        BuildingTestCase._check_for_artifacts(project_path, language, n=1)

    @parameterized.expand([
        # ("python", build_v2), # currently different behavior for build-v2
        # ("scala", build_v2), # currently different behavior for build-v2
        ("python", build),
        ("scala", build),
    ])
    def test_build_path_pipeline_invalid_filter_only(self, language, build_command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(build_command, ["--path", project_path, "--pipelines", "INVALID_PIPELINE_NAME"])
        assert result.exit_code == 1
        BuildingTestCase._check_for_artifacts(project_path, language, n=0)

