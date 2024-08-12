
from src.pbt import build, build_v2
from parameterized import parameterized
import unittest
from click.testing import CliRunner
import os
import shutil
import uuid
from git import Repo
import glob

CURRENT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
REPO_PATH = os.path.dirname(CURRENT_DIRECTORY)
RESOURCES_PATH = os.path.join(CURRENT_DIRECTORY, "resources")
SAMPLE_REPO = "https://github.com/prophecy-samples/HelloProphecy.git"
PROJECT_PATH = str(os.getcwd()) + "/test/resources/HelloWorld"
ERROR_PROJECT_PATH = str(os.getcwd()) + "/test/resources/HelloWorldBuildError"



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


    @parameterized.expand([
        #("python", build),
        ("python", build_v2),
        #("scala", build),
        #("scala", build_v2),
    ])
    def test_build_path_default(self, language, build_command):
        runner = CliRunner()
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        result = runner.invoke(build_command, ["--path", project_path])
        assert result.exit_code == 0
        assert "Found 5 pipelines" in result.output

        artifacts = []
        # Find any .whl files in the current directory
        if language == 'python':
            artifacts += glob.glob(os.path.join(project_path, "**", '*.whl'), recursive=True)

        # Find any .jar files in the current directory
        if language == 'scala':
            artifacts = glob.glob(os.path.join(project_path, "**", '*.jar'), recursive=True)

        # make sure we found correct build artifacts:
        assert len(list(artifacts)) == 5

    #
    # def test_build_v2_path_default_build_errors(self):
    #     runner = CliRunner()
    #     result = runner.invoke(build_v2, ["--path", ERROR_PROJECT_PATH])
    #     assert result.exit_code == 1
    #
    #
    # def test_build_v2_path_default_build_errors_ignore_errors(self):
    #     runner = CliRunner()
    #     result = runner.invoke(build_v2, ["--path", ERROR_PROJECT_PATH, "--ignore-build-errors"])
    #     assert result.exit_code == 0
    #
    #
    # def test_build_path_pipeline_filter(self):
    #     runner = CliRunner()
    #     result = runner.invoke(build, ["--path", PROJECT_PATH, "--pipelines", "customers_orders,join_agg_sort"])
    #     assert result.exit_code == 0
    #     assert "Found 4 pipelines" in result.output
    #     assert "Building 2 pipelines" in result.output
    #     assert "Filtering pipelines: ['customers_orders', 'join_agg_sort']" in result.output
    #     assert "Building pipeline pipelines/customers_orders" in result.output
    #     assert "Building pipeline pipelines/join_agg_sort" in result.output
    #
    #
    # def test_build_path_pipeline_with_invalid_filter(self):
    #     runner = CliRunner()
    #     result = runner.invoke(
    #         build,
    #         [
    #             "--path",
    #             PROJECT_PATH,
    #             "--pipelines",
    #             "customers_orders,INVALID_PIPELINE_NAME",
    #         ],
    #     )
    #     assert result.exit_code == 0
    #     assert "Found 4 pipelines" in result.output
    #     assert "Building 1 pipelines" in result.output
    #     assert "Filtering pipelines: ['customers_orders', 'INVALID_PIPELINE_NAME']" in result.output
    #     assert "Building pipeline pipelines/customers_orders" in result.output
    #
    #
    # def test_build_path_pipeline_invalid_filter_only(self):
    #     runner = CliRunner()
    #     result = runner.invoke(build, ["--path", PROJECT_PATH, "--pipelines", "INVALID_PIPELINE_NAME"])
    #     assert result.exit_code == 1
    #     assert "Found 4 pipelines" in result.output
    #     assert "No matching pipelines found for given pipelines names: ['INVALID_PIPELINE_NAME']" in result.output
