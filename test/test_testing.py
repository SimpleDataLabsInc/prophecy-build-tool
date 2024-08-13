from click.testing import CliRunner
from src.pbt import test, test_v2
import unittest
import os
import shutil
import uuid
from git import Repo
import glob
from parameterized import parameterized

SAMPLE_REPO = "https://github.com/prophecy-samples/HelloProphecy.git"


class TestingTestCase(unittest.TestCase):

    @staticmethod
    def _get_tmp_sample_repo(repo_url=SAMPLE_REPO):
        new_path = os.path.join("/tmp/", SAMPLE_REPO.split("/")[-1], f"{uuid.uuid4()}")
        repo = Repo.clone_from(repo_url, new_path)
        return repo, new_path

    def setUp(self):
        self.repo, self.repo_path = TestingTestCase._get_tmp_sample_repo()
        self.python_project_path = os.path.join(self.repo_path, "prophecy")
        self.scala_project_path = os.path.join(self.repo_path, "prophecy_scala")

    def tearDown(self):
        if self.repo_path:
            shutil.rmtree(self.repo_path)

    @staticmethod
    def _check_for_n_artifacts(project_path, language, n=5):
        artifacts = []
        # Find any .whl files in the current directory
        if language == 'python':
            # coverage and unit tests
            artifacts += glob.glob(os.path.join(project_path, "**", 'coverage.xml'), recursive=True)
            artifacts += glob.glob(os.path.join(project_path, "**", 'report.xml'), recursive=True)

            # Find any .jar files in the current directory
        if language == 'scala':
            # artifacts = glob.glob(os.path.join(project_path, "**", 'report.xml'), recursive=True)
            pass

            # make sure we found correct build artifacts:
        assert len(list(artifacts)) == n

    @parameterized.expand(
        [
            ("python", test, "./"),
            ("python", test, "./fake.jar,fake2.jar"),
            ("python", test, os.getcwd()),
            ("python", test_v2, "./"),
            ("python", test_v2, "./fake.jar,fake2.jar"),
            ("python", test_v2, os.getcwd())
        ]
    )
    def test_driver_paths(self, language, test_command, driver_library_path):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        with open('./fake.jar', 'w') as fd:
            fd.write("fake")
        with open('./fake2.jar', 'w') as fd:
            fd.write("fake")

        result = runner.invoke(test_command, ["--path", project_path, "--driver-library-path", driver_library_path])
        assert result.exit_code == 0
        assert "fake.jar" in result.output.replace("\n", "")
        assert "fake2.jar" in result.output.replace("\n", "")

    @parameterized.expand(
        [
            ("python", test),
            ("python", test_v2),
            ("scala", test),
            ("scala", test_v2),
        ]
    )
    def test_path_default(self, language, test_command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(test_command, ["--path", project_path])
        assert result.exit_code == 0
        items = [i for i in os.listdir(os.path.join(project_path, "pipelines"))
                 if os.path.isdir(os.path.join(project_path, "pipelines", i))]
        for i in items:
            if test_command is test:
                assert f"Unit test for pipeline: pipelines/{i} succeeded."
            elif test_command is test_v2:
                assert f"Pipeline test succeeded : `pipelines/{i}`"

    @parameterized.expand(
        [
            ("python", test_v2),
            ("scala", test_v2),
        ]
    )
    def test_test_v2_relative_path(self, language, test_command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(test_command, ["--path", os.path.relpath(project_path, os.getcwd())])
        assert result.exit_code == 0

    @parameterized.expand(
        [
            ("python", test),
            ("scala", test),
            ("python", test_v2),
            ("scala", test_v2),
        ]
    )
    def test_test_with_pipeline_filter(self, language, test_command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        pipelines_to_test = [i for i in os.listdir(os.path.join(project_path, "pipelines"))
                             if os.path.isdir(os.path.join(project_path, "pipelines", i))][:2]
        runner = CliRunner()
        result = runner.invoke(test, ["--path", project_path, "--pipelines", ",".join(pipelines_to_test)])
        assert result.exit_code == 0
        for p in pipelines_to_test:
            if os.path.isdir(os.path.join(project_path, p)):
                if test_command is test:
                    assert f"Unit test for pipeline: pipelines/{p} succeeded."
                elif test_command is test_v2:
                    assert f"Pipeline test succeeded : `pipelines/{p}`"

    @parameterized.expand(
        [
            ("python", test),
            ("scala", test),
            # ("python", test_v2), # TODO --pipeline option not present in v2
            # ("scala", test_v2), # TODO --pipeline option not present in v2
        ]
    )
    def test_test_with_pipeline_filter_one_notfound_pipeline(self, language, test_command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        pipeline_to_test = [i for i in os.listdir(os.path.join(project_path, "pipelines"))
                            if os.path.isdir(os.path.join(project_path, "pipelines", i))][0]
        runner = CliRunner()
        result = runner.invoke(test_command, ["--path", project_path, "--pipelines", f"{pipeline_to_test},notfound"])
        assert result.exit_code == 1
        assert "Filtered pipelines doesn't match with passed filter" in result.output

    @parameterized.expand(
        [
            ("python", test),
            ("scala", test),
            # ("python", test_v2), # TODO --pipeline option not present in v2
            # ("scala", test_v2), # TODO --pipeline option not present in v2
        ]
    )
    def test_test_with_pipeline_filter_all_notfound_pipelines(self, language, test_command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        runner = CliRunner()
        result = runner.invoke(test_command, ["--path", project_path, "--pipelines", f"notfound1,notfound2,notfound3"])
        assert result.exit_code == 1
        assert "Filtered pipelines doesn't match with passed filter" in result.output

    @parameterized.expand(
        [
            ("python", test_v2),
            ("python", test),
            # scala does not currently output coverage reports or junit reports
        ]
    )
    def test_coverage_and_test_report_generation(self, language, test_command):
        project_path = self.python_project_path if language == 'python' else self.scala_project_path
        pipelines_to_test = [i for i in os.listdir(os.path.join(project_path, "pipelines"))
                             if os.path.isdir(os.path.join(project_path, "pipelines", i))]
        runner = CliRunner()
        result = runner.invoke(test_command, ["--path", project_path])
        assert result.exit_code == 0
        for p in pipelines_to_test:
            coverage_path = os.path.join(project_path, f"./pipelines/{p}/code/coverage.xml")
            assert f"Testing pipeline `pipelines/{p}`" in result.output
            assert "Coverage XML written to file coverage.xml" in result.output
            assert (os.path.exists(coverage_path))
            assert (os.path.exists(os.path.join(project_path, f"./pipelines/{p}/code/report.xml")))

            with open(coverage_path, 'r') as fd:
                content = fd.read()
                # check to make sure that .coveragerc got picked up and made absolute paths:
                assert ("<source>/" in content)
                # verify that some coverage was written
                assert ("<package name=\"job\"" in content)
                # check that setup.py is ignored
                assert ("<class name=\"setup.py\"" not in content)
                # make sure we are not doing coverage for test directory
                assert ("<package name=\"test\"" not in content)
